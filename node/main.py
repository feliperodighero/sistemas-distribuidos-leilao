"""API HTTP do nó (FastAPI).

Rotas públicas (clientes):   /api/...   + páginas / (modo apresentação), /cliente e /dashboard
Rota interna (entre nós):    POST /internal/msg   (envelope descrito em docs/PROTOCOLO.md)
Administração (demo):        /api/admin/crash, /api/admin/restart, /api/admin/speed
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .node import LeaderUnavailable, Node, NodeCrashed

STATIC = Path(__file__).parent / "static"
node = Node()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await node.start()
    yield
    await node.stop()


app = FastAPI(title="Leilão On-line Distribuído", lifespan=lifespan)

ALWAYS_ON = ("/api/admin/", "/static/", "/health")
PAGES = ("/", "/cliente", "/dashboard")


@app.middleware("http")
async def crashed_guard(request: Request, call_next):
    """Nó "derrubado": só as páginas e a administração respondem; o resto devolve 503."""
    path = request.url.path
    if node.crashed and path not in PAGES and not path.startswith(ALWAYS_ON):
        return JSONResponse({"crashed": True, "detail": f"nó {node.id} está fora do ar"},
                            status_code=503, headers={"X-Node-Crashed": "1"})
    return await call_next(request)


# Os clientes web trocam de nó quando um cai, então precisam chamar outras origens.
# (adicionado depois do guard para ficar por fora dele e pôr os cabeçalhos CORS também no 503)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
                   expose_headers=["X-Node-Crashed"])
app.mount("/static", StaticFiles(directory=STATIC), name="static")


class BidIn(BaseModel):
    bidder: str = Field(min_length=1, max_length=40)
    amount: float = Field(gt=0, le=1_000_000_000)
    request_id: str | None = Field(default=None, max_length=40)  # idempotência ao reenviar


class SpeedIn(BaseModel):
    slow: bool


class AuctionIn(BaseModel):
    title: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=300)
    start_price: float = Field(gt=0, le=1_000_000_000)
    min_increment: float = Field(default=1.0, gt=0, le=1_000_000_000)
    duration_s: int = Field(default=120, ge=10, le=7200)


# ---------------------------------------------------------------- comunicação entre nós
@app.post("/internal/msg")
async def internal_message(envelope: dict):
    try:
        return await node.handle_message(envelope)
    except NodeCrashed:
        raise HTTPException(503, "nó fora do ar")


# ---------------------------------------------------------------- administração (modo apresentação)
@app.post("/api/admin/crash")
async def admin_crash():
    """Falha simulada: o nó para de enviar e de responder (como se o processo morresse)."""
    node.crash()
    return {"node_id": node.id, "crashed": True}


@app.post("/api/admin/restart")
async def admin_restart():
    """Religa o nó com a memória vazia, como um processo recém-iniciado."""
    node.restart()
    return {"node_id": node.id, "crashed": False}


@app.post("/api/admin/speed")
async def admin_speed(body: SpeedIn):
    node.set_speed(body.slow)
    return {"node_id": node.id, "slow": node.slow}


# ---------------------------------------------------------------- API dos clientes
@app.get("/health")
async def health():
    return {"ok": True, "node_id": node.id, "crashed": node.crashed}


@app.get("/api/auctions")
async def list_auctions():
    return {"node_id": node.id, "commit_seq": node.log.commit_seq, "auctions": node.log.state.list_summaries()}


@app.get("/api/auctions/{auction_id}")
async def get_auction(auction_id: str):
    auction = node.log.state.auctions.get(auction_id)
    if auction is None:
        raise HTTPException(404, "leilão inexistente")
    return dict(node.log.state.summary(auction), bids=auction["bids"])


@app.post("/api/auctions", status_code=201)
async def create_auction(body: AuctionIn):
    try:
        result = await node.submit_auction(body.title, body.description, body.start_price,
                                           body.min_increment, body.duration_s)
    except LeaderUnavailable as exc:
        raise HTTPException(503, str(exc))
    if not result.get("created"):
        return JSONResponse(result, status_code=503)
    return result


@app.post("/api/auctions/{auction_id}/bids")
async def place_bid(auction_id: str, body: BidIn):
    try:
        result = await node.submit_bid(auction_id, body.bidder, body.amount, body.request_id)
    except LeaderUnavailable as exc:
        raise HTTPException(503, str(exc))
    result["received_by"] = node.id
    return JSONResponse(result, status_code=201 if result.get("accepted") else 409)


@app.get("/api/cluster")
async def cluster():
    """Visão resumida do cluster (usada pelos clientes para trocar de nó se este cair)."""
    nodes = [{"node_id": node.id, "public_url": node.status()["public_url"], "alive": True}]
    nodes += [{"node_id": p.node_id, "public_url": p.public_url, "alive": p.alive}
              for p in node.membership.peers.values()]
    return {"node_id": node.id, "leader_id": node.leader_id, "term": node.term,
            "lamport": node.clock.value, "role": node.role,
            "nodes": sorted(nodes, key=lambda n: n["node_id"])}


@app.get("/api/node")
async def node_status():
    """Estado só deste nó."""
    return node.status()


@app.get("/api/cluster/status")
async def cluster_status():
    """Estado de todos os nós (para o painel): pergunta STATUS a cada vizinho."""
    peers = list(node.membership.peers.values())
    replies = await asyncio.gather(*(node.transport.send(p.url, "STATUS", {}, timeout=0.8) for p in peers))
    nodes = [node.status()]
    for peer, reply in zip(peers, replies):
        if reply:
            nodes.append(reply["payload"])
        else:
            nodes.append({"node_id": peer.node_id, "public_url": peer.public_url, "role": "down",
                          "last_seen_s": round(peer.seconds_since_seen(), 1)})
    # nós que já conheci antes de um reinício ou que outro nó vê: não sumir da mesa
    seen = {n["node_id"] for n in nodes}
    for peer_status in [n for n in nodes if n.get("peers")]:
        for p in peer_status["peers"]:
            if p["node_id"] not in seen:
                seen.add(p["node_id"])
                nodes.append({"node_id": p["node_id"], "public_url": p.get("public_url", ""),
                              "role": "down", "last_seen_s": p.get("last_seen_s")})
    nodes.sort(key=lambda n: n["node_id"])
    return {"asked": node.id, "nodes": nodes}


# ---------------------------------------------------------------- páginas
@app.get("/")
async def demo():
    return FileResponse(STATIC / "demo.html")


@app.get("/cliente")
async def client_page():
    return FileResponse(STATIC / "index.html")


@app.get("/dashboard")
async def dashboard():
    return FileResponse(STATIC / "dashboard.html")
