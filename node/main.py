"""API HTTP do nó (FastAPI).

Rotas públicas (clientes):   /api/...   + páginas / (leilão) e /dashboard (painel)
Rota interna (entre nós):    POST /internal/msg   (envelope descrito em docs/PROTOCOLO.md)
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .node import LeaderUnavailable, Node

STATIC = Path(__file__).parent / "static"
node = Node()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await node.start()
    yield
    await node.stop()


app = FastAPI(title="Leilão On-line Distribuído", lifespan=lifespan)
# Os clientes web trocam de nó quando um cai, então precisam chamar outras origens.
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=STATIC), name="static")


class BidIn(BaseModel):
    bidder: str = Field(min_length=1, max_length=40)
    amount: float = Field(gt=0, le=1_000_000_000)
    request_id: str | None = Field(default=None, max_length=40)  # idempotência ao reenviar


class AuctionIn(BaseModel):
    title: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=300)
    start_price: float = Field(gt=0, le=1_000_000_000)
    min_increment: float = Field(default=1.0, gt=0, le=1_000_000_000)
    duration_s: int = Field(default=120, ge=10, le=7200)


# ---------------------------------------------------------------- comunicação entre nós
@app.post("/internal/msg")
async def internal_message(envelope: dict):
    return await node.handle_message(envelope)


# ---------------------------------------------------------------- API dos clientes
@app.get("/health")
async def health():
    return {"ok": True, "node_id": node.id}


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
    nodes.sort(key=lambda n: n["node_id"])
    return {"asked": node.id, "nodes": nodes}


# ---------------------------------------------------------------- páginas
@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/dashboard")
async def dashboard():
    return FileResponse(STATIC / "dashboard.html")
