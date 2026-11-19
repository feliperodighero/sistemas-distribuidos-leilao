#!/usr/bin/env python3
"""Cliente de linha de comando do leilão distribuído (só biblioteca padrão).

Fala com qualquer nó; se o nó em uso não responder, tenta o próximo da lista.

Exemplos:
  python client/cli.py status
  python client/cli.py criar "Notebook" --preco 100 --incremento 10 --duracao 180
  python client/cli.py listar
  python client/cli.py lance <id> 150 --nome Felipe
  python client/cli.py ver <id>
  python client/cli.py acompanhar <id>
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

DEFAULT_NODES = os.getenv("LEILAO_NODES", "http://localhost:8001,http://localhost:8002,http://localhost:8003")


class Client:
    def __init__(self, nodes: list[str]):
        self.nodes = nodes
        self.current = 0

    def call(self, method: str, path: str, body: dict | None = None, timeout: float = 12.0):
        last_error = None
        for _ in range(len(self.nodes)):
            base = self.nodes[self.current]
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(base + path, data=data, method=method,
                                         headers={"content-type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return resp.status, json.loads(resp.read() or b"{}"), base
            except urllib.error.HTTPError as err:  # o nó respondeu com erro (409, 503...)
                return err.code, json.loads(err.read() or b"{}"), base
            except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
                last_error = err
                self.current = (self.current + 1) % len(self.nodes)
                print(f"  ! {base} não respondeu; tentando {self.nodes[self.current]}", file=sys.stderr)
        raise SystemExit(f"nenhum nó respondeu ({last_error})")


def brl(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def cmd_status(c: Client, _args):
    _, body, base = c.call("GET", "/api/cluster")
    print(f"consultado: {base} (nó {body['node_id']})  líder: nó {body['leader_id']}  termo: {body['term']}")
    for n in body["nodes"]:
        print(f"  nó {n['node_id']}: {'no ar' if n['alive'] else 'FORA DO AR'}  {n['public_url']}")


def cmd_listar(c: Client, _args):
    _, body, base = c.call("GET", "/api/auctions")
    print(f"(lido do nó {body['node_id']}, commit {body['commit_seq']})")
    for a in body["auctions"]:
        top = a["highest_bid"]
        atual = f"{brl(top['amount'])} por {top['bidder']}" if top else f"inicial {brl(a['start_price'])}"
        estado = f"termina em {int(a['remaining_s'])}s" if a["status"] == "open" else "encerrado"
        print(f"  {a['id']}  {a['title']:<24} {atual:<32} {estado}")


def cmd_ver(c: Client, args):
    status, a, _ = c.call("GET", f"/api/auctions/{args.id}")
    if status != 200:
        raise SystemExit(a.get("detail", "erro"))
    print(f"{a['title']}  ({a['status']})  mínimo próximo lance: {brl(a['min_next_bid'])}")
    for b in a["bids"]:
        print(f"  seq {b['seq']:>3}  (L={b['lamport']}, nó {b['node_id']})  {b['bidder']:<16} {brl(b['amount'])}")
    if a["winner"]:
        print(f"VENCEDOR: {a['winner']['bidder']} com {brl(a['winner']['amount'])}")


def cmd_lance(c: Client, args):
    body = {"bidder": args.nome, "amount": args.valor, "request_id": uuid.uuid4().hex[:16]}
    status, r, base = c.call("POST", f"/api/auctions/{args.id}/bids", body)
    if r.get("accepted"):
        b = r["bid"]
        print(f"ACEITO via {base}: {brl(b['amount'])}  seq={b['seq']}  Lamport=(L={b['lamport']}, nó {b['node_id']})")
    else:
        print(f"RECUSADO ({status}) via {base}: {r.get('reason') or r.get('detail')}")


def cmd_criar(c: Client, args):
    body = {"title": args.titulo, "start_price": args.preco, "min_increment": args.incremento,
            "duration_s": args.duracao}
    status, r, base = c.call("POST", "/api/auctions", body)
    if r.get("created"):
        print(f"criado via {base}: id {r['auction']['id']}")
    else:
        print(f"falhou ({status}): {r.get('reason') or r.get('detail')}")


def cmd_acompanhar(c: Client, args):
    seen = 0
    while True:
        status, a, base = c.call("GET", f"/api/auctions/{args.id}", timeout=3)
        for b in a.get("bids", [])[seen:]:
            print(f"  seq {b['seq']:>3}  (L={b['lamport']}, nó {b['node_id']})  {b['bidder']:<16} {brl(b['amount'])}")
        seen = len(a.get("bids", []))
        if a.get("status") == "closed":
            w = a.get("winner")
            print(f"ENCERRADO. Vencedor: {w['bidder']} com {brl(w['amount'])}" if w else "ENCERRADO sem lances")
            return
        time.sleep(1)


def main():
    p = argparse.ArgumentParser(description="Cliente do leilão distribuído")
    p.add_argument("--nos", default=DEFAULT_NODES, help="URLs dos nós, separadas por vírgula")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("listar")
    s = sub.add_parser("ver"); s.add_argument("id")
    s = sub.add_parser("acompanhar"); s.add_argument("id")
    s = sub.add_parser("lance"); s.add_argument("id"); s.add_argument("valor", type=float)
    s.add_argument("--nome", default=os.getenv("USER", "cli"))
    s = sub.add_parser("criar"); s.add_argument("titulo")
    s.add_argument("--preco", type=float, default=100); s.add_argument("--incremento", type=float, default=10)
    s.add_argument("--duracao", type=int, default=180)
    args = p.parse_args()
    client = Client([u.strip().rstrip("/") for u in args.nos.split(",") if u.strip()])
    globals()[f"cmd_{args.cmd}"](client, args)


if __name__ == "__main__":
    main()
