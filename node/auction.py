"""Máquina de estados do leilão.

O estado é construído aplicando as entradas do log replicado, em ordem de `seq`.
Como a função `apply` é determinística, todos os nós que aplicam o mesmo log
chegam exatamente ao mesmo estado (mesmo lance vencedor, mesmo histórico).
"""
from __future__ import annotations

import time

from .lamport import order_key

EPS = 1e-9


def money(value: float) -> float:
    return round(float(value), 2)


class AuctionState:
    def __init__(self) -> None:
        self.auctions: dict[str, dict] = {}
        self.bids_by_id: dict[str, dict] = {}  # idempotência: o mesmo lance não entra duas vezes

    # ------------------------------------------------------------------ aplicar log
    def apply(self, entry: dict) -> None:
        kind, data = entry["kind"], entry["data"]
        if kind == "CREATE_AUCTION":
            auction = dict(data)
            auction.update(status="open", bids=[], winner=None, created_seq=entry["seq"])
            self.auctions[auction["id"]] = auction
        elif kind == "BID":
            bid = dict(data)
            bid["seq"] = entry["seq"]
            self.auctions[bid["auction_id"]]["bids"].append(bid)
            self.bids_by_id[bid["id"]] = bid
        elif kind == "CLOSE":
            auction = self.auctions[data["auction_id"]]
            auction["status"] = "closed"
            auction["closed_seq"] = entry["seq"]
            auction["winner"] = self.highest_bid(auction)
        else:
            raise ValueError(f"tipo de entrada desconhecido: {kind}")

    # ------------------------------------------------------------------ regras
    @staticmethod
    def highest_bid(auction: dict) -> dict | None:
        """Maior lance; empate de valor é decidido pela ordem total de Lamport (menor vence)."""
        if not auction["bids"]:
            return None
        return max(
            auction["bids"],
            key=lambda b: (b["amount"], tuple(-x for x in order_key(b["lamport"], b["node_id"]))),
        )

    def min_next_bid(self, auction: dict) -> float:
        top = self.highest_bid(auction)
        if top is None:
            return money(auction["start_price"])
        return money(top["amount"] + auction["min_increment"])

    def validate_bid(self, bid: dict, now: float | None = None) -> str | None:
        """Retorna None se o lance é válido, ou o motivo da rejeição."""
        now = time.time() if now is None else now
        auction = self.auctions.get(bid["auction_id"])
        if auction is None:
            return "leilão inexistente"
        if auction["status"] != "open" or now >= auction["ends_at"]:
            return "leilão encerrado"
        if not str(bid.get("bidder", "")).strip():
            return "nome do licitante é obrigatório"
        minimum = self.min_next_bid(auction)
        if bid["amount"] + EPS < minimum:
            return f"lance abaixo do mínimo (mínimo atual: R$ {minimum:.2f})"
        return None

    # ------------------------------------------------------------------ visões
    def summary(self, auction: dict, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        top = self.highest_bid(auction)
        return {
            "id": auction["id"],
            "title": auction["title"],
            "description": auction.get("description", ""),
            "start_price": auction["start_price"],
            "min_increment": auction["min_increment"],
            "ends_at": auction["ends_at"],
            "remaining_s": max(0.0, auction["ends_at"] - now) if auction["status"] == "open" else 0.0,
            "status": auction["status"],
            "highest_bid": top,
            "bid_count": len(auction["bids"]),
            "min_next_bid": self.min_next_bid(auction),
            "winner": auction["winner"],
            "created_by_node": auction.get("created_by_node"),
        }

    def list_summaries(self) -> list[dict]:
        now = time.time()
        items = [self.summary(a, now) for a in self.auctions.values()]
        # abertos primeiro, depois os que terminam antes
        items.sort(key=lambda a: (a["status"] != "open", a["ends_at"]))
        return items

    def open_auctions_past_deadline(self, now: float | None = None) -> list[str]:
        now = time.time() if now is None else now
        return [a["id"] for a in self.auctions.values() if a["status"] == "open" and now >= a["ends_at"]]
