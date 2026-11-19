"""Comunicação entre nós: HTTP/JSON (REST) com um envelope único.

Toda mensagem entre nós é um POST em /internal/msg com o corpo:

    {"type": "HEARTBEAT", "sender": 2, "term": 4, "lamport": 117, "payload": {...}}

e a resposta usa o mesmo envelope (type = "ACK", ou "ANSWER" para ELECTION).
O relógio de Lamport é atualizado em todo envio e recebimento, aqui mesmo.
"""
from __future__ import annotations

import httpx

from . import config


class Transport:
    def __init__(self, node) -> None:
        self.node = node
        self.http = httpx.AsyncClient()

    def envelope(self, msg_type: str, payload: dict | None) -> dict:
        return {
            "type": msg_type,
            "sender": self.node.id,
            "term": self.node.term,
            "lamport": self.node.clock.tick(),  # regra 1 de Lamport: incrementa antes de enviar
            "payload": payload or {},
        }

    async def send(self, url: str, msg_type: str, payload: dict | None = None,
                   timeout: float | None = None) -> dict | None:
        """Envia e devolve o envelope de resposta, ou None se o nó não respondeu."""
        timeout = timeout or config.RPC_TIMEOUT
        body = self.envelope(msg_type, payload)
        try:
            resp = await self.http.post(
                f"{url}/internal/msg", json=body,
                timeout=httpx.Timeout(timeout, connect=min(0.5, timeout)),
            )
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPError, ValueError):
            return None
        self.node.on_reply(data)  # regra 3 de Lamport + detector de falhas
        return data

    async def close(self) -> None:
        await self.http.aclose()
