"""Comunicação entre nós: HTTP/JSON (REST) com um envelope único.

Toda mensagem entre nós é um POST em /internal/msg com o corpo:

    {"type": "HEARTBEAT", "sender": 2, "term": 4, "lamport": 117, "payload": {...}}

e a resposta usa o mesmo envelope (type = "ACK", ou "ANSWER" para ELECTION).
O relógio de Lamport é atualizado em todo envio e recebimento, aqui mesmo.

Cada mensagem enviada também fica registrada em `sent` (últimas 150), só para o
modo apresentação desenhar as mensagens viajando entre os nós.
"""
from __future__ import annotations

import time
from collections import deque

import httpx

from . import config

NOT_RECORDED = {"HELLO", "STATUS"}  # descoberta e painel: não poluem a animação


class Transport:
    def __init__(self, node) -> None:
        self.node = node
        self.http = httpx.AsyncClient()
        self.sent: deque[dict] = deque(maxlen=150)
        self._seq = 0

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
        if self.node.crashed:
            return None  # nó "derrubado" não fala com ninguém
        timeout = timeout or config.RPC_TIMEOUT
        body = self.envelope(msg_type, payload)
        started = time.time()
        data = None
        try:
            resp = await self.http.post(
                f"{url}/internal/msg", json=body,
                timeout=httpx.Timeout(timeout, connect=min(0.5, timeout)),
            )
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPError, ValueError):
            data = None
        self._record(url, msg_type, body["lamport"], started, data is not None)
        if data is None or self.node.crashed:
            return None
        self.node.on_reply(data)  # regra 3 de Lamport + detector de falhas
        return data

    def _record(self, url: str, msg_type: str, lamport: int, started: float, ok: bool) -> None:
        if msg_type in NOT_RECORDED:
            return
        to = next((p.node_id for p in self.node.membership.peers.values() if p.url == url), None)
        if to is None:
            return
        self._seq += 1
        self.sent.append({"n": self._seq, "t": started, "to": to, "type": msg_type,
                          "lamport": lamport, "ok": ok, "dur": round(time.time() - started, 3)})

    async def close(self) -> None:
        await self.http.aclose()
