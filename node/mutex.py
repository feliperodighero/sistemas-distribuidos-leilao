"""Exclusão mútua distribuída sobre cada item do leilão (algoritmo centralizado).

O leiloeiro (líder) é o COORDENADOR do mutex. Um nó que recebe um lance envia o
pedido ao coordenador (mensagem BID_REQUEST, que faz o papel de REQUEST). O
coordenador mantém, para cada item, quem está na seção crítica e uma fila de espera.

Diferença em relação ao algoritmo centralizado clássico (fila FIFO): aqui a fila é
de PRIORIDADE pelo timestamp de Lamport (L, node_id). Para que pedidos concorrentes
que chegam quase ao mesmo tempo sejam ordenados pelo relógio lógico (e não pela
sorte da rede), cada pedido espera uma pequena janela (hold-back) antes de poder
ser atendido.

Garantias:
  * Segurança: no máximo um lance por item dentro da seção crítica.
  * Itens diferentes não se bloqueiam (um lock por item).
  * Ordem: entre pedidos que estão na fila ao mesmo tempo, o de menor (L, nó) entra primeiro.
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from contextlib import asynccontextmanager


class _ItemLock:
    def __init__(self) -> None:
        self.holder: dict | None = None
        self.queue: list = []  # heap de (chave_lamport, contador, future, info, instante_entrada)


class MutexCoordinator:
    def __init__(self, holdback: float = 0.15, on_event=None) -> None:
        self.holdback = holdback
        self.locks: dict[str, _ItemLock] = {}
        self._counter = itertools.count()
        self._on_event = on_event or (lambda kind, msg, code=None, **data: None)

    @asynccontextmanager
    async def hold(self, item: str, key: tuple[int, int], info: dict):
        """Uso: `async with mutex.hold(item, (L, nó), info): ...` — seção crítica."""
        await self.acquire(item, key, info)
        try:
            yield
        finally:
            self.release(item)

    async def acquire(self, item: str, key: tuple[int, int], info: dict) -> None:
        loop = asyncio.get_running_loop()
        lock = self.locks.setdefault(item, _ItemLock())
        fut = loop.create_future()
        info = dict(info, key=list(key))
        heapq.heappush(lock.queue, (tuple(key), next(self._counter), fut, info, time.monotonic()))
        self._on_event("MUTEX", f"pedido {fmt(key)} entrou na fila do item {short(item)} "
                                f"({len(lock.queue)} na fila)",
                       code="mutex_queued", key=list(key), size=len(lock.queue), item=item)
        loop.call_later(self.holdback, self._dispatch, item)
        try:
            await fut
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():
                self.release(item)  # já tinha recebido a concessão
            else:
                lock.queue = [e for e in lock.queue if e[2] is not fut]
                heapq.heapify(lock.queue)
            raise

    def release(self, item: str) -> None:
        lock = self.locks[item]
        if lock.holder is not None:
            self._on_event("MUTEX", f"{fmt(lock.holder['key'])} liberou o item {short(item)}",
                           code="mutex_released", key=lock.holder["key"], item=item)
        lock.holder = None
        self._dispatch(item)

    def _dispatch(self, item: str) -> None:
        lock = self.locks.get(item)
        if lock is None or lock.holder is not None or not lock.queue:
            return
        key, _, fut, info, entered = lock.queue[0]
        if time.monotonic() - entered < self.holdback - 0.005:
            return  # ainda na janela de hold-back; o timer desse pedido chamará _dispatch de novo
        heapq.heappop(lock.queue)
        if fut.cancelled():
            self._dispatch(item)
            return
        lock.holder = info
        self._on_event("MUTEX", f"item {short(item)} concedido a {fmt(key)}",
                       code="mutex_granted", key=list(key), item=item, waiting=len(lock.queue))
        fut.set_result(None)

    def snapshot(self) -> dict:
        """Estado atual (para o painel)."""
        out = {}
        for item, lock in self.locks.items():
            if lock.holder is None and not lock.queue:
                continue
            out[item] = {
                "holder": lock.holder,
                "queue": [e[3] for e in sorted(lock.queue, key=lambda e: (e[0], e[1]))],
            }
        return out


def fmt(key) -> str:
    return f"(L={key[0]}, nó {key[1]})"


def short(item: str) -> str:
    return item[:6]
