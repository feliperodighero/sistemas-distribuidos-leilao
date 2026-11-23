"""Eleição do leiloeiro com o algoritmo Bully (valentão).

Regras:
  * Quando um nó percebe que não há líder (o líder parou de mandar heartbeat, ou o nó
    acabou de subir), ele inicia uma eleição: manda ELECTION para todos os nós com ID
    MAIOR que o seu.
  * Quem recebe ELECTION de um nó menor responde ANSWER ("estou vivo, deixa comigo")
    e inicia a sua própria eleição.
  * Se ninguém maior responder dentro de ELECTION_TIMEOUT, o nó se declara líder e
    manda COORDINATOR para todos.
  * Se alguém maior respondeu, o nó espera um COORDINATOR por COORDINATOR_TIMEOUT;
    se não chegar (o maior caiu no meio), recomeça a eleição.
Resultado: o nó vivo com o maior ID vira o leiloeiro.

Termo (mandato): cada novo líder usa termo = maior termo conhecido + 1. Mensagens de um
líder com termo antigo são recusadas, o que evita que um líder "zumbi" continue escrevendo.
"""
from __future__ import annotations

import asyncio
import time

from . import config


class BullyElection:
    def __init__(self, node) -> None:
        self.node = node
        self.running = False
        self.coordinator_event = asyncio.Event()

    def trigger(self, reason: str) -> None:
        if self.running or self.node.is_leader or self.node.crashed:
            return
        wait = config.STARTUP_GRACE - (time.monotonic() - self.node.restarted_at)
        if wait > 0:
            # acabou de subir: primeiro descobre os vizinhos, depois decide se precisa de eleição
            asyncio.get_running_loop().call_later(wait, self.trigger, reason)
            return
        self.running = True
        asyncio.create_task(self._run(reason))

    async def _run(self, reason: str) -> None:
        node = self.node
        node.events.add("ELEIÇÃO", f"iniciando eleição Bully ({reason})", code="election_start", reason=reason)
        incarnation = node.incarnation
        try:
            while True:
                if node.crashed or node.incarnation != incarnation:
                    return  # o nó foi derrubado/reiniciado no meio da eleição
                higher = [p for p in node.membership.alive_peers() if p.node_id > node.id]
                if not higher:
                    await node.become_leader()
                    return
                self.coordinator_event.clear()
                node.events.add("ELEIÇÃO", f"ELECTION enviado para nós {[p.node_id for p in higher]}",
                                code="election_sent", to=[p.node_id for p in higher])
                replies = await asyncio.gather(*(
                    node.transport.send(p.url, "ELECTION", {}, timeout=config.ELECTION_TIMEOUT) for p in higher
                ))
                answered = [p.node_id for p, r in zip(higher, replies) if r and r["type"] == "ANSWER"]
                if node.crashed or node.incarnation != incarnation:
                    return
                if not answered:
                    node.events.add("ELEIÇÃO", "nenhum nó maior respondeu", code="election_no_answer")
                    await node.become_leader()
                    return
                node.events.add("ELEIÇÃO", f"nós {answered} responderam ANSWER; aguardando COORDINATOR",
                                code="election_answered", by=answered)
                try:
                    await asyncio.wait_for(self.coordinator_event.wait(), config.COORDINATOR_TIMEOUT)
                    return
                except asyncio.TimeoutError:
                    node.events.add("ELEIÇÃO", "COORDINATOR não chegou; recomeçando eleição")
        finally:
            self.running = False

    # ------------------------------------------------------------------ mensagens recebidas
    def on_election(self, sender: int) -> dict:
        node = self.node
        if node.is_leader:
            # Já sou o líder: reafirmo a liderança para todos (sem novo termo).
            asyncio.create_task(node.announce_coordinator())
        else:
            self.trigger(f"ELECTION recebido do nó {sender}")
        return {"alive": True}

    def on_coordinator(self, sender: int, term: int) -> dict:
        node = self.node
        if term < node.term:
            return {"accepted": False, "reason": "termo antigo"}
        current = node.membership.get(node.leader_id)
        if (term == node.term and current is not None and current.alive
                and node.leader_id != node.id and sender < node.leader_id):
            # empate de termo: já sigo um líder vivo com ID maior
            return {"accepted": False, "reason": "já existe líder com ID maior neste termo"}
        if sender < node.id:
            # Bully: um nó com ID menor se declarou líder, mas eu estou vivo e sou maior.
            self.trigger(f"COORDINATOR do nó {sender}, que tem ID menor")
            return {"accepted": False, "reason": "existe nó com ID maior"}
        node.set_leader(sender, term)
        self.coordinator_event.set()
        return {"accepted": True}
