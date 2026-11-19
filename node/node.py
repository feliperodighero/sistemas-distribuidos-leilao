"""O nó do leilão: junta descoberta, heartbeat, eleição, exclusão mútua e replicação.

Papéis:
  follower   réplica comum; recebe lances de clientes e os encaminha ao leiloeiro
  recovering acabou de vencer a eleição e está sincronizando o log
  leader     leiloeiro: coordena o mutex de cada item, valida e replica os lances,
             e encerra os leilões no horário
"""
from __future__ import annotations

import asyncio
import time
import uuid

from . import config
from .auction import money
from .election import BullyElection
from .events import EventLog
from .lamport import LamportClock, order_key
from .membership import Membership
from .mutex import MutexCoordinator
from .replication import NoQuorum, NotLeader, ReplicatedLog, Replicator
from .transport import Transport


class LeaderUnavailable(Exception):
    pass


class Node:
    def __init__(self) -> None:
        self.id = config.NODE_ID
        self.started_at = time.time()
        self.clock = LamportClock()
        self.events = EventLog(self.id, self.clock)
        self.term = 0
        self.leader_id: int | None = None
        self.role = "follower"
        self.synced_term = -1  # termo do líder com quem meu log foi verificado
        self.log = ReplicatedLog()
        self.membership = Membership(self)
        self.transport = Transport(self)
        self.election = BullyElection(self)
        self.replicator = Replicator(self)
        self.mutex = MutexCoordinator(config.MUTEX_HOLDBACK, on_event=self.events.add)
        self._tasks: list[asyncio.Task] = []
        self._hb_inflight: set[int] = set()
        self._closing: set[str] = set()

    # ================================================================== ciclo de vida
    async def start(self) -> None:
        self.events.add("INÍCIO", f"nó {self.id} iniciado; descoberta via DNS {config.DISCOVERY_HOSTS}")
        self._tasks = [
            asyncio.create_task(self.membership.discovery_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._auction_closer_loop()),
        ]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await self.transport.close()

    # ================================================================== papéis
    @property
    def is_leader(self) -> bool:
        return self.role in ("leader", "recovering")

    @property
    def is_ready_leader(self) -> bool:
        return self.role == "leader"

    def set_leader(self, leader_id: int, term: int) -> None:
        """Passo a seguir o líder `leader_id` (vale também para descer de líder)."""
        self.term = max(self.term, term)
        if leader_id == self.id:
            return
        changed = self.leader_id != leader_id
        was_leader = self.is_leader
        self.leader_id = leader_id
        self.role = "follower"
        if changed or was_leader:
            self.events.add("LÍDER", f"novo leiloeiro: nó {leader_id} (termo {self.term})")

    async def become_leader(self) -> None:
        self.term += 1
        my_term = self.term
        self.leader_id = self.id
        self.role = "recovering"
        self.events.add("LÍDER", f"assumi como LEILOEIRO (termo {my_term}); sincronizando log")
        await self.announce_coordinator()
        await self.replicator.recover()
        if self.role == "recovering" and self.term == my_term:
            self.role = "leader"
            self.synced_term = my_term
            self.events.add("LÍDER", f"pronto para receber lances (commit={self.log.commit_seq})")

    async def announce_coordinator(self) -> None:
        peers = self.membership.alive_peers()
        await asyncio.gather(*(self.transport.send(p.url, "COORDINATOR", {"leader_id": self.id}) for p in peers))

    def observe_term(self, term: int, sender: int) -> None:
        """Viu um termo maior que o meu: existe um líder mais novo. Se eu era líder, desço."""
        if term > self.term:
            self.term = term
            if self.is_leader:
                self.events.add("LÍDER", f"termo {term} visto no nó {sender}: deixo de ser líder")
                self.role = "follower"
                self.leader_id = None

    # ================================================================== heartbeat / falhas
    async def _heartbeat_loop(self) -> None:
        await asyncio.sleep(config.STARTUP_GRACE)
        if self.leader_id is None:
            self.election.trigger("nó iniciou e não conhece líder")
        while True:
            for peer in list(self.membership.peers.values()):
                if peer.node_id not in self._hb_inflight:
                    asyncio.create_task(self._heartbeat(peer))
            self._check_failures()
            await asyncio.sleep(config.HEARTBEAT_INTERVAL)

    async def _heartbeat(self, peer) -> None:
        self._hb_inflight.add(peer.node_id)
        try:
            resp = await self.transport.send(peer.url, "HEARTBEAT", {
                "role": self.role,
                "leader_id": self.leader_id,
                "commit_seq": self.log.commit_seq,
            })
            if resp and self.is_ready_leader and resp["type"] == "ACK":
                await self.replicator.maybe_sync(peer, resp["payload"])
        finally:
            self._hb_inflight.discard(peer.node_id)

    def _check_failures(self) -> None:
        self.membership.check_failures()
        if self.is_leader:
            return
        leader = self.membership.get(self.leader_id)
        if self.leader_id is not None and (leader is None or not leader.alive):
            self.events.add("FALHA", f"leiloeiro (nó {self.leader_id}) caiu")
            self.leader_id = None
        if self.leader_id is None:
            self.election.trigger("sem leiloeiro")

    # ================================================================== mensagens recebidas
    async def handle_message(self, env: dict) -> dict:
        self.clock.update(env["lamport"])  # regra 3 de Lamport
        sender, term, payload = int(env["sender"]), int(env["term"]), env.get("payload") or {}
        self.membership.touch(sender)
        msg_type = env["type"]
        if msg_type not in ("COORDINATOR", "HEARTBEAT"):
            self.observe_term(term, sender)
        handler = getattr(self, f"_on_{msg_type.lower()}", None)
        if handler is None:
            result, resp_type = {"error": f"tipo desconhecido: {msg_type}"}, "ERROR"
        else:
            result = await handler(sender, term, payload)
            resp_type = "ANSWER" if msg_type == "ELECTION" else "ACK"
        return {"type": resp_type, "sender": self.id, "term": self.term,
                "lamport": self.clock.tick(), "payload": result}

    def on_reply(self, env: dict) -> None:
        self.clock.update(env.get("lamport", 0))
        sender = int(env.get("sender", -1))
        self.membership.touch(sender)
        self.observe_term(int(env.get("term", 0)), sender)

    async def _on_hello(self, sender, term, p):
        return {"node_id": self.id, "public_url": config.PUBLIC_URL,
                "leader_id": self.leader_id, "term": self.term}

    async def _on_heartbeat(self, sender, term, p):
        claims_leader = p.get("role") in ("leader", "recovering") and p.get("leader_id") == sender
        if claims_leader:
            if self.is_leader:
                if (term, sender) > (self.term, self.id):
                    self.set_leader(sender, term)  # líder "mais novo" ou maior: eu desço
            elif term > self.term or (term == self.term and (self.leader_id is None or sender >= self.leader_id)):
                # termo maior vence; em empate de termo (duas eleições simultâneas), vence o maior ID
                self.set_leader(sender, term)
            if sender < self.id and not self.is_leader:
                self.election.trigger(f"líder atual (nó {sender}) tem ID menor que o meu")
        else:
            self.observe_term(term, sender)
            if sender == self.leader_id and not self.is_leader:
                # quem eu achava que era o líder diz que não é mais: esqueço e espero/eleijo outro
                self.events.add("LÍDER", f"nó {sender} informou que não é mais o leiloeiro")
                self.leader_id = None
        if self.leader_id == sender and self.synced_term == term:
            self.log.set_commit(p.get("commit_seq", 0))
        return {"last_seq": self.log.last_seq, "last_id": self.log.last_id,
                "commit_seq": self.log.commit_seq, "synced_term": self.synced_term}

    async def _on_election(self, sender, term, p):
        return self.election.on_election(sender)

    async def _on_coordinator(self, sender, term, p):
        return self.election.on_coordinator(sender, term)

    async def _on_replicate(self, sender, term, p):
        if term < self.term:
            return {"ok": False, "stale_term": True}
        self.set_leader(sender, term)
        if not self.log.accept(p["entry"], p.get("prev_id")):
            return {"ok": False, "need_snapshot": True}
        self.synced_term = term
        self.log.set_commit(p.get("commit_seq", 0))
        return {"ok": True}

    async def _on_snapshot(self, sender, term, p):
        if term < self.term:
            return {"ok": False, "stale_term": True}
        self.set_leader(sender, term)
        changed = self.log.install(p["entries"], p.get("commit_seq", 0))
        self.synced_term = term
        if changed:
            self.events.add("REPLICAÇÃO", f"snapshot do líder instalado: {self.log.last_seq} entradas, "
                                          f"hash {self.log.committed_hash()}")
        return {"ok": True, "changed": changed}

    async def _on_commit(self, sender, term, p):
        if sender == self.leader_id and self.synced_term == term:
            self.log.set_commit(p.get("commit_seq", 0))
        return {"commit_seq": self.log.commit_seq}

    async def _on_state_request(self, sender, term, p):
        return {"entries": self.log.entries, "commit_seq": self.log.commit_seq,
                "last_term": self.log.last_term, "last_seq": self.log.last_seq}

    async def _on_status(self, sender, term, p):
        return self.status()

    async def _on_bid_request(self, sender, term, bid):
        if not self.is_leader:
            return {"error": "not_leader", "leader_id": self.leader_id}
        if not self.is_ready_leader:
            return {"error": "not_ready"}
        return await self.leader_handle_bid(bid)

    async def _on_create_request(self, sender, term, data):
        if not self.is_leader:
            return {"error": "not_leader", "leader_id": self.leader_id}
        if not self.is_ready_leader:
            return {"error": "not_ready"}
        return await self.leader_create_auction(data)

    # ================================================================== lógica do leiloeiro
    async def leader_handle_bid(self, bid: dict) -> dict:
        """Executado SOMENTE no líder. Seção crítica por item, ordenada por Lamport."""
        existing = self.log.state.bids_by_id.get(bid["id"])
        if existing:
            return {"accepted": True, "bid": existing, "duplicate": True}
        key = order_key(bid["lamport"], bid["node_id"])
        info = {"bid_id": bid["id"], "bidder": bid["bidder"], "amount": bid["amount"], "node_id": bid["node_id"]}
        async with self.mutex.hold(bid["auction_id"], key, info):
            await asyncio.sleep(config.CS_DELAY)  # "trabalho" dentro da seção crítica
            existing = self.log.state.bids_by_id.get(bid["id"])
            if existing:
                return {"accepted": True, "bid": existing, "duplicate": True}
            reason = self.log.state.validate_bid(bid)
            if reason:
                self.events.add("LANCE", f"REJEITADO {bid['bidder']} R$ {bid['amount']:.2f} "
                                         f"(L={bid['lamport']}, nó {bid['node_id']}): {reason}")
                return {"accepted": False, "reason": reason, "bid": bid}
            try:
                entry = await self.replicator.commit("BID", bid, bid["lamport"])
            except NoQuorum as exc:
                return {"accepted": False, "reason": str(exc), "bid": bid}
            except NotLeader:
                return {"error": "not_leader"}
            self.events.add("LANCE", f"ACEITO {bid['bidder']} R$ {bid['amount']:.2f} "
                                     f"(L={bid['lamport']}, nó {bid['node_id']}) seq={entry['seq']}")
            return {"accepted": True, "bid": dict(bid, seq=entry["seq"])}

    async def leader_create_auction(self, data: dict) -> dict:
        if data["id"] in self.log.state.auctions:
            return {"created": True, "auction": self.log.state.summary(self.log.state.auctions[data["id"]])}
        data = dict(data, ends_at=time.time() + data["duration_s"])  # prazo definido pelo leiloeiro
        try:
            await self.replicator.commit("CREATE_AUCTION", data, data["lamport"])
        except NoQuorum as exc:
            return {"created": False, "reason": str(exc)}
        except NotLeader:
            return {"error": "not_leader"}
        self.events.add("LEILÃO", f"leilão '{data['title']}' criado ({data['duration_s']}s)")
        return {"created": True, "auction": self.log.state.summary(self.log.state.auctions[data["id"]])}

    async def _auction_closer_loop(self) -> None:
        """O leiloeiro encerra os leilões cujo prazo acabou (continua após troca de líder)."""
        while True:
            await asyncio.sleep(0.5)
            if not self.is_ready_leader:
                continue
            for auction_id in self.log.state.open_auctions_past_deadline():
                if auction_id not in self._closing:
                    self._closing.add(auction_id)
                    asyncio.create_task(self._close_auction(auction_id))

    async def _close_auction(self, auction_id: str) -> None:
        try:
            key = order_key(self.clock.tick(), self.id)
            async with self.mutex.hold(auction_id, key, {"action": "ENCERRAR", "node_id": self.id}):
                auction = self.log.state.auctions[auction_id]
                if auction["status"] != "open":
                    return
                winner = self.log.state.highest_bid(auction)
                await self.replicator.commit("CLOSE", {"auction_id": auction_id, "winner": winner}, key[0])
                who = f"{winner['bidder']} com R$ {winner['amount']:.2f}" if winner else "sem lances"
                self.events.add("LEILÃO", f"'{auction['title']}' ENCERRADO — vencedor: {who}")
        except (NoQuorum, NotLeader):
            pass  # tenta de novo na próxima volta do loop
        finally:
            self._closing.discard(auction_id)

    # ================================================================== entrada dos clientes
    async def submit_bid(self, auction_id: str, bidder: str, amount: float, request_id: str | None) -> dict:
        bid = {
            "id": request_id or uuid.uuid4().hex[:12],
            "auction_id": auction_id,
            "bidder": bidder.strip(),
            "amount": money(amount),
            "node_id": self.id,               # nó que recebeu o lance do cliente
            "lamport": self.clock.tick(),     # timestamp de Lamport do evento "lance recebido"
            "received_at": time.time(),
        }
        self.events.add("LANCE", f"recebido do cliente: {bid['bidder']} R$ {bid['amount']:.2f} (L={bid['lamport']})")
        result = await self._route_to_leader("BID_REQUEST", bid, self.leader_handle_bid)
        if result.get("accepted") and "seq" in result.get("bid", {}):
            await self._wait_commit(result["bid"]["seq"])
        return result

    async def submit_auction(self, title: str, description: str, start_price: float,
                             min_increment: float, duration_s: int) -> dict:
        data = {
            "id": uuid.uuid4().hex[:12],
            "title": title.strip(),
            "description": description.strip(),
            "start_price": money(start_price),
            "min_increment": money(min_increment),
            "duration_s": int(duration_s),
            "created_by_node": self.id,
            "lamport": self.clock.tick(),
        }
        result = await self._route_to_leader("CREATE_REQUEST", data, self.leader_create_auction)
        if result.get("created"):
            await self._wait_until(lambda: data["id"] in self.log.state.auctions)
        return result

    async def _route_to_leader(self, msg_type: str, payload: dict, local_handler) -> dict:
        """Entrega a operação ao leiloeiro; se ele cair no meio, espera a eleição e tenta de novo."""
        deadline = time.monotonic() + config.FORWARD_TIMEOUT
        while time.monotonic() < deadline:
            if self.is_ready_leader:
                result = await local_handler(payload)
                if result.get("error") != "not_leader":
                    return result
            elif not self.is_leader:
                leader = self.membership.get(self.leader_id)
                if leader is not None and leader.alive:
                    resp = await self.transport.send(leader.url, msg_type, payload, timeout=5.0)
                    if resp and resp["type"] == "ACK" and "error" not in resp["payload"]:
                        return resp["payload"]
            await asyncio.sleep(0.3)
        raise LeaderUnavailable("leiloeiro indisponível (eleição em andamento?); tente novamente")

    async def _wait_commit(self, seq: int, timeout: float = 1.5) -> None:
        await self._wait_until(lambda: self.log.commit_seq >= seq, timeout)

    async def _wait_until(self, cond, timeout: float = 1.5) -> None:
        end = time.monotonic() + timeout
        while not cond() and time.monotonic() < end:
            await asyncio.sleep(0.05)

    # ================================================================== observabilidade
    def status(self) -> dict:
        return {
            "node_id": self.id,
            "public_url": config.PUBLIC_URL,
            "role": self.role,
            "leader_id": self.leader_id,
            "term": self.term,
            "lamport": self.clock.value,
            "last_seq": self.log.last_seq,
            "commit_seq": self.log.commit_seq,
            "log_hash": self.log.committed_hash(),
            "cluster_size": self.membership.cluster_size(),
            "write_quorum": self.membership.write_quorum(),
            "election_running": self.election.running,
            "uptime_s": round(time.time() - self.started_at, 1),
            "peers": [
                {"node_id": p.node_id, "alive": p.alive, "public_url": p.public_url,
                 "last_seen_s": round(p.seconds_since_seen(), 1)}
                for p in sorted(self.membership.peers.values(), key=lambda p: p.node_id)
            ],
            "locks": self.mutex.snapshot() if self.is_leader else {},
            "events": self.events.recent(30),
        }
