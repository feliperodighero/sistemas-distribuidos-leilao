"""Log replicado (primário-backup com quórum).

Toda alteração do leilão (criar leilão, lance aceito, encerramento) vira uma ENTRADA
de log. Só o líder cria entradas. Uma entrada é considerada CONFIRMADA (commit)
quando a maioria dos nós a gravou. Os nós só aplicam ao estado do leilão as entradas
confirmadas (até `commit_seq`).

Cada entrada tem:
  seq     posição no log (1, 2, 3, ...)
  term    termo (mandato) do líder que a criou
  id      identificador único (para comparar logs)
  kind    CREATE_AUCTION | BID | CLOSE
  data    conteúdo
  lamport timestamp de Lamport do evento

Verificação de consistência (inspirada no Raft): ao replicar a entrada `seq`, o líder
manda junto o `id` da entrada `seq-1` (prev_id). O seguidor só aceita se o seu log
tiver essa mesma entrada; caso contrário pede um SNAPSHOT (log completo).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import uuid

from .auction import AuctionState
from . import config


class NoQuorum(Exception):
    def __init__(self, acks: int, needed: int):
        super().__init__(f"sem quórum: {acks} de {needed} confirmações necessárias")
        self.acks, self.needed = acks, needed


class NotLeader(Exception):
    pass


class ReplicatedLog:
    def __init__(self) -> None:
        self.entries: list[dict] = []
        self.commit_seq = 0
        self.applied = 0
        self.state = AuctionState()

    # ------------------------------------------------------------------ propriedades
    @property
    def last_seq(self) -> int:
        return len(self.entries)

    @property
    def last_term(self) -> int:
        return self.entries[-1]["term"] if self.entries else 0

    @property
    def last_id(self) -> str | None:
        return self.entries[-1]["id"] if self.entries else None

    def id_at(self, seq: int) -> str | None:
        return self.entries[seq - 1]["id"] if 1 <= seq <= len(self.entries) else None

    # ------------------------------------------------------------------ líder
    def new_entry(self, term: int, kind: str, data: dict, lamport: int, leader: int) -> dict:
        entry = {
            "seq": self.last_seq + 1,
            "term": term,
            "id": uuid.uuid4().hex[:12],
            "kind": kind,
            "data": data,
            "lamport": lamport,
            "leader": leader,
        }
        self.entries.append(entry)
        return entry

    # ------------------------------------------------------------------ seguidor
    def accept(self, entry: dict, prev_id: str | None) -> bool:
        """Aplica uma entrada vinda do líder. Retorna False se o log está divergente/atrasado."""
        seq = entry["seq"]
        if seq - 1 > len(self.entries):
            return False  # falta entrada anterior
        if seq > 1 and self.id_at(seq - 1) != prev_id:
            return False  # histórico diferente do líder
        if self.id_at(seq) == entry["id"]:
            return True  # duplicada (reenvio)
        self._truncate(seq - 1)  # descarta entradas não confirmadas de um líder antigo
        self.entries.append(entry)
        return True

    def install(self, entries: list[dict], commit_seq: int) -> bool:
        """Instala um snapshot (log completo do líder). Retorna True se o log mudou."""
        is_prefix = len(entries) <= len(self.entries) and all(
            e["id"] == self.entries[i]["id"] for i, e in enumerate(entries)
        )
        changed = False
        if not is_prefix:
            self.entries = [dict(e) for e in entries]
            self.commit_seq = 0
            self._rebuild()
            changed = True
        self.set_commit(commit_seq)
        return changed

    # ------------------------------------------------------------------ commit / estado
    def set_commit(self, seq: int) -> None:
        seq = min(seq, len(self.entries))
        if seq < self.applied:
            self.commit_seq = seq
            self._rebuild()
            return
        self.commit_seq = max(self.commit_seq, seq)
        while self.applied < self.commit_seq:
            self.state.apply(self.entries[self.applied])
            self.applied += 1

    def truncate(self, n: int) -> None:
        self._truncate(n)

    def _truncate(self, n: int) -> None:
        del self.entries[n:]
        if self.commit_seq > n:
            self.commit_seq = n
        if self.applied > n:
            self._rebuild()

    def _rebuild(self) -> None:
        self.state = AuctionState()
        self.applied = 0
        target, self.commit_seq = self.commit_seq, 0
        self.set_commit(target)

    def committed_hash(self) -> str:
        """Resumo do log confirmado: nós com o mesmo hash têm exatamente o mesmo estado."""
        ids = [e["id"] for e in self.entries[: self.commit_seq]]
        return hashlib.sha256(json.dumps(ids).encode()).hexdigest()[:10]


class Replicator:
    """Lógica de replicação executada pelo líder (e recuperação ao assumir)."""

    def __init__(self, node) -> None:
        self.node = node
        self.append_lock = asyncio.Lock()

    @property
    def log(self) -> ReplicatedLog:
        return self.node.log

    async def commit(self, kind: str, data: dict, lamport: int) -> dict:
        """Cria uma entrada, replica para os seguidores e confirma ao atingir o quórum."""
        node = self.node
        async with self.append_lock:
            if not node.is_ready_leader:
                raise NotLeader()
            prev_id = self.log.last_id
            entry = self.log.new_entry(node.term, kind, data, lamport, node.id)
            peers = node.membership.alive_peers()
            results = await asyncio.gather(*(self._replicate_to(p, entry, prev_id) for p in peers))
            if node.term != entry["term"] or not node.is_ready_leader:
                # perdi a liderança enquanto replicava (apareceu um líder mais novo)
                self.log.truncate(entry["seq"] - 1)
                raise NotLeader()
            acks = 1 + sum(1 for ok in results if ok)  # 1 = o próprio líder
            needed = node.membership.write_quorum()
            if acks < needed:
                self.log.truncate(entry["seq"] - 1)
                node.events.add("REPLICAÇÃO", f"entrada {entry['seq']} ({kind}) NÃO confirmada: "
                                              f"{acks}/{needed} nós")
                raise NoQuorum(acks, needed)
            self.log.set_commit(entry["seq"])
            node.events.add("REPLICAÇÃO", f"entrada {entry['seq']} ({kind}) confirmada por "
                                          f"{acks}/{node.membership.cluster_size()} nós")
            asyncio.create_task(self._broadcast_commit())
            return entry

    async def _replicate_to(self, peer, entry: dict, prev_id: str | None) -> bool:
        resp = await self.node.transport.send(peer.url, "REPLICATE", {
            "entry": entry, "prev_id": prev_id, "commit_seq": self.log.commit_seq,
        })
        if resp is None:
            return False
        payload = resp["payload"]
        if payload.get("ok"):
            return True
        if payload.get("need_snapshot"):
            return await self.send_snapshot(peer)
        return False

    async def send_snapshot(self, peer) -> bool:
        resp = await self.node.transport.send(peer.url, "SNAPSHOT", {
            "entries": self.log.entries, "commit_seq": self.log.commit_seq,
        }, timeout=max(2.0, config.RPC_TIMEOUT))
        ok = bool(resp and resp["payload"].get("ok"))
        if ok and resp["payload"].get("changed"):
            self.node.events.add("REPLICAÇÃO", f"snapshot ({self.log.last_seq} entradas) enviado ao nó {peer.node_id}")
        return ok

    async def _broadcast_commit(self) -> None:
        peers = self.node.membership.alive_peers()
        await asyncio.gather(*(self.node.transport.send(p.url, "COMMIT", {"commit_seq": self.log.commit_seq})
                               for p in peers))

    async def maybe_sync(self, peer, info: dict) -> None:
        """Chamado pelo líder com a resposta de heartbeat de um seguidor: corrige atrasos."""
        f_seq, f_id = info.get("last_seq", 0), info.get("last_id")
        in_sync = (
            info.get("synced_term") == self.node.term
            and f_seq <= self.log.last_seq
            and self.log.id_at(f_seq) == f_id
            and f_seq >= self.log.commit_seq
        )
        if not in_sync and not self.append_lock.locked():
            await self.send_snapshot(peer)

    async def recover(self) -> None:
        """Novo líder: coleta o log de todos os nós vivos e adota o mais atualizado.

        Uma entrada confirmada está em uma maioria de nós; como a maioria precisa estar
        viva para o sistema aceitar escritas, pelo menos um nó vivo tem cada entrada
        confirmada. Escolhemos o log com maior (termo da última entrada, tamanho).
        """
        node = self.node
        peers = node.membership.alive_peers()
        responses = await asyncio.gather(*(node.transport.send(p.url, "STATE_REQUEST", {}, timeout=2.0)
                                           for p in peers))
        best_entries, best_from = self.log.entries, node.id
        best_key = (self.log.last_term, self.log.last_seq)
        for peer, resp in zip(peers, responses):
            if not resp:
                continue
            p = resp["payload"]
            key = (p["last_term"], p["last_seq"])
            if key > best_key:
                best_key, best_entries, best_from = key, p["entries"], peer.node_id
        if best_from != node.id:
            self.log.install(best_entries, 0)
        self.log.set_commit(self.log.last_seq)
        node.events.add("RECUPERAÇÃO", f"log adotado do nó {best_from}: {self.log.last_seq} entradas, "
                                        f"hash {self.log.committed_hash()}")
        await asyncio.gather(*(self.send_snapshot(p) for p in peers))
