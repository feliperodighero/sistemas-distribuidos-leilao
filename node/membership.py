"""Descoberta de serviços e detector de falhas.

Descoberta: todos os nós estão na mesma rede Docker com o alias "leilao-cluster".
O DNS interno do Docker devolve o IP de todos os contêineres vivos com esse alias.
A cada DISCOVERY_INTERVAL o nó resolve o nome, manda HELLO para cada IP novo e
aprende o ID do nó que está ali. Nenhum endereço fica fixo no código, e um nó novo
(ex.: `docker compose --profile extra up node4`) é encontrado automaticamente.

Detector de falhas: cada contato bem-sucedido (heartbeat, resposta, qualquer mensagem)
atualiza `last_seen`. Se um nó fica FAILURE_TIMEOUT segundos sem contato, ele passa a
ser considerado suspeito (fora do ar).
"""
from __future__ import annotations

import asyncio
import socket
import time
from dataclasses import dataclass, field

from . import config


@dataclass
class Peer:
    node_id: int
    url: str                # endereço interno (http://IP:8000)
    public_url: str = ""    # endereço para navegadores (http://localhost:800X)
    last_seen: float = field(default_factory=time.monotonic)
    alive: bool = True

    def seconds_since_seen(self) -> float:
        return time.monotonic() - self.last_seen


class Membership:
    def __init__(self, node) -> None:
        self.node = node
        self.peers: dict[int, Peer] = {}
        self.ip_to_id: dict[str, int] = {}
        self.self_ips: set[str] = set()

    # ------------------------------------------------------------------ descoberta
    async def resolve(self) -> set[str]:
        loop = asyncio.get_running_loop()
        ips: set[str] = set()
        for host in config.DISCOVERY_HOSTS:
            try:
                infos = await loop.getaddrinfo(host, config.PEER_PORT, type=socket.SOCK_STREAM)
                ips.update(info[4][0] for info in infos)
            except socket.gaierror:
                pass
        return ips

    async def discover_once(self) -> None:
        ips = await self.resolve()
        to_probe = [ip for ip in ips if ip not in self.self_ips and self._needs_probe(ip)]
        await asyncio.gather(*(self.probe(ip) for ip in to_probe))

    def _needs_probe(self, ip: str) -> bool:
        """IP nunca visto, ou cujo nó está fora do ar (pode ter reiniciado com outro ID/IP)."""
        node_id = self.ip_to_id.get(ip)
        peer = self.peers.get(node_id) if node_id is not None else None
        return peer is None or not peer.alive

    async def probe(self, ip: str) -> None:
        url = f"http://{ip}:{config.PEER_PORT}"
        resp = await self.node.transport.send(url, "HELLO", {"public_url": config.PUBLIC_URL})
        if not resp:
            return
        node_id = int(resp["payload"]["node_id"])
        if node_id == self.node.id:
            self.self_ips.add(ip)
            return
        self.ip_to_id[ip] = node_id
        self.add_or_update(node_id, url, resp["payload"].get("public_url", ""))

    def add_or_update(self, node_id: int, url: str, public_url: str = "") -> None:
        peer = self.peers.get(node_id)
        if peer is None:
            self.peers[node_id] = Peer(node_id, url, public_url)
            self.node.events.add("DESCOBERTA", f"nó {node_id} encontrado em {url}")
            return
        if peer.url != url:
            self.node.events.add("DESCOBERTA", f"nó {node_id} mudou de endereço: {url}")
            peer.url = url
        if public_url:
            peer.public_url = public_url
        self.touch(node_id)

    async def discovery_loop(self) -> None:
        while True:
            await self.discover_once()
            await asyncio.sleep(config.DISCOVERY_INTERVAL)

    # ------------------------------------------------------------------ falhas
    def touch(self, node_id: int) -> None:
        peer = self.peers.get(node_id)
        if peer is None:
            return
        peer.last_seen = time.monotonic()
        if not peer.alive:
            peer.alive = True
            self.node.events.add("HEARTBEAT", f"nó {node_id} voltou a responder")

    def check_failures(self) -> list[int]:
        """Marca como suspeitos os nós sem contato recente. Retorna os que acabaram de cair."""
        newly_dead = []
        for peer in self.peers.values():
            if peer.alive and peer.seconds_since_seen() > config.FAILURE_TIMEOUT:
                peer.alive = False
                newly_dead.append(peer.node_id)
                self.node.events.add("HEARTBEAT", f"nó {peer.node_id} sem heartbeat há "
                                                  f"{peer.seconds_since_seen():.1f}s: considerado FORA DO AR")
        return newly_dead

    # ------------------------------------------------------------------ consultas
    def alive_peers(self) -> list[Peer]:
        return [p for p in self.peers.values() if p.alive]

    def get(self, node_id: int | None) -> Peer | None:
        return self.peers.get(node_id) if node_id is not None else None

    def cluster_size(self) -> int:
        """Todos os nós já vistos (vivos ou não) + este. Base para o quórum."""
        return len(self.peers) + 1

    def write_quorum(self) -> int:
        if config.WRITE_QUORUM.isdigit():
            return max(1, int(config.WRITE_QUORUM))
        return self.cluster_size() // 2 + 1
