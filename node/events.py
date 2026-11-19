"""Registro de eventos do nó: aparece no `docker compose logs` e no painel."""
import logging
import time
from collections import deque

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)  # não poluir o log com cada requisição
log = logging.getLogger("leilao")


class EventLog:
    def __init__(self, node_id: int, clock, maxlen: int = 80):
        self.node_id = node_id
        self.clock = clock
        self.items: deque[dict] = deque(maxlen=maxlen)
        self.counter = 0  # desempate local para eventos com o mesmo Lamport

    def add(self, kind: str, message: str) -> None:
        self.counter += 1
        ev = {
            "node_id": self.node_id,
            "n": self.counter,
            "time": time.time(),
            "lamport": self.clock.value,
            "kind": kind,
            "message": message,
        }
        self.items.append(ev)
        log.info("[nó %s | L=%-5s] %-11s %s", self.node_id, ev["lamport"], kind, message)

    def recent(self, n: int = 30) -> list[dict]:
        return list(self.items)[-n:]
