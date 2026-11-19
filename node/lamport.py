"""Relógio lógico de Lamport.

Regras (Lamport, 1978):
  1. Antes de um evento local (ex.: receber um lance de um cliente) ou de enviar
     uma mensagem, o nó incrementa o relógio: L = L + 1.
  2. Toda mensagem leva o valor atual de L.
  3. Ao receber uma mensagem com valor Lm, o nó faz L = max(L, Lm) + 1.

Com isso, se o evento a "aconteceu antes" de b (a -> b), então L(a) < L(b).
Para ter uma ORDEM TOTAL (todos os nós concordam), desempatamos pelo ID do nó:
o par (L, node_id) é comparado lexicograficamente.

Não é preciso trava (lock): o nó roda em um único event loop asyncio,
então essas operações nunca são executadas em paralelo.
"""


class LamportClock:
    def __init__(self, start: int = 0):
        self._time = start

    @property
    def value(self) -> int:
        return self._time

    def tick(self) -> int:
        """Evento local ou envio de mensagem."""
        self._time += 1
        return self._time

    def update(self, received: int) -> int:
        """Recebimento de mensagem com timestamp `received`."""
        self._time = max(self._time, int(received)) + 1
        return self._time


def order_key(lamport: int, node_id: int) -> tuple[int, int]:
    """Chave de ordem total: primeiro o relógio de Lamport, depois o ID do nó."""
    return (int(lamport), int(node_id))
