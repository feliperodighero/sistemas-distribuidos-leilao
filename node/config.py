"""Configuração do nó, lida de variáveis de ambiente (definidas no docker-compose.yml).

Nenhum endereço de outro nó aparece aqui: os nós se encontram pelo nome DNS
compartilhado em DISCOVERY_HOSTS (alias de rede do Docker).
"""
import os


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


# Identidade do nó. O ID é usado pelo algoritmo Bully (maior ID vivo vira líder).
NODE_ID = int(os.getenv("NODE_ID", "1"))

# Porta HTTP em que o nó escuta (dentro do contêiner).
PORT = int(os.getenv("PORT", "8000"))

# Porta usada para falar com os outros nós (igual em todos os contêineres).
PEER_PORT = int(os.getenv("PEER_PORT", str(PORT)))

# Nomes DNS que resolvem para os IPs dos nós do cluster (separados por vírgula).
# No Docker Compose, todos os nós têm o alias "leilao-cluster" na mesma rede,
# então resolver esse nome devolve o IP de todos os contêineres vivos.
DISCOVERY_HOSTS = [h.strip() for h in os.getenv("DISCOVERY_HOSTS", "leilao-cluster").split(",") if h.strip()]

# URL pela qual o navegador (fora do Docker) alcança este nó. Usada só pelos clientes.
PUBLIC_URL = os.getenv("PUBLIC_URL", f"http://localhost:{PORT}")

# Tempos (segundos)
DISCOVERY_INTERVAL = _float("DISCOVERY_INTERVAL", 2.0)    # de quanto em quanto tempo resolve o DNS
HEARTBEAT_INTERVAL = _float("HEARTBEAT_INTERVAL", 1.0)    # período do heartbeat
FAILURE_TIMEOUT = _float("FAILURE_TIMEOUT", 3.0)          # sem heartbeat por esse tempo => nó suspeito
ELECTION_TIMEOUT = _float("ELECTION_TIMEOUT", 1.5)        # espera por ANSWER no Bully
COORDINATOR_TIMEOUT = _float("COORDINATOR_TIMEOUT", 4.0)  # espera por COORDINATOR depois de um ANSWER
STARTUP_GRACE = _float("STARTUP_GRACE", 3.0)              # tempo para descobrir vizinhos antes da 1ª eleição
RPC_TIMEOUT = _float("RPC_TIMEOUT", 1.0)                  # timeout padrão de uma mensagem entre nós
FORWARD_TIMEOUT = _float("FORWARD_TIMEOUT", 15.0)         # quanto um nó insiste em entregar um lance ao líder

# Exclusão mútua: janela de espera (hold-back) antes de conceder o item, para que pedidos
# concorrentes que chegam quase juntos sejam ordenados pelo timestamp de Lamport.
MUTEX_HOLDBACK = _float("MUTEX_HOLDBACK", 0.15)
# Tempo simulado de processamento dentro da seção crítica (deixa a fila visível no painel).
CS_DELAY = _float("CS_DELAY", 0.25)

# Quórum de escrita: "majority" (padrão) ou um número inteiro de nós.
WRITE_QUORUM = os.getenv("WRITE_QUORUM", "majority")
