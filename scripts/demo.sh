#!/usr/bin/env bash
# Atalhos para a apresentação. Uso: ./scripts/demo.sh <comando>
#
#   subir         constrói e sobe os nós 1, 2 e 3
#   estado        mostra papel, termo, commit e hash de cada nó
#   lider         mostra quem é o leiloeiro
#   derrubar N    para o nó N (docker compose stop)   ex.: derrubar 3
#   matar N       mata o nó N sem aviso (docker kill)
#   pausar N      congela o nó N (simula travamento; não responde heartbeat)
#   retomar N     descongela o nó N
#   voltar N      sobe de novo o nó N
#   no4           adiciona o nó 4 com o cluster rodando (descoberta automática)
#   logs          acompanha os eventos de todos os nós
#   descer        derruba tudo
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1:-}" in
  subir)    docker compose up -d --build && sleep 6 && python3 scripts/check_consistency.py ;;
  estado)   python3 scripts/check_consistency.py ;;
  lider)    curl -s localhost:8001/api/cluster || curl -s localhost:8002/api/cluster; echo ;;
  derrubar) docker compose stop "node$2" ;;
  matar)    docker compose kill "node$2" ;;
  pausar)   docker compose pause "node$2" ;;
  retomar)  docker compose unpause "node$2" ;;
  voltar)   docker compose start "node$2" ;;
  no4)      docker compose --profile extra up -d node4 ;;
  logs)     docker compose logs -f --no-log-prefix | grep -v -E "^$" ;;
  descer)   docker compose --profile extra down ;;
  *)        sed -n '2,16p' "$0" ;;
esac
