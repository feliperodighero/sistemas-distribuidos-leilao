# Leilão on-line distribuído

Trabalho final de Sistemas Distribuídos (2026) — Leonardo Borghelot, Enzo Mariga, Bernardo Kujava e Felipe Zarichta.

Vários servidores recebem lances ao mesmo tempo e **todos concordam sobre a ordem dos lances e o vencedor**, mesmo quando o leiloeiro (líder) cai no meio do leilão.

| Requisito | Como foi resolvido | Onde está no código |
|---|---|---|
| 3+ nós em contêineres | `node1`, `node2`, `node3` (e `node4` opcional) no Docker Compose | `docker-compose.yml` |
| Comunicação documentada | HTTP/JSON com envelope único em `POST /internal/msg` | `node/transport.py`, `docs/PROTOCOLO.md` |
| Descoberta de serviços | alias DNS `leilao-cluster` na rede Docker + mensagem `HELLO` | `node/membership.py` |
| Eleição + heartbeat | Bully com termos; heartbeat de 1 s, falha após 3 s | `node/election.py`, `node/node.py` |
| Coordenação / ordenação | Relógio de Lamport em toda mensagem + exclusão mútua por item (coordenador = leiloeiro, fila de prioridade por `(L, nó)`) | `node/lamport.py`, `node/mutex.py` |
| Replicação e consistência | Log replicado com confirmação por maioria e recuperação do log no novo líder | `node/replication.py` |
| Falha ao vivo | `docker compose stop node3` → o nó 2 assume em ~4 s sem perder lances | `scripts/demo.sh`, `docs/ROTEIRO_DEMO.md` |
| Ponto extra: observabilidade | Painel com nós, líder, termos, relógios de Lamport, fila do mutex e linha do tempo | `node/static/dashboard.html` |

## Como subir

Pré-requisito: Docker com o plugin Compose (Docker Desktop no Windows/macOS).

```bash
docker compose up --build
```

Abra no navegador:

- Cliente do leilão: http://localhost:8001, http://localhost:8002 e http://localhost:8003. Cada aba usa um nó; abra duas para ter dois clientes.
- Painel do cluster: http://localhost:8001/dashboard

Em cerca de 5 s os nós se descobrem e o nó 3 (maior ID) é eleito leiloeiro.

### Derrubar o líder e ver a recuperação

```bash
docker compose stop node3     # o nó 2 assume em ~4 s; nenhum lance confirmado se perde
docker compose start node3    # o nó 3 volta, sincroniza o log e retoma a liderança (Bully)
```

Outros cenários: `docker compose pause node3` (nó travado), `docker compose kill node3` (queda abrupta), `docker compose stop node2 node3` (sem maioria: o sistema passa a recusar lances), `docker compose --profile extra up -d node4` (nó novo descoberto sozinho).

Atalhos para a apresentação: `./scripts/demo.sh` (lista os comandos).

### Cliente de linha de comando

Usa só a biblioteca padrão do Python e troca de nó sozinho se o atual cair.

```bash
python client/cli.py status
python client/cli.py criar "Notebook" --preco 100 --incremento 10 --duracao 180
python client/cli.py listar
python client/cli.py lance <id> 150 --nome Felipe
python client/cli.py acompanhar <id>
```

### Verificar consistência

```bash
python scripts/check_consistency.py
```

Mostra papel, termo, commit, relógio de Lamport e o **hash do log confirmado** de cada nó. Hash igual em todos significa estado idêntico.

## Testes

```bash
pip install -r requirements-dev.txt
pytest                                                # 18 testes unitários (Lamport, leilão, log, mutex)

docker compose up -d --build
LEILAO_INTEGRACAO=1 pytest tests/test_integracao.py -v   # cluster real: queda do líder, concorrência, quórum
```

No Windows (PowerShell): `$env:LEILAO_INTEGRACAO=1; pytest tests/test_integracao.py -v`.

## Estrutura

```
node/
  main.py          API HTTP (rotas dos clientes e rota interna)
  node.py          o nó: papéis, heartbeat, tratamento das mensagens, lógica do leiloeiro
  election.py      algoritmo Bully
  membership.py    descoberta via DNS e detector de falhas
  lamport.py       relógio de Lamport
  mutex.py         exclusão mútua por item (coordenador central + fila por Lamport)
  replication.py   log replicado, quórum, snapshot e recuperação
  auction.py       regras do leilão (máquina de estados determinística)
  transport.py     envio de mensagens (envelope + Lamport)
  config.py        parâmetros por variável de ambiente
  static/          cliente web e painel
client/cli.py      cliente de linha de comando
scripts/           check_consistency.py, demo.sh
tests/             testes unitários e de integração
docs/              PROTOCOLO.md, ROTEIRO_DEMO.md
```

## Uso de IA

O grupo usou o Claude (Anthropic) como apoio na implementação e na redação da documentação, conforme declarado no relatório. Todo o código foi revisado, testado e é explicado pelo grupo.
