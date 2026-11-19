# Roteiro da apresentação (25/11 · 8 min de demo + 4 min de perguntas)

## Antes de entrar (pelo menos 10 min antes)

1. `docker compose up -d --build` e esperar `./scripts/demo.sh estado` mostrar **CONSISTENTE** com o nó 3 como leiloeiro.
2. Abrir quatro janelas:
   - **Navegador A:** http://localhost:8001, com o nome "Leonardo" no campo de nome.
   - **Navegador B:** http://localhost:8002, com o nome "Enzo".
   - **Painel:** http://localhost:8001/dashboard (é o que fica no projetor na maior parte do tempo).
   - **Terminal:** na pasta do projeto, com fonte grande.
3. Criar um leilão de **10 minutos** ("Notebook Dell i7", inicial R$ 1.500, incremento R$ 50) para não acabar no meio da demo.
4. Plano B: o vídeo de 3–4 min gravado no ensaio, aberto em outra aba.

> Não use o nó 3 nos navegadores: ele vai ser derrubado. Os clientes estão nos nós 1 e 2.

## Roteiro (8 min)

| Tempo | Quem | O que fazer | O que falar |
|---|---|---|---|
| 0:00–1:00 | Felipe | Slide da arquitetura | 3 nós iguais em contêineres; qualquer um recebe lances; o leiloeiro é eleito; HTTP/JSON com envelope único; descoberta por DNS do Docker, sem IP no código. |
| 1:00–2:00 | Leonardo | Painel | Os 3 nós com o mesmo hash; o nó 3 é leiloeiro porque tem o maior ID (Bully); termo; relógio de Lamport de cada nó; heartbeat a cada 1 s. |
| 2:00–3:30 | Enzo | Navegadores A e B dando lances alternados | Cada lance tem `(L, nó)`; o nó que recebe encaminha ao leiloeiro; só confirma quando a maioria gravou. Mostrar o histórico igual nas duas abas. |
| 3:30–4:30 | Bernardo | Painel → "Teste de concorrência" | Três lances iguais ao mesmo tempo, um por nó. A fila do mutex ordena por Lamport: só um é aceito, o de menor `(L, nó)`, mesmo que tenha chegado por último. Apontar na linha do tempo. |
| 4:30–6:30 | Felipe | Terminal: `./scripts/demo.sh derrubar 3`, depois um lance no navegador A logo em seguida | Em ~3 s sem heartbeat o nó 3 é dado como fora do ar; o nó 1 manda ELECTION; o nó 2 vence com termo novo, recupera o log e fica pronto. O lance enviado durante a eleição ficou esperando e foi aceito. O hash continua igual e nenhum lance se perdeu. |
| 6:30–7:30 | Leonardo | `./scripts/demo.sh voltar 3` | O nó 3 volta vazio, recebe o log (snapshot) e, por ter ID maior, retoma a liderança (Bully). Hash igual nos 3. |
| 7:30–8:00 | Enzo | (Se sobrar tempo) `./scripts/demo.sh derrubar 2` com o 3 também fora | Sem maioria o sistema recusa lances: escolhemos consistência (CP). Volte os nós depois. |

## Perguntas prováveis (todos precisam saber responder)

**Por que Bully e não Ring?**
Os nós se conhecem pela descoberta (todos falam com todos), o cluster é pequeno e o Bully elege em uma rodada (ELECTION → ANSWER → COORDINATOR). O custo O(n²) de mensagens não importa com 3–4 nós.

**Como vocês detectam falha?**
Todo nó manda HEARTBEAT para todos a cada 1 s. Qualquer resposta ou mensagem atualiza `last_seen`. Com mais de 3 s sem contato, o nó é marcado como fora do ar; se for o líder, começa a eleição. Sem resposta não dá para distinguir nó lento de nó morto, por isso usamos timeout.

**O que é o termo? Para que serve?**
É o número do mandato do líder (cada eleição vencida soma 1). Seguidores recusam REPLICATE de termo menor, então um líder antigo que "acordou" (por exemplo, depois de `docker pause`) não consegue escrever. Ao ver um termo maior, ele desce.

**Onde entra o relógio de Lamport?**
Em toda mensagem: incrementa ao enviar e faz `max(L, recebido) + 1` ao receber. Cada lance recebe o timestamp `(L, nó)` no nó de entrada. Esse par dá uma ordem total que todos os nós calculam igual. Ele define a prioridade na fila do mutex, desempata lances de mesmo valor e ordena a linha do tempo do painel.

**Lamport não garante a ordem real dos lances, garante?**
Não. Garante que, se A causou B, então L(A) < L(B). Para eventos concorrentes, a ordem é arbitrária, mas **igual para todos**, que é o que o leilão precisa: todos concordarem sobre quem venceu. Relógio vetorial detectaria concorrência, mas não daria ordem total.

**Como funciona a exclusão mútua?**
Algoritmo centralizado: o leiloeiro é o coordenador, com um lock por item. O BID_REQUEST é o pedido; o lance espera numa fila de prioridade por `(L, nó)` com uma janela de 150 ms para pedidos concorrentes se ordenarem pelo relógio lógico. Dentro da seção crítica: validar → replicar → liberar. Sem isso, dois lances poderiam ser validados contra o mesmo "maior lance" e os dois seriam aceitos. Itens diferentes não se bloqueiam.

**E se o coordenador do mutex cair?**
O lock está na memória do líder, então some com ele. O pedido em andamento não foi confirmado (não chegou à maioria) e o nó de origem reenvia ao novo líder com o mesmo `id`. Se ele já tinha sido confirmado, o novo líder reconhece o `id` e não duplica.

**Que consistência vocês oferecem?**
Escritas são linearizáveis: um único líder ordena tudo e só confirma com maioria. Leituras são locais em cada nó, então podem ficar alguns milissegundos atrás do líder. Quem deu o lance espera seu nó aplicar o commit antes de receber a resposta, garantindo "leia o que escreveu".

**CAP: vocês são CP ou AP?**
CP. Sem maioria (por exemplo, 1 de 3 nós vivos), o sistema recusa lances em vez de aceitar algo que poderia divergir. Num leilão, aceitar lances em dois lados de uma partição poderia gerar dois vencedores. A disponibilidade cai durante os ~4 s de eleição e quando não há maioria.

**Como o novo líder sabe o estado?**
Pede o log a todos os vivos (STATE_REQUEST) e fica com o de maior (termo da última entrada, tamanho). Como toda entrada confirmada está numa maioria, algum nó vivo a tem. Depois manda SNAPSHOT para todos e só então aceita lances.

**E se dois nós se elegerem ao mesmo tempo?**
Pode acontecer quando nós sobem juntos. A regra é: termo maior vence; em empate de termo, vence o maior ID. O líder perdedor desce ao receber o heartbeat do outro. O teste de quórum pegou exatamente esse caso, e a correção está em `node.py` (`_on_heartbeat`) e em `election.py`.

**Como os nós se encontram sem IP fixo?**
Todos os contêineres têm o alias `leilao-cluster` na rede Docker. O DNS do Docker devolve o IP de todos, e o nó manda HELLO para cada IP para saber o ID. Por isso o nó 4 entra sozinho.

**Qual o limite do sistema?**
Estado só em memória (se todos caírem juntos, perde-se tudo; dá para gravar o log em disco); o líder é gargalo de escrita; o prazo do leilão usa o relógio físico do líder; e um nó novo com ID maior sempre toma a liderança (característica do Bully).
