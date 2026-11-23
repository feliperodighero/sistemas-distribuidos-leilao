# Roteiro da apresentação (25/11 · 8 min de demo + 4 min de perguntas)

## Antes de entrar (pelo menos 10 min antes)

1. Abra o Docker Desktop, rode `docker compose up -d --build` e espere `docker compose ps` mostrar os 3 nós `healthy`.
2. Abra **http://localhost:8001** (modo apresentação) em tela cheia (F11) no projetor. Em outra aba, deixe o terminal na pasta do projeto, com fonte grande.
3. Clique em **Novo lote** para abrir um lote de 10 minutos (o anterior pode ter acabado).
4. Se quiser, troque os nomes dos jogadores (Ana, Beto, Carla) pelos nomes do grupo: clique no nome e digite.
5. Plano B: o vídeo de 3–4 min gravado no ensaio, aberto em outra aba.

## Roteiro (8 min)

| Tempo | Quem | O que fazer na tela | O que falar |
|---|---|---|---|
| 0:00–1:00 | Felipe | Slide da arquitetura | 3 nós iguais em contêineres; qualquer um recebe lances; o leiloeiro é eleito; HTTP/JSON com envelope único; descoberta pelo DNS do Docker, sem IP no código. |
| 1:00–2:00 | Leonardo | Modo apresentação parado, só olhando a mesa | O nó 3 é o leiloeiro porque tem o maior ID (Bully). As bolinhas pequenas são heartbeats: todos perguntam "está vivo?" a todos, a cada 1 s. Cada nó mostra o relógio de Lamport subindo a cada mensagem e o mesmo log/hash. |
| 2:00–3:00 | Enzo | Ana e Beto dão lances alternados (botões "Lance") | O jogador fala com o seu nó; o nó carimba (L, nó) e manda "lance" ao leiloeiro; o leiloeiro manda "replica" e só confirma com a maioria ("commit"). O log ganha um bloco novo igual em todos os nós. |
| 3:00–4:30 | Bernardo | Ligar **Câmera lenta** → **Todos dão o mesmo lance** | Três lances iguais ao mesmo tempo, um por nó. O leiloeiro coloca na fila (exclusão mútua: um lance por vez no lote) e a fila anda pelo menor (L, nó), não por quem chegou primeiro. Só um é aceito; os outros são recusados com o novo mínimo. Desligar a câmera lenta. |
| 4:30–6:30 | Felipe | **Derrubar o leiloeiro** e, logo depois, um lance da Ana | O nó 3 para de responder: as bolinhas para ele ficam vermelhas. Em 3 s os outros percebem (heartbeat), o nó 1 manda "eleição", o nó 2 vence (termo novo), recupera o log e fica pronto. O lance enviado durante a eleição esperou e foi aceito. Mostrar log e hash iguais. |
| 6:30–7:30 | Leonardo | **Religar nós derrubados** | O nó 3 volta com memória vazia, recebe a "cópia do log" e, por ter ID maior, retoma a liderança (Bully). Os três ficam com o mesmo hash. |
| 7:30–8:00 | Enzo | (Se sobrar tempo) No terminal: `docker compose stop node2 node3`, depois um lance | Falha real de contêiner. Com 1 de 3 nós vivo não há maioria: o lance é recusado ("sem quórum"). Escolhemos consistência (CP). Depois: `docker compose start node2 node3`. |

> Se o professor pedir para derrubar "de verdade", use o terminal: `docker compose stop node3` faz o mesmo que o botão, mas parando o contêiner. A página mostra o nó fora do ar do mesmo jeito; para religar, `docker compose start node3`.

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

**O botão "Derrubar" é uma falha de verdade?**
Ele simula a queda do processo: o nó para de enviar e de responder qualquer mensagem, e ao religar volta sem memória. Para os outros nós é idêntico a um contêiner parado: eles só percebem pelo heartbeat. Por isso também mostramos `docker compose stop`, que para o contêiner de fato.

**Vocês encontraram algum bug nos testes?**
Dois. (1) Dois nós religados juntos se elegiam com o mesmo termo; resolvemos com desempate por ID. (2) Um líder que voltava com memória vazia podia se eleger antes de descobrir os vizinhos e fazer as réplicas "esquecerem" lances confirmados. Corrigimos em três camadas: espera de descoberta antes da eleição, o novo líder redescobre os vizinhos antes de recuperar, e as réplicas recusam apagar uma entrada confirmada (respondem `ahead`, e o líder recupera o log de novo).

**Qual o limite do sistema?**
Estado só em memória (se todos caírem juntos, perde-se tudo; dá para gravar o log em disco); o líder é gargalo de escrita; o prazo do leilão usa o relógio físico do líder; e um nó novo com ID maior sempre toma a liderança (característica do Bully).
