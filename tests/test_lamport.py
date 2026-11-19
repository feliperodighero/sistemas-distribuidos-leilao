from node.lamport import LamportClock, order_key


def test_tick_incrementa():
    c = LamportClock()
    assert c.tick() == 1
    assert c.tick() == 2


def test_update_usa_maximo_mais_um():
    c = LamportClock(5)
    assert c.update(10) == 11   # mensagem "do futuro": pula para 10 + 1
    assert c.update(3) == 12    # mensagem antiga: só incrementa


def test_causalidade_entre_dois_nos():
    a, b = LamportClock(), LamportClock()
    envio = a.tick()            # a envia mensagem
    recebimento = b.update(envio)
    assert recebimento > envio  # envio -> recebimento  =>  L(envio) < L(recebimento)


def test_ordem_total_desempata_pelo_id_do_no():
    eventos = [order_key(7, 3), order_key(7, 1), order_key(5, 2)]
    assert sorted(eventos) == [(5, 2), (7, 1), (7, 3)]
