from node.replication import ReplicatedLog


def entry(log: ReplicatedLog, kind="CREATE_AUCTION", term=1, i=0):
    data = {"id": f"a{i}", "title": "x", "start_price": 1.0, "min_increment": 1.0,
            "ends_at": 9e12, "duration_s": 60}
    return log.new_entry(term, kind, data, lamport=i, leader=3)


def test_seguidor_aceita_em_ordem_e_recusa_buraco():
    leader, follower = ReplicatedLog(), ReplicatedLog()
    e1, e2, e3 = entry(leader, i=1), entry(leader, i=2), entry(leader, i=3)
    assert follower.accept(e1, None)
    assert not follower.accept(e3, e2["id"])  # faltou a entrada 2 -> pede snapshot
    assert follower.accept(e2, e1["id"])
    assert follower.accept(e3, e2["id"])
    assert follower.accept(e3, e2["id"])       # reenvio é idempotente
    assert follower.last_seq == 3


def test_seguidor_recusa_historico_divergente():
    leader, follower = ReplicatedLog(), ReplicatedLog()
    e1 = entry(leader, i=1)
    entry(follower, i=99)                      # follower tem outra entrada na posição 1
    e2 = entry(leader, i=2)
    assert not follower.accept(e2, e1["id"])


def test_entrada_nao_confirmada_e_sobrescrita_por_novo_lider():
    old = ReplicatedLog()
    e1 = entry(old, i=1)
    old.set_commit(1)
    entry(old, term=1, i=2)                    # entrada 2 nunca confirmada (líder caiu)
    new_leader = ReplicatedLog()
    new_leader.install(old.entries[:1], 1)
    e2_new = entry(new_leader, term=2, i=7)
    assert old.accept(e2_new, e1["id"])        # substitui a entrada 2 antiga
    assert old.entries[1]["term"] == 2


def test_commit_aplica_somente_entradas_confirmadas():
    log = ReplicatedLog()
    entry(log, i=1); entry(log, i=2)
    log.set_commit(1)
    assert len(log.state.auctions) == 1
    log.set_commit(2)
    assert len(log.state.auctions) == 2


def test_snapshot_mais_curto_nao_apaga_entradas_novas():
    leader, follower = ReplicatedLog(), ReplicatedLog()
    e1, e2 = entry(leader, i=1), entry(leader, i=2)
    follower.accept(e1, None); follower.accept(e2, e1["id"])
    changed = follower.install(leader.entries[:1], 1)  # snapshot antigo chegou atrasado
    assert not changed and follower.last_seq == 2


def test_hash_igual_para_logs_iguais():
    a, b = ReplicatedLog(), ReplicatedLog()
    entry(a, i=1); a.set_commit(1)
    b.install(a.entries, 1)
    assert a.committed_hash() == b.committed_hash()
