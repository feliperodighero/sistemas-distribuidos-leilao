import random
import time

from node.auction import AuctionState
from node.replication import ReplicatedLog


def make_log(n_bids: int = 0) -> ReplicatedLog:
    log = ReplicatedLog()
    log.new_entry(1, "CREATE_AUCTION", {
        "id": "a1", "title": "Notebook", "start_price": 100.0, "min_increment": 10.0,
        "ends_at": time.time() + 60, "duration_s": 60,
    }, lamport=1, leader=3)
    for i in range(n_bids):
        log.new_entry(1, "BID", {"id": f"b{i}", "auction_id": "a1", "bidder": f"p{i}",
                                 "amount": 100.0 + 10 * i, "node_id": 1 + i % 3, "lamport": 10 + i}, 10 + i, 3)
    log.set_commit(log.last_seq)
    return log


def test_primeiro_lance_precisa_do_preco_inicial():
    st = make_log().state
    assert st.validate_bid({"auction_id": "a1", "bidder": "x", "amount": 99}) is not None
    assert st.validate_bid({"auction_id": "a1", "bidder": "x", "amount": 100}) is None


def test_lance_precisa_superar_maior_mais_incremento():
    st = make_log(2).state  # maior lance = 110
    assert st.min_next_bid(st.auctions["a1"]) == 120
    assert "mínimo" in st.validate_bid({"auction_id": "a1", "bidder": "x", "amount": 115})
    assert st.validate_bid({"auction_id": "a1", "bidder": "x", "amount": 120}) is None


def test_leilao_encerrado_rejeita_lance():
    log = make_log(1)
    log.new_entry(1, "CLOSE", {"auction_id": "a1"}, 50, 3)
    log.set_commit(log.last_seq)
    st = log.state
    assert st.auctions["a1"]["status"] == "closed"
    assert st.auctions["a1"]["winner"]["id"] == "b0"
    assert st.validate_bid({"auction_id": "a1", "bidder": "x", "amount": 1000}) == "leilão encerrado"


def test_empate_de_valor_vence_menor_timestamp_de_lamport():
    a = {"bids": [
        {"id": "x", "amount": 150, "lamport": 20, "node_id": 1},
        {"id": "y", "amount": 150, "lamport": 12, "node_id": 3},
        {"id": "z", "amount": 150, "lamport": 12, "node_id": 2},
    ]}
    assert AuctionState.highest_bid(a)["id"] == "z"  # (12, nó 2) < (12, nó 3) < (20, nó 1)


def test_mesmo_log_mesmo_estado_em_qualquer_no():
    """Determinismo: dois nós que aplicam o mesmo log chegam ao mesmo estado."""
    leader = make_log(5)
    follower = ReplicatedLog()
    follower.install(leader.entries, leader.commit_seq)
    assert follower.committed_hash() == leader.committed_hash()
    assert follower.state.list_summaries()[0]["highest_bid"] == leader.state.list_summaries()[0]["highest_bid"]


def test_vencedor_nao_depende_da_ordem_de_chegada():
    bids = [{"id": str(i), "amount": random.choice([100, 150, 150, 200, 200]),
             "lamport": random.randint(1, 50), "node_id": random.randint(1, 3)} for i in range(30)]
    winners = {AuctionState.highest_bid({"bids": random.sample(bids, len(bids))})["id"] for _ in range(20)}
    assert len(winners) == 1
