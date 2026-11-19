"""Teste de integração com o cluster real em Docker.

Pré-requisito: `docker compose up -d --build` já executado (nós 1, 2 e 3 no ar).
Rodar com:  LEILAO_INTEGRACAO=1 pytest tests/test_integracao.py -v

Cenário: cria leilão, dá lances por nós diferentes, derruba o líder com `docker compose
stop`, confirma que outro nó assume, que nenhum lance confirmado se perdeu, que um
novo lance funciona, e que o líder antigo volta e sincroniza.
"""
import json
import os
import subprocess
import time
import urllib.error
import urllib.request

import pytest

pytestmark = pytest.mark.skipif(os.getenv("LEILAO_INTEGRACAO") != "1",
                                reason="defina LEILAO_INTEGRACAO=1 com o cluster no ar")

NODES = {1: "http://localhost:8001", 2: "http://localhost:8002", 3: "http://localhost:8003"}


def call(method, url, body=None, timeout=12):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def status(node_id):
    try:
        return call("GET", NODES[node_id] + "/api/node", timeout=2)[1]
    except Exception:
        return None


def wait_for(cond, timeout=20, step=0.5):
    end = time.time() + timeout
    while time.time() < end:
        result = cond()
        if result:
            return result
        time.sleep(step)
    raise AssertionError("condição não atingida a tempo")


def leader_of(alive):
    def check():
        views = [status(n) for n in alive]
        if all(views) and len({v["leader_id"] for v in views}) == 1:
            leader = views[0]["leader_id"]
            if any(v["node_id"] == leader and v["role"] == "leader" for v in views):
                return leader
        return None
    return wait_for(check)


def same_hash(alive):
    def check():
        views = [status(n) for n in alive]
        return all(views) and len({v["log_hash"] for v in views}) == 1
    return wait_for(check)


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def test_queda_do_lider_sem_perder_lances():
    leader = leader_of([1, 2, 3])
    assert leader == 3, "Bully: o maior ID vivo deve ser o leiloeiro"

    _, r = call("POST", NODES[1] + "/api/auctions",
                {"title": "Teste integração", "start_price": 100, "min_increment": 10, "duration_s": 300})
    auction = r["auction"]["id"]
    for i, (node, amount) in enumerate([(1, 100), (2, 110), (3, 120)]):
        code, r = call("POST", f"{NODES[node]}/api/auctions/{auction}/bids", {"bidder": f"p{i}", "amount": amount})
        assert code == 201, r
    same_hash([1, 2, 3])

    compose("stop", f"node{leader}")
    try:
        # lance enviado durante a eleição: o nó 1 segura e reenvia ao novo líder
        code, r = call("POST", f"{NODES[1]}/api/auctions/{auction}/bids", {"bidder": "durante-falha", "amount": 130})
        assert code == 201, r
        new_leader = leader_of([1, 2])
        assert new_leader == 2
        _, a = call("GET", f"{NODES[2]}/api/auctions/{auction}")
        assert [b["amount"] for b in a["bids"]] == [100, 110, 120, 130]
        same_hash([1, 2])
    finally:
        compose("start", f"node{leader}")

    assert leader_of([1, 2, 3]) == 3  # o nó 3 volta e retoma a liderança (Bully)
    same_hash([1, 2, 3])
    _, a = call("GET", f"{NODES[3]}/api/auctions/{auction}")
    assert len(a["bids"]) == 4


def test_lances_concorrentes_um_so_vencedor():
    leader_of([1, 2, 3])
    _, r = call("POST", NODES[2] + "/api/auctions",
                {"title": "Concorrência", "start_price": 100, "min_increment": 10, "duration_s": 300})
    auction = r["auction"]["id"]
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(3) as ex:
        results = list(ex.map(lambda n: call("POST", f"{NODES[n]}/api/auctions/{auction}/bids",
                                             {"bidder": f"nó{n}", "amount": 150}), [1, 2, 3]))
    accepted = [r for code, r in results if r.get("accepted")]
    assert len(accepted) == 1
    winner_key = (accepted[0]["bid"]["lamport"], accepted[0]["bid"]["node_id"])
    keys = [(r["bid"]["lamport"], r["bid"]["node_id"]) for _, r in results]
    assert winner_key == min(keys), "o menor timestamp de Lamport deve vencer o empate"
    same_hash([1, 2, 3])


def test_sem_quorum_rejeita_escrita():
    leader_of([1, 2, 3])
    _, r = call("POST", NODES[1] + "/api/auctions",
                {"title": "Quórum", "start_price": 100, "min_increment": 10, "duration_s": 300})
    auction = r["auction"]["id"]
    compose("stop", "node2", "node3")
    try:
        wait_for(lambda: (s := status(1)) and s["role"] == "leader", timeout=20)
        code, r = call("POST", f"{NODES[1]}/api/auctions/{auction}/bids", {"bidder": "sozinho", "amount": 100})
        assert code == 409 and "quórum" in r["reason"]
    finally:
        compose("start", "node2", "node3")
    leader_of([1, 2, 3])
