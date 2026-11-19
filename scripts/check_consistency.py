#!/usr/bin/env python3
"""Compara o estado de todos os nós: papel, termo, log confirmado e hash.

Uso: python scripts/check_consistency.py [--nos http://localhost:8001,http://localhost:8002,...]
Saída 0 se todos os nós no ar têm o mesmo hash; 1 caso contrário.
"""
import argparse
import json
import sys
import urllib.request

DEFAULT = "http://localhost:8001,http://localhost:8002,http://localhost:8003,http://localhost:8004"


def get(url):
    try:
        with urllib.request.urlopen(url + "/api/node", timeout=2) as r:
            return json.loads(r.read())
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nos", default=DEFAULT)
    urls = [u.strip() for u in p.parse_args().nos.split(",") if u.strip()]
    print(f"{'nó':<4}{'papel':<12}{'líder':<7}{'termo':<7}{'commit':<8}{'Lamport':<9}hash")
    hashes = set()
    for url in urls:
        s = get(url)
        if s is None:
            print(f"{'-':<4}{'fora do ar':<12}{'':<7}{'':<7}{'':<8}{'':<9}({url})")
            continue
        hashes.add(s["log_hash"])
        print(f"{s['node_id']:<4}{s['role']:<12}{str(s['leader_id']):<7}{s['term']:<7}"
              f"{s['commit_seq']:<8}{s['lamport']:<9}{s['log_hash']}")
    if len(hashes) == 1:
        print("\nCONSISTENTE: todos os nós no ar têm o mesmo estado confirmado.")
        return 0
    print("\nATENÇÃO: estados diferentes (pode ser sincronização em andamento; rode de novo).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
