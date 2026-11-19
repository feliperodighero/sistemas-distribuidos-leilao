import asyncio

from node.mutex import MutexCoordinator


def test_exclusao_mutua_e_ordem_por_lamport():
    """Pedidos concorrentes entram um de cada vez, na ordem (L, nó) — não na ordem de chegada."""
    async def run():
        mutex = MutexCoordinator(holdback=0.05)
        inside, order = 0, []

        async def bid(key, delay):
            nonlocal inside
            await asyncio.sleep(delay)  # chegada em ordem diferente da ordem de Lamport
            async with mutex.hold("item", key, {"who": key}):
                inside += 1
                assert inside == 1, "dois lances na seção crítica ao mesmo tempo!"
                order.append(key)
                await asyncio.sleep(0.02)
                inside -= 1

        await asyncio.gather(bid((30, 1), 0.00), bid((12, 3), 0.01), bid((12, 2), 0.02))
        return order

    assert asyncio.run(run()) == [(12, 2), (12, 3), (30, 1)]


def test_itens_diferentes_nao_se_bloqueiam():
    async def run():
        mutex = MutexCoordinator(holdback=0.01)
        both_inside = asyncio.Event()
        count = 0

        async def hold(item):
            nonlocal count
            async with mutex.hold(item, (1, 1), {}):
                count += 1
                if count == 2:
                    both_inside.set()
                await asyncio.wait_for(both_inside.wait(), 1)

        await asyncio.gather(hold("A"), hold("B"))
        return True

    assert asyncio.run(run())
