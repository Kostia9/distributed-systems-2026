import argparse
import asyncio
from time import perf_counter
from typing import List

import httpx


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Perf test client for facade-service")
    p.add_argument("--base-url", default="http://localhost:8000", help="Facade URL")
    p.add_argument("--scenario", choices=["1", "2"], default="1")
    p.add_argument("--clients", type=int, default=10)
    p.add_argument("--n", type=int, default=10_000, help="requests per client")
    p.add_argument("--amount", type=int, default=1)
    return p.parse_args()


async def worker(
    client: httpx.AsyncClient, base_url: str, user_id: str, n: int, amount: int
) -> None:
    for _ in range(n):
        r = await client.post(
            f"{base_url}/transaction", json={"user_id": user_id, "amount": amount}
        )
        r.raise_for_status()


async def main() -> None:
    args = parse_args()
    base_url = args.base_url.rstrip("/")

    users: List[str]
    if args.scenario == "1":
        users = [f"user{i}" for i in range(args.clients)]
    else:
        users = ["shared_user"] * args.clients

    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    timeout = httpx.Timeout(30.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        # optional: reset timings
        try:
            await client.post(f"{base_url}/metrics/reset")
        except Exception:
            pass

        t0 = perf_counter()
        await asyncio.gather(
            *[
                worker(client, base_url, users[i], args.n, args.amount)
                for i in range(args.clients)
            ]
        )
        dt = perf_counter() - t0

        total = args.clients * args.n
        rps = total / dt if dt > 0 else float("inf")

        print(
            f"scenario={args.scenario} clients={args.clients} n={args.n} total_requests={total}"
        )
        print(f"total_time_s={dt:.3f}  rps={rps:.1f}")

        metrics = (await client.get(f"{base_url}/metrics")).json()
        print("metrics:", metrics)

        accounts = (await client.get(f"{base_url}/accounts")).json()
        print("accounts:", accounts)

        # sanity check expected balances for the assignment's default parameters
        if args.amount == 1 and args.clients == 10 and args.n == 10_000:
            if args.scenario == "1":
                ok = all(
                    accounts["balances"].get(f"user{i}", 0) == 10_000 for i in range(10)
                )
                print("expected: each user balance == 10000 |", "OK" if ok else "FAIL")
            else:
                ok = accounts["balances"].get("shared_user", 0) == 100_000
                print(
                    "expected: shared_user balance == 100000 |", "OK" if ok else "FAIL"
                )


if __name__ == "__main__":
    asyncio.run(main())
