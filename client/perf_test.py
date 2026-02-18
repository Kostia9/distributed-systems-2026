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
    amount = 1  # money per transaction

    users: List[str]
    if args.scenario == "1":
        users = [f"user{i}" for i in range(args.clients)]
    elif args.scenario == "2":
        users = ["shared_user"] * args.clients
    else:
        raise ValueError(f"Unknown scenario: {args.scenario}")

    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    timeout = httpx.Timeout(30.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        # reset timings
        await client.post(f"{base_url}/metrics/reset")

        # snapshot balances before the test
        before = (await client.get(f"{base_url}/accounts")).json()
        balances_before = before.get("balances", {})

        t0 = perf_counter()
        await asyncio.gather(
            *[
                worker(client, base_url, users[i], args.n, amount)
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
        balances_after = accounts.get("balances", {})
        print("accounts:", accounts)

        expected_delta = args.n * amount
        if args.scenario == "1":
            ok = all(
                balances_after.get(f"user{i}", 0) - balances_before.get(f"user{i}", 0)
                == expected_delta
                for i in range(args.clients)
            )
            print(
                f"expected: each user delta == +{expected_delta} |",
                "OK" if ok else "FAIL",
            )
        else:
            expected_total_delta = args.clients * args.n * amount
            actual_delta = balances_after.get("shared_user", 0) - balances_before.get(
                "shared_user", 0
            )
            ok = actual_delta == expected_total_delta
            print(
                f"expected: shared_user delta == +{expected_total_delta} |",
                f"actual delta={actual_delta} |",
                "OK" if ok else "FAIL",
            )


if __name__ == "__main__":
    asyncio.run(main())
