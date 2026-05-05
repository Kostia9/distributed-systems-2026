import argparse
import asyncio
from time import perf_counter

import httpx


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Perf test client for facade-service")
    p.add_argument("--base-url", default="http://localhost:8000", help="Facade URL")
    p.add_argument("--scenario", choices=["1", "2"], default="1")
    p.add_argument("--clients", type=int, default=10)
    p.add_argument("--n", type=int, default=10_000, help="requests per client")
    p.add_argument(
        "--settle-timeout",
        type=float,
        default=120.0,
        help="seconds to wait for balances to converge",
    )
    p.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="seconds between balance polls",
    )
    return p.parse_args()


async def worker(
    client: httpx.AsyncClient, base_url: str, user_id: str, n: int, amount: int
) -> None:
    for _ in range(n):
        r = await client.post(
            f"{base_url}/transaction", json={"user_id": user_id, "amount": amount}
        )
        r.raise_for_status()


def build_expected_balances(
    scenario: str,
    clients: int,
    n: int,
    amount: int,
    balances_before: dict[str, int],
) -> dict[str, int]:
    expected_delta = n * amount
    if scenario == "1":
        return {
            f"user{i}": balances_before.get(f"user{i}", 0) + expected_delta
            for i in range(clients)
        }
    return {"shared_user": balances_before.get("shared_user", 0) + clients * n * amount}


async def wait_for_expected_balances(
    client: httpx.AsyncClient,
    base_url: str,
    expected_balances: dict[str, int],
    timeout_s: float,
    poll_interval_s: float,
) -> tuple[dict, bool, float]:
    deadline = perf_counter() + timeout_s
    last_accounts: dict = {"balances": None}
    settle_started = perf_counter()

    while True:
        resp = await client.get(f"{base_url}/accounts")
        resp.raise_for_status()
        last_accounts = resp.json()
        balances = last_accounts.get("balances") or {}

        if all(
            balances.get(user_id, 0) == expected
            for user_id, expected in expected_balances.items()
        ):
            return last_accounts, True, perf_counter() - settle_started

        if perf_counter() >= deadline:
            return last_accounts, False, perf_counter() - settle_started

        await asyncio.sleep(poll_interval_s)


async def main() -> None:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    amount = 1  # money per transaction

    users: list[str]
    if args.scenario == "1":
        users = [f"user{i}" for i in range(args.clients)]
    elif args.scenario == "2":
        users = ["shared_user"] * args.clients
    else:
        raise ValueError(f"Unknown scenario: {args.scenario}")

    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    timeout = httpx.Timeout(30.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        before = (await client.get(f"{base_url}/accounts")).json()
        balances_before = before.get("balances") or {}
        expected_balances = build_expected_balances(
            args.scenario,
            args.clients,
            args.n,
            amount,
            balances_before,
        )

        await client.post(f"{base_url}/metrics/reset/all")

        t0 = perf_counter()
        await asyncio.gather(
            *[
                worker(client, base_url, users[i], args.n, amount)
                for i in range(args.clients)
            ]
        )
        dt = perf_counter() - t0
        post_metrics = (await client.get(f"{base_url}/metrics/all")).json()

        total = args.clients * args.n
        rps = total / dt if dt > 0 else float("inf")

        print(
            f"scenario={args.scenario} clients={args.clients} "
            f"n={args.n} total_requests={total}"
        )
        print(f"post_time_s={dt:.3f}  post_rps={rps:.1f}")

        accounts, ok, settle_time = await wait_for_expected_balances(
            client,
            base_url,
            expected_balances,
            args.settle_timeout,
            args.poll_interval,
        )
        balances_after = accounts.get("balances") or {}
        total_time = dt + settle_time
        print(f"settle_time_s={settle_time:.3f}")
        print(f"total_time_s={total_time:.3f}")
        print("accounts:", accounts)

        metrics = (await client.get(f"{base_url}/metrics/all")).json()
        facade_metrics = post_metrics.get("facade") or {}
        counter_metrics = metrics.get("counter") or {}
        logging_contribution = (
            facade_metrics.get("logging", {}).get("total_s", 0.0)
        )
        counter_contribution = (
            facade_metrics.get("counter", {}).get("total_s", 0.0)
            + counter_metrics.get("db_apply", {}).get("total_s", 0.0)
        )

        print(f"logging_service_contribution_s={logging_contribution:.3f}")
        print(f"counter_service_contribution_s={counter_contribution:.3f}")
        print("facade_metrics (logging-http, counter-queue.put):", facade_metrics)
        print("counter_metrics (db-apply):", counter_metrics)

        print(
            f"expected_balances={expected_balances} | "
            f"actual_balances={balances_after} |",
            "OK" if ok else "FAIL",
        )


if __name__ == "__main__":
    asyncio.run(main())
