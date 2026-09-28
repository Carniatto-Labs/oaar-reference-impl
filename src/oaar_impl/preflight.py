import os
import sys
import time
import urllib.request

import psycopg

POSTGRES_DSN = os.environ.get(
    "OAAR_POSTGRES_DSN", "postgresql://oaar:oaar@postgres:5432/oaar"
)

HTTP_CHECKS = [
    ("otel-collector", "http://otel-collector:13133/"),
    ("tempo", "http://tempo:3200/ready"),
    ("prometheus", "http://prometheus:9090/-/ready"),
    ("loki", "http://loki:3100/ready"),
    ("grafana", "http://grafana:3000/api/health"),
    ("mcp-server", "http://mcp-server:8081/health"),
]

MAX_ROUNDS = 15
ROUND_SLEEP_SECONDS = 2.0


def check_http(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status == 200
    except Exception:
        return False


def check_postgres() -> bool:
    try:
        with psycopg.connect(POSTGRES_DSN, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone() == (1,)
    except Exception:
        return False


def build_checks():
    checks = [("postgres", check_postgres)]
    checks += [(name, (lambda url: (lambda: check_http(url)))(url)) for name, url in HTTP_CHECKS]
    return checks


def main() -> int:
    pending = build_checks()
    results = {}
    for _ in range(MAX_ROUNDS):
        remaining = []
        for name, fn in pending:
            ok = fn()
            results[name] = ok
            if not ok:
                remaining.append((name, fn))
        pending = remaining
        if not pending:
            break
        time.sleep(ROUND_SLEEP_SECONDS)

    width = max(len(name) for name in results)
    failed = 0
    for name, ok in results.items():
        status = "PASS" if ok else "FAIL"
        if not ok:
            failed += 1
        print(f"{name.ljust(width)}  {status}")
    total = len(results)
    print(f"\n{total - failed}/{total} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
