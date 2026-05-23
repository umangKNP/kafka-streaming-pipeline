"""
dashboard.py
Live CLI dashboard showing current VWAP per symbol.
Refreshes every 5 seconds by querying Postgres.
Run: python dashboard.py
"""

import time
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.text import Text

PG_DSN = "host=localhost port=5432 dbname=stockdb user=pipeline password=pipeline"
REFRESH_INTERVAL = 5


def fetch_latest(conn) -> list[dict]:
    sql = """
        SELECT DISTINCT ON (symbol)
            symbol,
            window_start,
            vwap,
            total_volume,
            trade_count,
            updated_at
        FROM price_aggregates
        ORDER BY symbol, window_start DESC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


def fetch_row_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM price_aggregates")
        return cur.fetchone()[0]


def build_table(rows: list[dict]) -> Table:
    table = Table(
        title="",
        show_header=True,
        header_style="bold green",
        border_style="dim",
        expand=True,
    )
    table.add_column("Symbol",       style="bold cyan",  width=10)
    table.add_column("VWAP ($)",     justify="right",    width=14)
    table.add_column("Volume",       justify="right",    width=12)
    table.add_column("Trades",       justify="right",    width=10)
    table.add_column("Window Start", width=22)
    table.add_column("Updated",      width=22)

    for row in sorted(rows, key=lambda r: r["symbol"]):
        table.add_row(
            row["symbol"],
            f"{float(row['vwap']):,.4f}",
            f"{row['total_volume']:,}",
            str(row["trade_count"]),
            row["window_start"].strftime("%Y-%m-%d %H:%M:%S"),
            row["updated_at"].strftime("%H:%M:%S"),
        )

    return table


def run():
    conn = psycopg2.connect(PG_DSN)
    console = Console()

    with Live(console=console, refresh_per_second=1) as live:
        while True:
            rows      = fetch_latest(conn)
            row_count = fetch_row_count(conn)
            now       = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            if rows:
                table = build_table(rows)
                panel = Panel(
                    table,
                    title=f"[bold green]📈 Stock VWAP Dashboard[/bold green]  |  {now}",
                    subtitle=f"[dim]{row_count} aggregate rows stored | refreshes every {REFRESH_INTERVAL}s[/dim]",
                    border_style="green",
                )
            else:
                panel = Panel(
                    Text("Waiting for data... make sure producer and consumer are running.", justify="center"),
                    title=f"[bold yellow]📈 Stock VWAP Dashboard[/bold yellow]  |  {now}",
                    border_style="yellow",
                )

            live.update(panel)
            time.sleep(REFRESH_INTERVAL)


if __name__ == "__main__":
    run()
