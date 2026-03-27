from __future__ import annotations

import argparse
import sqlite3
import csv
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="audit.db")
    ap.add_argument("--out", default="phase1_top_signals.csv")
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        raise SystemExit(f"DB not found: {db}")

    conn = sqlite3.connect(str(db))
    cur = conn.cursor()

    q = f"""
    SELECT a.symbol,a.bar_end_time,a.total_sig,a.short_sig,a.mid_scale,a.threshold,a.should_trade,a.action,a.reason,a.channel,a.can_trade_short,a.risk_gate
    FROM signals_audit a
    JOIN (SELECT symbol, MAX(id) AS mid FROM signals_audit GROUP BY symbol) b
      ON a.symbol=b.symbol AND a.id=b.mid
    ORDER BY ABS(a.total_sig) DESC
    LIMIT {int(args.limit)};
    """
    rows = cur.execute(q).fetchall()
    headers = [d[0] for d in cur.description]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows -> {args.out}")


if __name__ == "__main__":
    main()
