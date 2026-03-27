from __future__ import annotations

import argparse
import sqlite3
import csv
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="audit.db")
    ap.add_argument("--out", default="phase1_md_quality.csv")
    ap.add_argument("--day", default=None, help="UTC day YYYY-MM-DD; default=today")
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        raise SystemExit(f"DB not found: {db}")

    conn = sqlite3.connect(str(db))
    cur = conn.cursor()

    if args.day is None:
        cur.execute("SELECT strftime('%Y-%m-%d','now')")
        day = cur.fetchone()[0]
    else:
        day = args.day

    q = (
        "SELECT day, symbol, buckets, duplicates, "
        "CASE WHEN (buckets+1) > 0 THEN (1.0*duplicates)/(buckets+1) ELSE 0 END AS dup_per_bucket, "
        "max_gap_sec, last_end_time, updated_ts "
        "FROM md_quality WHERE day=? ORDER BY dup_per_bucket DESC, max_gap_sec DESC"
    )
    rows = cur.execute(q, (day,)).fetchall()
    headers = [d[0] for d in cur.description]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows -> {args.out}")


if __name__ == "__main__":
    main()
