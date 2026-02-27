from __future__ import annotations

import argparse
import sqlite3
import csv
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="audit.db")
    ap.add_argument("--out", default="phase1_signals_audit.csv")
    ap.add_argument("--day", default=None, help="UTC day YYYY-MM-DD; default=all")
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        raise SystemExit(f"DB not found: {db}")

    conn = sqlite3.connect(str(db))
    cur = conn.cursor()

    q = "SELECT ts, symbol, bar_end_time, o,h,l,c,v,last3_close,range20,mr_sig,bo_sig,short_sig,mid_scale,total_sig,threshold,should_trade,action,reason FROM signals_audit"
    params = ()
    if args.day:
        q += " WHERE substr(bar_end_time,1,10)=?"
        params = (args.day,)
    q += " ORDER BY bar_end_time ASC"

    rows = cur.execute(q, params).fetchall()
    headers = [d[0] for d in cur.description]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows -> {args.out}")


if __name__ == "__main__":
    main()
