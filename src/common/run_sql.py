"""
Run the analytics SQL against the Gold Parquet layer using DuckDB.

Why this exists: SQL in a repo that has never been executed is a liability.
This registers every Gold table as a DuckDB view over its Parquet files and
runs each statement in `sql/analytics/`, so a broken query fails CI instead
of failing in front of a stakeholder.

The same statements run unmodified against Redshift; only the table
resolution differs (schema-qualified there, views here).

Usage:
    python -m src.common.run_sql                        # run every file
    python -m src.common.run_sql --file rcm_kpis.sql    # one file
    python -m src.common.run_sql --check                # exit non-zero on error
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import duckdb

from src.common.utils import PROJECT_ROOT, get_logger, layer_path, load_config

LOG = get_logger("sql.runner")
ANALYTICS_DIR = PROJECT_ROOT / "sql" / "analytics"


def register_gold(con: duckdb.DuckDBPyConnection, cfg: dict) -> list[str]:
    """Expose every Gold Parquet dataset as a DuckDB view of the same name."""
    gold_root = Path(layer_path("gold", "", cfg))
    registered = []
    for path in sorted(gold_root.iterdir()):
        if not path.is_dir():
            continue
        table = path.name
        # union_by_name copes with partitioned directories whose partition
        # column only exists in the path, not the file.
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM read_parquet('{path}/**/*.parquet', "
            f"hive_partitioning = true, union_by_name = true)"
        )
        registered.append(table)
    LOG.info("registered %d gold tables", len(registered))
    return registered


def split_statements(sql_text: str) -> list[tuple[str, str]]:
    """
    Split a file into (label, statement) pairs.

    The label is the `-- Qn. ...` heading above the statement, which makes
    the output readable instead of a wall of anonymous result sets.
    """
    # strip block headers made of dashes, keep the Qn titles
    statements = []
    current_label = "statement"
    buffer: list[str] = []

    for line in sql_text.splitlines():
        m = re.match(r"^--\s*(Q\d+\..*)$", line.strip())
        if m:
            current_label = m.group(1).strip()
        buffer.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(buffer).strip()
            # ignore chunks that are only comments
            body = re.sub(r"--.*$", "", stmt, flags=re.MULTILINE).strip()
            if body:
                statements.append((current_label, stmt))
            buffer = []
    return statements


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run analytics SQL against Gold Parquet")
    ap.add_argument("--file", default=None, help="single file in sql/analytics/")
    ap.add_argument("--check", action="store_true",
                    help="exit non-zero if any statement fails")
    ap.add_argument("--rows", type=int, default=10, help="rows to display")
    args = ap.parse_args(argv)

    cfg = load_config()
    con = duckdb.connect()
    register_gold(con, cfg)

    files = ([ANALYTICS_DIR / args.file] if args.file
             else sorted(ANALYTICS_DIR.glob("*.sql")))

    failures = 0
    for sql_file in files:
        print(f"\n{'=' * 78}\n{sql_file.name}\n{'=' * 78}")
        for label, stmt in split_statements(sql_file.read_text(encoding="utf-8")):
            try:
                result = con.execute(stmt).df()
                print(f"\n--- {label}  ({len(result)} rows) ---")
                with_pd_opts = result.head(args.rows).to_string(index=False)
                print(with_pd_opts)
            except Exception as exc:                        # noqa: BLE001
                failures += 1
                print(f"\n--- {label} :: FAILED ---\n{exc}")

    print(f"\n{'=' * 78}")
    if failures:
        print(f"{failures} statement(s) failed")
        return 1 if args.check else 0
    print("all statements executed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
