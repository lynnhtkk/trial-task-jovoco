import logging
import sys
from pathlib import Path

import duckdb


DATA_DIR = Path(__file__).parent / "data"
DB_PATH = Path(__file__).parent / "pipeline.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# BRONZE – Staging (stg schema)

def load_bronze(con: duckdb.DuckDBPyConnection) -> None:
    """Load raw CSV files into the staging schema with full idempotency."""
    log.info("BRONZE: loading raw CSV files into stg schema")
    con.execute("CREATE SCHEMA IF NOT EXISTS stg")

    csv_tables = {
        "stg.customers":   DATA_DIR / "customers.csv",
        "stg.orders":      DATA_DIR / "orders.csv",
        "stg.order_items": DATA_DIR / "order_items.csv",
        "stg.products":    DATA_DIR / "products.csv",
        "stg.stores":      DATA_DIR / "stores.csv",
    }

    for table, csv_path in csv_tables.items():
        if not csv_path.exists():
            raise FileNotFoundError(f"Source file not found: {csv_path}")

        log.info("  Loading %-18s ← %s", table, csv_path.name)
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(
            f"""
            CREATE TABLE {table} AS
            SELECT * FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
            """
        )
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info("    ✓ %d rows staged", count)

    log.info("=== BRONZE complete ===\n")


# Pipeline orchestration

def run_pipeline() -> None:
    """Execute the Bronze ingestion pipeline in a single connection."""
    log.info("Starting ETL pipeline  |  DB: %s", DB_PATH)
    log.info("-" * 60)

    try:
        con = duckdb.connect(str(DB_PATH))

        # ---------- Bronze ----------
        with con:
            load_bronze(con)

        con.close()
        log.info("Pipeline finished successfully ✓")
        log.info("Database written to: %s", DB_PATH.resolve())

    except FileNotFoundError as exc:
        log.error("Source file missing: %s", exc)
        sys.exit(1)
    except duckdb.Error as exc:
        log.error("DuckDB error: %s", exc)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        log.error("Unexpected error: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
