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

    log.info("BRONZE complete\n")


# SILVER – Cleaning & Transformation (slv schema)

def transform_silver(con: duckdb.DuckDBPyConnection) -> None:
    """Clean and normalise Bronze data into the slv schema with full idempotency."""
    log.info("SILVER: cleaning & transforming Bronze data")
    con.execute("CREATE SCHEMA IF NOT EXISTS slv")

    # slv.customers
    log.info("  Transforming slv.customers …")
    con.execute("DROP TABLE IF EXISTS slv.customers")
    con.execute(
        """
        CREATE TABLE slv.customers AS
        SELECT
            CAST("CustomerID" AS INTEGER) AS customer_id,
            TRIM("Name") AS name,
            COALESCE(NULLIF(TRIM("City"), ''), 'Unknown') AS city,
            TRY_CAST(
                CASE
                    WHEN "Registration Date" LIKE '____-__-__'
                        THEN "Registration Date"
                    WHEN "Registration Date" LIKE '__.__.____ '
                      OR "Registration Date" LIKE '__.__.____'
                        THEN STRFTIME(
                                STRPTIME("Registration Date", '%d.%m.%Y'),
                                '%Y-%m-%d'
                             )
                    ELSE NULL
                END
            AS DATE) AS registration_date,
            COALESCE(NULLIF(TRIM("Type"), ''), 'Unknown') AS type
        FROM stg.customers
        WHERE "CustomerID" IS NOT NULL
          AND TRIM("CustomerID") <> ''
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM slv.customers").fetchone()[0])

    # slv.stores
    log.info("  Transforming slv.stores …")
    con.execute("DROP TABLE IF EXISTS slv.stores")
    con.execute(
        """
        CREATE TABLE slv.stores AS
        SELECT DISTINCT
            CAST("Store" AS INTEGER) AS store_id,
            LOWER(TRIM("Title")) AS title,
            TRIM("City") AS city,
            COALESCE(NULLIF(TRIM("Region"), ''), 'Unknown') AS region
        FROM stg.stores
        WHERE "Store" IS NOT NULL
          AND TRIM("Store") <> ''
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM slv.stores").fetchone()[0])

    # slv.products
    log.info("  Transforming slv.products …")
    con.execute("DROP TABLE IF EXISTS slv.products")
    con.execute(
        """
        CREATE TABLE slv.products AS
        SELECT
            CAST("Product" AS INTEGER)            AS product_id,
            TRIM("Title")                         AS title,
            LOWER(TRIM("Category")) AS category,
            TRY_CAST("Cost" AS DOUBLE)            AS cost
        FROM stg.products
        WHERE "Product" IS NOT NULL
          AND TRIM("Product") <> ''
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM slv.products").fetchone()[0])

    # slv.orders
    log.info("  Transforming slv.orders …")
    con.execute("DROP TABLE IF EXISTS slv.orders")
    con.execute(
        """
        CREATE TABLE slv.orders AS
        WITH deduped AS (
            -- Keep only the first occurrence of each Order ID
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY "Order" ORDER BY "Order") AS rn
            FROM stg.orders
            WHERE "Order" IS NOT NULL
              AND TRIM("Order") <> ''
        ),
        raw AS (
            SELECT
                CAST("Order" AS INTEGER)  AS order_id,
                TRIM("Customer Name") AS raw_customer_name,
                -- Reverse "Lastname Firstname" → "Firstname Lastname"
                TRIM(
                    SPLIT_PART("Customer Name", ' ', 2)
                    || ' ' ||
                    SPLIT_PART("Customer Name", ' ', 1)
                ) AS reversed_name,
                TRY_CAST(
                    CASE
                        WHEN "Date" LIKE '__/__/____'
                            THEN STRFTIME(STRPTIME("Date", '%d/%m/%Y'), '%Y-%m-%d')
                        WHEN "Date" LIKE '____-__-__'
                            THEN "Date"
                        ELSE NULL
                    END
                AS DATE) AS order_date,
                LOWER(NULLIF(TRIM("Status"), '')) AS status,
                TRY_CAST(
                    NULLIF(TRIM(SPLIT_PART("Store", '.', 1)), '')
                AS INTEGER) AS store_id
            FROM deduped
            WHERE rn = 1
        )
        SELECT
            r.order_id,
            -- Prefer reversed lookup; fall back to as-is lookup
            COALESCE(c_rev.customer_id, c_asis.customer_id) AS customer_id,
            r.store_id,
            r.order_date,
            r.status
        FROM raw r
        LEFT JOIN slv.customers c_rev
               ON c_rev.name = r.reversed_name
        LEFT JOIN slv.customers c_asis
               ON c_asis.name = r.raw_customer_name
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM slv.orders").fetchone()[0])

    # Warn about any orders whose customer could not be resolved
    unresolved = con.execute(
        "SELECT COUNT(*) FROM slv.orders WHERE customer_id IS NULL"
    ).fetchone()[0]
    if unresolved:
        log.warning("    ⚠ %d order(s) could not be matched to a customer", unresolved)

    # slv.order_items
    log.info("  Transforming slv.order_items …")
    con.execute("DROP TABLE IF EXISTS slv.order_items")
    con.execute(
        """
        CREATE TABLE slv.order_items AS
        WITH deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY "Item" ORDER BY "Item") AS rn
            FROM stg.order_items
            WHERE "Item" IS NOT NULL
              AND TRIM("Item") <> ''
        )
        SELECT
            CAST(d."Item"  AS INTEGER) AS item_id,
            CAST(d."Order" AS INTEGER) AS order_id,
            p.product_id,
            COALESCE(TRY_CAST(d."Qty" AS INTEGER), 1) AS quantity,
            TRY_CAST(d."Price" AS DOUBLE) AS price
        FROM deduped d
        LEFT JOIN slv.products p
               ON p.title = TRIM(d."Product")
        WHERE d.rn = 1
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM slv.order_items").fetchone()[0])

    log.info("SILVER complete\n")


# GOLD – Star Schema (gold schema)

def build_gold(con: duckdb.DuckDBPyConnection) -> None:
    """Build the star-schema analytical layer in the gold schema from Silver data."""
    log.info("GOLD: building Star Schema")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # dim_customers - direct projection
    log.info("  Building gold.dim_customers …")
    con.execute("DROP TABLE IF EXISTS gold.dim_customers")
    con.execute(
        """
        CREATE TABLE gold.dim_customers AS
        SELECT customer_id, name, city, registration_date, type
        FROM slv.customers
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM gold.dim_customers").fetchone()[0])

    # dim_products - direct projection
    log.info("  Building gold.dim_products …")
    con.execute("DROP TABLE IF EXISTS gold.dim_products")
    con.execute(
        """
        CREATE TABLE gold.dim_products AS
        SELECT product_id, title, category, cost
        FROM slv.products
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM gold.dim_products").fetchone()[0])

    # dim_stores - direct projection
    log.info("  Building gold.dim_stores …")
    con.execute("DROP TABLE IF EXISTS gold.dim_stores")
    con.execute(
        """
        CREATE TABLE gold.dim_stores AS
        SELECT store_id, title, city, region
        FROM slv.stores
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM gold.dim_stores").fetchone()[0])

    # fact_sales 
    log.info("  Building gold.fact_sales …")
    con.execute("DROP TABLE IF EXISTS gold.fact_sales")
    con.execute(
        """
        CREATE TABLE gold.fact_sales AS
        SELECT
            oi.item_id,
            o.order_id,
            o.customer_id,
            oi.product_id,
            o.store_id,
            o.order_date AS date,
            oi.quantity,
            oi.price,
            ROUND(oi.quantity * oi.price, 2) AS revenue
        FROM slv.order_items oi
        JOIN slv.orders o ON o.order_id = oi.order_id
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM gold.fact_sales").fetchone()[0])

    # dim_date – derived inline from the distinct dates in fact_sales
    log.info("  Building gold.dim_date …")
    con.execute("DROP TABLE IF EXISTS gold.dim_date")
    con.execute(
        """
        CREATE TABLE gold.dim_date AS
        SELECT DISTINCT
            date,
            MONTH(date)    AS month,
            QUARTER(date)  AS quarter,
            YEAR(date)     AS year
        FROM gold.fact_sales
        WHERE date IS NOT NULL
        ORDER BY date
        """
    )
    log.info("    ✓ %d rows", con.execute("SELECT COUNT(*) FROM gold.dim_date").fetchone()[0])

    log.info("GOLD complete\n")


# Pipeline orchestration

def run_pipeline() -> None:
    """Execute the full Bronze → Silver → Gold pipeline in a single connection."""
    log.info("Starting ETL pipeline  |  DB: %s", DB_PATH)
    log.info("-" * 60)

    try:
        con = duckdb.connect(str(DB_PATH))

        with con:
            # BRONZE
            load_bronze(con)

            # SILVER
            transform_silver(con)

            # GOLD
            build_gold(con)

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
