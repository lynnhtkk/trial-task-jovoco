import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent / "pipeline.db"
SQL_DIR = Path(__file__).parent / "sql"

def run_sql_file(file_name: str):
    """Reads a SQL file and executes it against the Gold layer."""
    query_path = SQL_DIR / file_name
    
    if not query_path.exists():
        print(f"Error: Could not find {query_path}")
        return

    with open(query_path, 'r') as file:
        query = file.read()

    print("")
    print(f"Executing: {file_name}\n")
    
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        result_df = con.sql(query).df() # returns pandas DataFrame
        print(result_df.to_string(index=False))
        print("\n")

if __name__ == "__main__":
    queries_to_run = [
        "1_top_products.sql",
        "2_customer_segmentation.sql",
        "3_product_pairs.sql",
        "4_multi_quarter_customers.sql"
    ]
    
    for query_file in queries_to_run:
        run_sql_file(query_file)