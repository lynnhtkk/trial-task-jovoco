# Trial Task (Walkthrough)

## Level 1: Semantic Model (Star Schema)

To provide a suitable analytical layer, the data is modeled using a Star Schema. This separates the measurable sales events from the descriptive context, optimizing the data for aggregation and reporting.

**Grain:** One row per individual item sold within an order.

### Fact Table

* **`fact_sales`**: `order_id`, `item_id`, `customer_id` (FK), `product_id` (FK), `store_id` (FK), `date`, `quantity`, `price`, `revenue`

### Dimension Tables

* **`dim_customers`**: `customer_id` (PK), `name`, `city`, `registration_date`, `type`
* **`dim_products`**: `product_id` (PK), `title`, `category`, `cost`
* **`dim_stores`**: `store_id` (PK), `title`, `city`, `region`
* **`dim_date`**: `date` (PK), `month`, `quarter`, `year`

**Modeling Notes on Data Quality:** The raw data contains inconsistencies (e.g., `orders` linking to customers via a reversed name string rather than an ID, and inconsistent date formats). The ETL pipeline will standardize these formats and perform the necessary lookups during the Silver transformation phase to ensure the Gold layer uses clean, integer-based foreign keys.

---

## Level 2 & 4: ETL Pipeline & Automation

The ETL process is fully automated using a single Python script (`etl_pipeline.py`) that orchestrates a local **DuckDB** database. The pipeline follows the Medallion Architecture (Bronze, Silver, Gold) and is designed to be fully **idempotent**, meaning it can be run multiple times safely without duplicating data.

---

## Level 3: Analytical Queries

The analytical Gold layer is queried using modular SQL scripts to provide high-value business insights. These queries demonstrate advanced SQL techniques and a deep understanding of the star-schema relationships.

* **Modular Architecture:** Each business question is answered by a dedicated script in the `sql/` directory, promoting maintainability and reusability.
* **Automated Execution:** The `run_analytics.py` script serves as an orchestrator, executing the queries against the `pipeline.db` and outputting results in a clean, tabular format.
* **Analytical Highlights:**
    * **Top-5 Products by Region:** Utilizes `DENSE_RANK()` window functions to rank performance within geographic partitions.
    * **Customer Segmentation:** Implements `NTILE(3)` to bucket customers into High, Mid, and Low-Value segments based on rolling 12-month revenue.
    * **Market Basket Analysis:** Employs self-joins on `fact_sales` to identify the top 10 most frequent product pairings in a single transaction.
    * **Data Integrity:** All analytical scripts connect to DuckDB in **Read-Only** mode, ensuring that reporting processes never inadvertently modify the Gold layer.

---

## Bonus: Sales Development Dashboard

To visualize the pipeline's output, a dynamic dashboard was developed using **Plotly**. This serves as the final proof of the data’s quality and utility.

* **KPIs Tracked:** Monthly revenue trends vs. order volume, revenue by product category, and quarterly regional performance.
* **Operational Insight:** Includes a breakdown of order statuses to monitor fulfillment health.
* **Execution:** Run `python dashboard.py` to launch the interactive visualization in your web browser.
