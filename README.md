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
