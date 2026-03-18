

import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

DB_PATH = Path(__file__).parent / "pipeline.db"


def load_data(con: duckdb.DuckDBPyConnection) -> dict[str, pd.DataFrame]:

    # Monthly revenue trend
    monthly = con.execute("""
        SELECT
            DATE_TRUNC('month', date) AS month,
            SUM(revenue)              AS revenue,
            COUNT(DISTINCT order_id)  AS orders
        FROM gold.fact_sales
        WHERE date IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).df()

    # Revenue by product (all time)
    by_product = con.execute("""
        SELECT
            dp.title      AS product,
            SUM(fs.revenue) AS revenue
        FROM gold.fact_sales   fs
        JOIN gold.dim_products dp ON dp.product_id = fs.product_id
        GROUP BY dp.title
        ORDER BY revenue DESC
    """).df()

    # Revenue by region per quarter
    by_region = con.execute("""
        SELECT
            CAST(dd.year AS VARCHAR) || ' Q' || CAST(dd.quarter AS VARCHAR) AS period,
            ds.region,
            SUM(fs.revenue) AS revenue
        FROM gold.fact_sales  fs
        JOIN gold.dim_date    dd ON dd.date     = fs.date
        JOIN gold.dim_stores  ds ON ds.store_id  = fs.store_id
        WHERE fs.store_id IS NOT NULL
        GROUP BY period, ds.region
        ORDER BY MIN(dd.year), MIN(dd.quarter), ds.region
    """).df()

    # Order status breakdown
    by_status = con.execute("""
        SELECT
            o.status,
            COUNT(DISTINCT fs.order_id) AS orders
        FROM gold.fact_sales  fs
        JOIN slv.orders       o  ON o.order_id = fs.order_id
        WHERE o.status IS NOT NULL
        GROUP BY o.status
        ORDER BY orders DESC
    """).df()

    return {
        "monthly":    monthly,
        "by_product": by_product,
        "by_region":  by_region,
        "by_status":  by_status,
    }


def build_dashboard(data: dict[str, pd.DataFrame]) -> go.Figure:

    monthly    = data["monthly"]
    by_product = data["by_product"]
    by_region  = data["by_region"]
    by_status  = data["by_status"]

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Monthly Revenue & Order Volume",
            "Total Revenue by Product",
            "Quarterly Revenue by Region",
            "Order Status Breakdown",
        ),
        specs=[
            [{"secondary_y": True}, {"type": "bar"}],
            [{"type": "bar"},       {"type": "pie"}],
        ],
        vertical_spacing=0.18,
        horizontal_spacing=0.12,
    )

    # Chart 1: Monthly Revenue (Bars) + Order Count (Line)
    fig.add_trace(
        go.Bar(
            x=monthly["month"],
            y=monthly["revenue"].round(2),
            name="Revenue (€)",
            marker_color="#4f46e5",
            opacity=0.85,
        ),
        row=1, col=1, secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=monthly["month"],
            y=monthly["orders"],
            name="Orders",
            mode="lines+markers",
            line=dict(color="#f59e0b", width=2),
            marker=dict(size=5),
        ),
        row=1, col=1, secondary_y=True,
    )

    # Chart 2: Revenue by Product
    fig.add_trace(
        go.Bar(
            x=by_product["revenue"].round(2),
            y=by_product["product"],
            orientation="h",
            marker_color="#06b6d4",
            name="Product Revenue",
            showlegend=False,
        ),
        row=1, col=2,
    )
    # Chart 3: Quarterly Revenue by Region
    for region in by_region["region"].unique():
        subset = by_region[by_region["region"] == region]
        fig.add_trace(
            go.Bar(
                x=subset["period"],
                y=subset["revenue"].round(2),
                name=region,
            ),
            row=2, col=1,
        )
    fig.update_xaxes(categoryorder="category ascending", row=2, col=1)

    # Chart 4: Order Status Breakdown (Donut)
    fig.add_trace(
        go.Pie(
            labels=by_status["status"].str.title(),
            values=by_status["orders"],
            hole=0.4,
            name="Status",
            showlegend=True,
        ),
        row=2, col=2,
    )

    # Layout
    fig.update_layout(
        title=dict(
            text="Sales Development Dashboard",
            font=dict(size=22),
            x=0.5,
        ),
        height=780,
        barmode="group",
        plot_bgcolor="#f8fafc",
        paper_bgcolor="#ffffff",
        font=dict(family="Inter, Arial, sans-serif", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
    )

    # Secondary y-axis label for orders
    fig.update_yaxes(title_text="Revenue (€)", secondary_y=False, row=1, col=1)
    fig.update_yaxes(title_text="Orders",      secondary_y=True,  row=1, col=1)

    return fig


def main() -> None:
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run etl_pipeline.py first.")
        return

    con = duckdb.connect(str(DB_PATH), read_only=True)
    data = load_data(con)
    con.close()

    fig = build_dashboard(data)
    fig.show()


if __name__ == "__main__":
    main()
