"""
OmniFlow Executive Analytics & Real-Time Monitoring Dashboard
Module 5: Data Platform Visualization & Health Monitor
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb

# Configure layout
st.set_page_config(
    page_title="OmniFlow | E-Commerce Data Platform",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (Dark Neon Glassmorphism)
st.markdown("""
<style>
    .main {
        background-color: #0b0f19;
    }
    .metric-card {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.7), rgba(15, 23, 42, 0.9));
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 18px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.4);
        backdrop-filter: blur(10px);
        margin-bottom: 12px;
    }
    .metric-title {
        color: #94a3b8;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-value {
        color: #f8fafc;
        font-size: 1.8rem;
        font-weight: 700;
        margin-top: 4px;
    }
    .badge-critical {
        background-color: #ef4444;
        color: white;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .badge-high {
        background-color: #f97316;
        color: white;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .badge-medium {
        background-color: #eab308;
        color: black;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE_PATH = PROJECT_ROOT / "data_warehouse" / "warehouse.duckdb"
DATA_LAKE_PATH = PROJECT_ROOT / "data_lake"


def query_df(query, params=None):
    """Executes read-only query and closes connection immediately to avoid locking."""
    if not WAREHOUSE_PATH.exists():
        return pd.DataFrame()
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    try:
        if params:
            return con.execute(query, params).df()
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()
    finally:
        con.close()


def query_one(query, params=None):
    """Executes single-row query and closes connection immediately."""
    if not WAREHOUSE_PATH.exists():
        return None
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    try:
        if params:
            return con.execute(query, params).fetchone()
        return con.execute(query).fetchone()
    except Exception:
        return None
    finally:
        con.close()


def load_kpis():
    if not WAREHOUSE_PATH.exists():
        return {"orders": 0, "revenue": 0.0, "aov": 0.0, "fraud": 0, "customers": 0}

    orders_row = query_one("""
        SELECT 
            COUNT(*) AS total_orders,
            COALESCE(SUM(total_amount), 0.0) AS total_revenue,
            COALESCE(AVG(total_amount), 0.0) AS aov,
            COUNT(CASE WHEN is_fraud_suspect = true THEN 1 END) AS fraud_alerts
        FROM raw_orders
    """)

    cust_row = query_one("SELECT COUNT(*) FROM ecommerce_raw.customers")
    customers_cnt = cust_row[0] if cust_row else 0

    if orders_row:
        return {
            "orders": orders_row[0],
            "revenue": round(orders_row[1], 2),
            "aov": round(orders_row[2], 2),
            "fraud": orders_row[3],
            "customers": customers_cnt
        }
    return {"orders": 0, "revenue": 0.0, "aov": 0.0, "fraud": 0, "customers": 0}


# Sidebar Controls
with st.sidebar:
    st.image("https://images.unsplash.com/photo-1551288049-bebda4e38f71?auto=format&fit=crop&w=600&q=80", caption="OmniFlow Lakehouse Platform")
    st.title("Control Center")
    st.caption("Data Engineering Zoomcamp Architecture")

    st.markdown("---")
    st.subheader("Live Stream Simulation")
    stream_count = st.slider("Events to emit", min_value=10, max_value=100, value=25, step=5)
    
    if st.button("Emit Live Kafka Events", use_container_width=True, type="primary"):
        with st.spinner("Emitting events & updating Lake..."):
            subprocess.run([sys.executable, "streaming/producer.py", str(stream_count), "0.01"], cwd=str(PROJECT_ROOT))
            subprocess.run([sys.executable, "streaming/consumer_to_lake.py"], cwd=str(PROJECT_ROOT))
            subprocess.run([sys.executable, "streaming/fraud_detector.py"], cwd=str(PROJECT_ROOT))
            st.success(f"Emitted & ingested {stream_count} events!")
            st.rerun()

    st.markdown("---")
    st.subheader("Orchestration Triggers")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Run dlt", use_container_width=True):
            subprocess.run([sys.executable, "ingestion/dlt_pipeline.py"], cwd=str(PROJECT_ROOT))
            st.success("dlt run OK!")
            st.rerun()
    with col_b:
        if st.button("Run Spark", use_container_width=True):
            subprocess.run([sys.executable, "batch/run_batch.py"], cwd=str(PROJECT_ROOT))
            st.success("Batch run OK!")
            st.rerun()

    if st.button("Run dbt Marts & Tests", use_container_width=True):
        subprocess.run([sys.executable, "analytics_dbt/run_dbt.py"], cwd=str(PROJECT_ROOT))
        st.success("dbt models updated!")
        st.rerun()

    st.markdown("---")
    st.caption("Active Target: **DuckDB / Lakehouse (Local)**")
    st.caption("GCP BigQuery adapter: **Ready**")


# Header
st.title("OmniFlow | Real-Time E-Commerce Lakehouse")
st.markdown("End-to-End Enterprise Data Architecture: **Docker • Kafka • MinIO Lake • Kestra • dltHub • BigQuery/DuckDB • dbt • Apache Spark • Bruin**")

# Top KPI Row
kpis = load_kpis()
c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total Orders</div>
        <div class="metric-value">{kpis['orders']:,}</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Gross Revenue</div>
        <div class="metric-value">${kpis['revenue']:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Avg Order Value</div>
        <div class="metric-value">${kpis['aov']:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Fraud Suspects</div>
        <div class="metric-value" style="color: #f87171;">{kpis['fraud']}</div>
    </div>
    """, unsafe_allow_html=True)

with c5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Catalog / Users</div>
        <div class="metric-value">{kpis['customers']}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Main Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "Analytics Marts (dbt & Spark)",
    "Real-Time Streaming & Fraud Alerts",
    "dlt Ingested Catalog & Customers",
    "Pipeline Architecture & Health"
])

with tab1:
    df_sales = query_df("""
        SELECT 
            CAST(created_at AS DATE) AS order_date,
            ROUND(SUM(total_amount), 2) AS revenue,
            COUNT(order_id) AS orders,
            ROUND(AVG(total_amount), 2) AS aov
        FROM raw_orders
        GROUP BY 1
        ORDER BY 1 ASC
    """)

    col_chart1, col_chart2 = st.columns([2, 1])

    with col_chart1:
        st.subheader("Daily Revenue & Order Trajectory")
        if not df_sales.empty:
            fig_rev = px.area(
                df_sales,
                x="order_date",
                y="revenue",
                title="Gross Sales Trend (USD)",
                color_discrete_sequence=["#38bdf8"]
            )
            fig_rev.update_layout(template="plotly_dark", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_rev, use_container_width=True)
        else:
            st.info("No sales data yet. Click 'Emit Live Kafka Events' on the sidebar.")

    with col_chart2:
        st.subheader("Sales by Country")
        df_country = query_df("""
            SELECT country, ROUND(SUM(total_amount), 2) AS revenue
            FROM raw_orders
            GROUP BY country
            ORDER BY revenue DESC
            LIMIT 8
        """)
        if not df_country.empty:
            fig_country = px.bar(
                df_country,
                x="country",
                y="revenue",
                color="revenue",
                color_continuous_scale="Blues"
            )
            fig_country.update_layout(template="plotly_dark", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_country, use_container_width=True)

    st.markdown("---")
    col_rfm, col_cat = st.columns(2)

    with col_rfm:
        st.subheader("Customer RFM Value Segmentation")
        df_rfm = query_df("SELECT customer_segment, count(*) as count FROM gold_customer_rfm GROUP BY 1")
        if not df_rfm.empty:
            fig_rfm = px.pie(
                df_rfm,
                values="count",
                names="customer_segment",
                hole=0.45,
                color_discrete_sequence=px.colors.sequential.Teal
            )
            fig_rfm.update_layout(template="plotly_dark", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_rfm, use_container_width=True)
        else:
            st.caption("Run Spark batch to compute RFM segments.")

    with col_cat:
        st.subheader("Revenue by Product Category")
        df_cat = query_df("SELECT category, total_revenue FROM gold_category_sales ORDER BY total_revenue DESC")
        if not df_cat.empty:
            fig_cat = px.bar(
                df_cat,
                x="category",
                y="total_revenue",
                color="total_revenue",
                color_continuous_scale="Viridis"
            )
            fig_cat.update_layout(template="plotly_dark", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_cat, use_container_width=True)
        else:
            st.caption("Run Spark batch to view category revenue.")

with tab2:
    st.subheader("Real-Time Stream Ingestion & Fraud Detection")
    col_alerts, col_stream = st.columns([1, 1])

    with col_alerts:
        st.markdown("#### High-Risk & Fraud Monitoring")
        df_alerts = query_df("""
            SELECT alert_id, order_id, customer_id, total_amount, alert_type, severity, detected_at
            FROM fraud_alerts
            ORDER BY detected_at DESC
            LIMIT 10
        """)
        if not df_alerts.empty:
            st.dataframe(df_alerts, use_container_width=True)
        else:
            st.info("No active fraud alerts detected.")

    with col_stream:
        st.markdown("#### Latest Streaming Orders (Landing Zone)")
        df_stream = query_df("""
            SELECT order_id, customer_id, primary_category, total_amount, payment_method, city, country, is_fraud_suspect, created_at
            FROM raw_orders
            ORDER BY created_at DESC
            LIMIT 10
        """)
        if not df_stream.empty:
            st.dataframe(df_stream, use_container_width=True)
        else:
            st.info("No streaming orders found.")

with tab3:
    st.subheader("dltHub Ingested Tables (Staging & Catalog)")
    c_cust, c_prod = st.columns(2)
    with c_cust:
        st.markdown("#### Ingested Customers (dlt Source)")
        df_c = query_df("SELECT customer_id, first_name, last_name, country, tier, loyalty_points FROM ecommerce_raw.customers LIMIT 10")
        if not df_c.empty:
            st.dataframe(df_c, use_container_width=True)
        else:
            st.info("Run dlt pipeline to view customers.")

    with c_prod:
        st.markdown("#### Ingested Catalog (dlt Source)")
        df_p = query_df("SELECT product_id, name, category, cost_price, retail_price, stock_quantity FROM ecommerce_raw.products")
        if not df_p.empty:
            st.dataframe(df_p, use_container_width=True)
        else:
            st.info("Run dlt pipeline to view catalog.")

with tab4:
    st.subheader("End-to-End System Architecture (DataTalksClub Zoomcamp)")
    
    parquet_count = len(list(DATA_LAKE_PATH.glob("**/*.parquet")))
    order_count_res = query_one("SELECT count(*) FROM raw_orders")
    total_db_orders = order_count_res[0] if order_count_res else 0

    st.markdown(f"""
    | Component | Technology | Role | Status / Metrics |
    | :--- | :--- | :--- | :--- |
    | **Module 1** | Docker & PostgreSQL | Containerization, local OLTP & metadata | [READY] Online |
    | **Module 7** | Apache Kafka | Streaming orders & clickstream producer/consumer | [ACTIVE] Real-Time |
    | **Data Lake** | MinIO / S3 / Local | Partitioned Parquet Lake (bronze/silver/gold) | [LAKE] **{parquet_count} Parquet Partitions** |
    | **Module 2** | Kestra | Declarative YAML Workflow Orchestrator | [ORCHESTRATOR] Configured |
    | **Workshop** | dltHub (dlt) | REST API ingestion, auto-schema inference & evolution | [INGESTED] Synchronized |
    | **Module 3** | BigQuery / DuckDB | High-performance Analytical Data Warehouse | [WAREHOUSE] **{total_db_orders} Orders Stored** |
    | **Module 6** | Apache Spark | Distributed RFM & batch aggregation | [BATCH] Aggregated |
    | **Module 4** | dbt (Data Build Tool) | Staging views, dimensional marts & quality tests | [DBT] Validated |
    | **Module 5** | Bruin & Streamlit | Data Platform specs, quality assertions & Executive UI | [PLATFORM] Active |
    """)

    st.markdown("---")
    st.subheader("Infrastructure as Code (Terraform)")
    st.code("""
# Provision GCP Storage Lake & BigQuery datasets
cd terraform
terraform init
terraform plan
terraform apply
    """, language="bash")
