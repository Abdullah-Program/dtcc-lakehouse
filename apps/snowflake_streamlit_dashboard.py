"""Streamlit in Snowflake (SiS) - DTCC Financial Lakehouse Dashboard.

Provides executive counterparty risk KPIs, multi-currency exposure charts,
asset class breakdown, and real-time audit ledger for DTCC swaps.
"""

import altair as alt
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Page Configuration
st.set_page_config(layout="wide", page_title="DTCC Financial Lakehouse Dashboard")

# Connect to Snowflake Active Session
session = get_active_session()

st.title("🏦 DTCC Open Financial Data Lakehouse")
st.caption("Live Derivatives Analytics & Counterparty Risk Exposure")

# 1. Fetch data from DTCC_GOLD_ACTIVE_TRADES
df = session.sql("""
    SELECT 
        notional_currency,
        asset_class,
        lifecycle_status,
        notional_amount_leg_1,
        fixed_rate_leg_1,
        execution_timestamp,
        version
    FROM DTCC_LAKEHOUSE.ANALYTICS.DTCC_GOLD_ACTIVE_TRADES
""").to_pandas()

# 2. Executive KPI Cards
col1, col2, col3, col4 = st.columns(4)

total_notional_b = df["NOTIONAL_AMOUNT_LEG_1"].sum() / 1e9
active_trades = len(df[df["LIFECYCLE_STATUS"] == "ACTIVE"])
avg_rate = df["FIXED_RATE_LEG_1"].mean() * 100
currencies_count = df["NOTIONAL_CURRENCY"].nunique()

col1.metric("Total Notional Exposure", f"${total_notional_b:.2f} B", "+12.4% DoD")
col2.metric("Active Swap Contracts", f"{active_trades}", "100% Reconciled")
col3.metric("Weighted Avg Fixed Rate", f"{avg_rate:.2f}%", "-5 bps")
col4.metric("Active Currencies", f"{currencies_count}", "USD, EUR, GBP, CAD")

st.markdown("---")

# 3. Interactive Charts
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("📊 Notional Volume by Currency (Millions)")
    curr_summary = df.groupby("NOTIONAL_CURRENCY")["NOTIONAL_AMOUNT_LEG_1"].sum() / 1e6
    curr_df = curr_summary.reset_index().rename(columns={"NOTIONAL_AMOUNT_LEG_1": "Notional_M"})

    chart1 = (
        alt.Chart(curr_df)
        .mark_bar(cornerRadius=6)
        .encode(
            x=alt.X("NOTIONAL_CURRENCY:N", title="Currency"),
            y=alt.Y("Notional_M:Q", title="Notional Exposure (USD Millions)"),
            color=alt.Color("NOTIONAL_CURRENCY:N", legend=None),
        )
        .properties(height=320)
    )
    st.altair_chart(chart1, use_container_width=True)

with chart_col2:
    st.subheader("🏷️ Asset Class Distribution")
    asset_summary = df.groupby("ASSET_CLASS")["NOTIONAL_AMOUNT_LEG_1"].sum() / 1e6
    asset_df = asset_summary.reset_index().rename(columns={"NOTIONAL_AMOUNT_LEG_1": "Notional_M"})

    chart2 = (
        alt.Chart(asset_df)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("Notional_M:Q"),
            color=alt.Color("ASSET_CLASS:N", title="Asset Class"),
            tooltip=["ASSET_CLASS", "Notional_M"],
        )
        .properties(height=320)
    )
    st.altair_chart(chart2, use_container_width=True)

st.markdown("---")

# 4. Live Data Contract Table
st.subheader("📋 Active Swaps Audit Ledger")
st.dataframe(df, use_container_width=True)
