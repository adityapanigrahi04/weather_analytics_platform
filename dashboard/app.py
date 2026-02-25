import os
import pandas as pd
import streamlit as st
import snowflake.connector

st.set_page_config(page_title="Weather Analytics", layout="wide")
st.title("Weather Data Analytics Dashboard")


def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "WEATHER_ANALYST"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_WEATHER"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "WEATHER_DW"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    )


@st.cache_data(ttl=300)
def load_daily_summary(limit: int = 5000):
    query = f"""
        SELECT location_key, observed_date, avg_temp, max_temp, min_temp, total_precip_mm
        FROM WEATHER_DW.ANALYTICS.mv_daily_weather_summary
        ORDER BY observed_date DESC
        LIMIT {limit}
    """
    with get_conn() as conn:
        return pd.read_sql(query, conn)


df = load_daily_summary()

col1, col2 = st.columns(2)
with col1:
    st.subheader("Average Temperature Trend")
    trend = df.groupby("OBSERVED_DATE", as_index=False)["AVG_TEMP"].mean()
    st.line_chart(trend, x="OBSERVED_DATE", y="AVG_TEMP")

with col2:
    st.subheader("Precipitation by Day")
    precip = df.groupby("OBSERVED_DATE", as_index=False)["TOTAL_PRECIP_MM"].sum()
    st.bar_chart(precip, x="OBSERVED_DATE", y="TOTAL_PRECIP_MM")

st.subheader("Latest Metrics")
st.dataframe(df.head(100), use_container_width=True)
