# streamlit/app.py

import time
import pandas as pd
import streamlit as st
import clickhouse_connect

st.set_page_config(page_title="Weather Live Analytics", layout="wide")
st.title("🌦 Live Weather Analytics")

# --- ClickHouse connection (inside docker network) ---
client = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username="spark",
    password="sparkpass",
    database="weather",
)

# --- Sidebar controls ---
refresh_seconds = st.sidebar.slider("Auto refresh (seconds)", 1, 30, 5)
auto = st.sidebar.checkbox("Auto refresh", value=True)

rows = st.sidebar.slider("Rows to load", 50, 2000, 300)
cities_df = client.query_df("SELECT DISTINCT city FROM avg_temp_by_city ORDER BY city")
cities = ["All"] + cities_df["city"].tolist()

selected_city = st.sidebar.selectbox("City", cities)

# --- Load data from ClickHouse ---
events = client.query_df(f"""
    SELECT window_start, window_end, events
    FROM events_per_window
    ORDER BY window_start DESC
    LIMIT {rows}
""")

avg_temp = client.query_df(f"""
    SELECT window_start, window_end, city, avg_temp_c
    FROM avg_temp_by_city
    ORDER BY window_start DESC
    LIMIT {rows}
""")

# Sort for chart direction
events = events.sort_values("window_start")
avg_temp = avg_temp.sort_values("window_start")

# Optional filtering
if selected_city != "All":
    avg_temp = avg_temp[avg_temp["city"] == selected_city]

# --- KPIs ---
kpi1, kpi2, kpi3 = st.columns(3)

events_total = client.query("SELECT count() FROM events_per_window").result_rows[0][0]
avg_total = client.query("SELECT count() FROM avg_temp_by_city").result_rows[0][0]

latest_events = client.query("""
    SELECT events
    FROM events_per_window
    ORDER BY window_start DESC
    LIMIT 1
""").result_rows

latest_events_val = int(latest_events[0][0]) if latest_events else 0

with kpi1:
    st.metric("Total windows (events)", int(events_total))
with kpi2:
    st.metric("Total rows (avg temp)", int(avg_total))
with kpi3:
    st.metric("Latest events/10s", latest_events_val)

# --- Layout ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Events per 10 seconds")
    if len(events) == 0:
        st.info("No data yet.")
    else:
        st.line_chart(events.set_index("window_start")["events"])

with col2:
    st.subheader("Avg Temp per City ")

    latest_per_city = client.query_df(f"""
        SELECT city, argMax(avg_temp_c, window_start) AS avg_temp_c
        FROM avg_temp_by_city
        GROUP BY city
        ORDER BY avg_temp_c DESC
    """)

    if selected_city != "All":
        latest_per_city = latest_per_city[latest_per_city["city"] == selected_city]

    st.dataframe(latest_per_city, use_container_width=True)

# --- Avg temp over time (filtered) ---
st.subheader("Avg Temp Over Time")

if len(avg_temp) == 0:
    st.info("No data for this filter yet.")
else:
    pivot = avg_temp.pivot_table(
        index="window_start",
        columns="city",
        values="avg_temp_c",
        aggfunc="last"
    ).sort_index()
    pivot = pivot.ffill().bfill()
    st.line_chart(pivot)

# --- Top hottest cities now ---
st.subheader("Top Hottest Cities (latest per city)")
top_df = client.query_df("""
    SELECT city, argMax(avg_temp_c, window_start) AS avg_temp_c
    FROM avg_temp_by_city
    GROUP BY city
    ORDER BY avg_temp_c DESC
    LIMIT 10
""")
if selected_city != "All":
    top_df = top_df[top_df["city"] == selected_city]

st.bar_chart(top_df.set_index("city")["avg_temp_c"])

# --- Auto refresh ---
if auto:
    time.sleep(refresh_seconds)
    st.rerun()
