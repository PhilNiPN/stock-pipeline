import os
from datetime import date
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
import streamlit as st

st.set_page_config(page_title="Stock Dashboard", layout="wide")
st.title("Stock Candlesticks")

# Try to get DB_URL from secrets first, then environment variables, then construct from individual vars
DB_URL = None
try:
    DB_URL = st.secrets.get("DB_URL")
except:
    pass

if not DB_URL:
    DB_URL = os.getenv("DB_URL")

if not DB_URL:
    # Construct from individual environment variables
    db_host = os.getenv("DB_HOST", "db")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "stockdb")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "mysecretpassword")
    DB_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

@st.cache_resource
def get_engine(db_url: str):
    return create_engine(db_url, pool_pre_ping=True)

engine = get_engine(DB_URL)

@st.cache_data(ttl=120)
def get_tickers(_engine) -> list[str]:
    with _engine.begin() as conn:
        return conn.execute(
            text("SELECT DISTINCT ticker FROM price_metrics ORDER BY ticker")
        ).scalars().all()

with st.sidebar:
    st.header("Charts")
    options = get_tickers(engine) or ["AAPL","MSFT","NVDA","TSLA"]
    tick = st.selectbox("Ticker", options=options, index=0)
    start = st.date_input("Start", value=date(2022,1,1))
    end = st.date_input("End", value=date.today())

    st.header("Overlays")
    show_ema9  = st.checkbox("EMA 9",  value=True)
    show_ema20 = st.checkbox("EMA 20", value=True)
    show_ema50 = st.checkbox("EMA 50", value=True)
    ema_line_width = st.slider("EMA line width", 1, 5, 2)

keep = ["date","open","high","low","close","volume","ema_9","ema_20","ema_50"]
query = text("""
    SELECT date, open, high, low, close, volume, ema_9, ema_20, ema_50
    FROM price_metrics
    WHERE ticker = :t AND date BETWEEN :s AND :e
    ORDER BY date
""")

try:
    with engine.begin() as conn:
        df = pd.read_sql(query, conn, params={"t": tick.upper(), "s": start, "e": end})
except Exception as e:
    st.error("Database query failed.")
    st.exception(e)
    st.stop()

if df.empty:
    st.info("No rows for your selection yet. Try another range or run the ETL.")
    st.stop()

# Coerce types and validate
df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
num_cols = [c for c in keep if c in df.columns and c != "date"]
for c in num_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["date"]).sort_values("date")
missing = {"open","high","low","close"} - set(df.columns)
if missing:
    st.warning(f"Missing required columns: {', '.join(sorted(missing))}")

# Plot
fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                    row_heights=[0.75, 0.25])

fig.add_trace(go.Candlestick(
    x=df["date"],
    open=df.get("open"), high=df.get("high"), low=df.get("low"), close=df.get("close"),
    name="Candles"
), row=1, col=1)

def add_ema(col, label):
    if col in df.columns:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df[col], name=label, mode="lines", line=dict(width=ema_line_width)
        ), row=1, col=1)

if show_ema9:  add_ema("ema_9",  "EMA 9")
if show_ema20: add_ema("ema_20", "EMA 20")
if show_ema50: add_ema("ema_50", "EMA 50")

if "volume" in df.columns:
    up = (df["close"] >= df["open"]).fillna(False).to_numpy()
    vol_colors = np.where(up, "green", "red")
    fig.add_trace(go.Bar(x=df["date"], y=df["volume"], name="Volume",
                         marker=dict(color=vol_colors)), row=2, col=1)

fig.update_layout(
    xaxis_rangeslider_visible=False,
    height=700,
    margin=dict(l=10, r=10, t=40, b=10),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    hovermode="x unified"
)
fig.update_yaxes(title_text="Price", row=1, col=1)
fig.update_yaxes(title_text="Volume", row=2, col=1)
fig.update_xaxes(showspikes=True, spikesnap="cursor", spikemode="across")

st.plotly_chart(fig, use_container_width=True)

st.subheader("Raw data")
st.dataframe(df[keep if set(keep).issubset(df.columns) else df.columns])