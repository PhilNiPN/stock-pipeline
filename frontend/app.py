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
    
    st.header("Technical Indicators")
    show_macd = st.checkbox("MACD", value=True, help="Moving Average Convergence Divergence - shows momentum")
    show_bollinger = st.checkbox("Bollinger Bands", value=True, help="Price volatility bands around moving average")
    show_rsi = st.checkbox("RSI", value=True, help="Relative Strength Index - momentum oscillator (0-100)")
    
    with st.expander("Indicator Information"):
        st.markdown("""
        **MACD (Moving Average Convergence Divergence)**
        - Blue line: MACD line (12-EMA - 26-EMA)
        - Red line: Signal line (9-EMA of MACD)
        - Histogram: MACD - Signal (green=bullish, red=bearish)
        
        **Bollinger Bands**
        - Upper/Lower bands: 2 standard deviations from 20-period SMA
        - Middle band: 20-period Simple Moving Average
        - Price touching bands may indicate overbought/oversold conditions
        
        **RSI (Relative Strength Index)**
        - Range: 0-100
        - >70: Overbought (potential sell signal)
        - <30: Oversold (potential buy signal)
        - 50: Neutral level
        """)

keep = ["date","open","high","low","close","volume","return","volatility","ema_9","ema_20","ema_50",
        "macd","macd_signal","macd_histogram",
        "bb_middle","bb_upper","bb_lower","bb_width","bb_position",
        "rsi"]
query = text("""
    SELECT date, open, high, low, close, volume, return, volatility, ema_9, ema_20, ema_50,
           macd, macd_signal, macd_histogram,
           bb_middle, bb_upper, bb_lower, bb_width, bb_position,
           rsi
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

# Plot - Create subplots based on selected indicators
num_rows = 2  # Base: price chart + volume
if show_macd:
    num_rows += 1
if show_rsi:
    num_rows += 1

row_heights = [0.6, 0.2]  # Base: price + volume
if show_macd:
    row_heights.insert(-1, 0.15)  # Insert MACD before volume
if show_rsi:
    row_heights.insert(-1, 0.15)  # Insert RSI before volume

fig = make_subplots(rows=num_rows, cols=1, shared_xaxes=True, vertical_spacing=0.02,
                    row_heights=row_heights)

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

# Add Bollinger Bands
if show_bollinger and "bb_upper" in df.columns and "bb_lower" in df.columns and "bb_middle" in df.columns:
    # Upper band
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["bb_upper"], name="BB Upper", 
        mode="lines", line=dict(width=1, color="rgba(128,128,128,0.5)"),
        showlegend=False
    ), row=1, col=1)
    
    # Lower band
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["bb_lower"], name="BB Lower", 
        mode="lines", line=dict(width=1, color="rgba(128,128,128,0.5)"),
        fill="tonexty", fillcolor="rgba(128,128,128,0.1)",
        showlegend=False
    ), row=1, col=1)
    
    # Middle band
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["bb_middle"], name="BB Middle", 
        mode="lines", line=dict(width=1, color="rgba(128,128,128,0.7)")
    ), row=1, col=1)

# Add MACD chart
current_row = 2
if show_macd and "macd" in df.columns and "macd_signal" in df.columns and "macd_histogram" in df.columns:
    # MACD line
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["macd"], name="MACD", 
        mode="lines", line=dict(width=2, color="blue")
    ), row=current_row, col=1)
    
    # Signal line
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["macd_signal"], name="MACD Signal", 
        mode="lines", line=dict(width=2, color="red")
    ), row=current_row, col=1)
    
    # Histogram
    colors = ['green' if val >= 0 else 'red' for val in df["macd_histogram"]]
    fig.add_trace(go.Bar(
        x=df["date"], y=df["macd_histogram"], name="MACD Histogram",
        marker=dict(color=colors, opacity=0.7)
    ), row=current_row, col=1)
    
    # Zero line
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=current_row, col=1)
    current_row += 1

# Add RSI chart
if show_rsi and "rsi" in df.columns:
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["rsi"], name="RSI", 
        mode="lines", line=dict(width=2, color="purple")
    ), row=current_row, col=1)
    
    # RSI levels
    fig.add_hline(y=70, line_dash="dash", line_color="red", row=current_row, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", row=current_row, col=1)
    fig.add_hline(y=50, line_dash="dot", line_color="gray", row=current_row, col=1)
    current_row += 1

# Add volume chart
if "volume" in df.columns:
    up = (df["close"] >= df["open"]).fillna(False).to_numpy()
    vol_colors = np.where(up, "green", "red")
    fig.add_trace(go.Bar(x=df["date"], y=df["volume"], name="Volume",
                         marker=dict(color=vol_colors)), row=current_row, col=1)

# Update layout
fig.update_layout(
    xaxis_rangeslider_visible=False,
    height=800 + (200 if show_macd else 0) + (200 if show_rsi else 0),
    margin=dict(l=10, r=10, t=40, b=10),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    hovermode="x unified"
)

# Update axis labels
fig.update_yaxes(title_text="Price", row=1, col=1)

current_row = 2
if show_macd:
    fig.update_yaxes(title_text="MACD", row=current_row, col=1)
    current_row += 1

if show_rsi:
    fig.update_yaxes(title_text="RSI", row=current_row, col=1, range=[0, 100])
    current_row += 1

fig.update_yaxes(title_text="Volume", row=current_row, col=1)
fig.update_xaxes(showspikes=True, spikesnap="cursor", spikemode="across")

st.plotly_chart(fig, use_container_width=True)

# Technical Indicators Summary
if not df.empty:
    st.subheader("Technical Indicators Summary")
    
    latest = df.iloc[-1]
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Current Price", f"${latest['close']:.2f}")
        if 'return' in df.columns:
            daily_return = latest['return'] * 100 if not pd.isna(latest['return']) else 0
            st.metric("Daily Return", f"{daily_return:.2f}%")
    
    with col2:
        if 'rsi' in df.columns and not pd.isna(latest['rsi']):
            rsi_value = latest['rsi']
            rsi_status = "Overbought" if rsi_value > 70 else "Oversold" if rsi_value < 30 else "Neutral"
            st.metric("RSI", f"{rsi_value:.1f}", rsi_status)
        
        if 'bb_position' in df.columns and not pd.isna(latest['bb_position']):
            bb_pos = latest['bb_position']
            bb_status = "Above Upper" if bb_pos > 1 else "Below Lower" if bb_pos < 0 else "Within Bands"
            st.metric("BB Position", f"{bb_pos:.2f}", bb_status)
    
    with col3:
        if 'macd' in df.columns and not pd.isna(latest['macd']):
            macd_value = latest['macd']
            macd_signal = latest.get('macd_signal', 0) if not pd.isna(latest.get('macd_signal')) else 0
            macd_status = "Bullish" if macd_value > macd_signal else "Bearish"
            st.metric("MACD", f"{macd_value:.4f}", macd_status)
        
        if 'volatility' in df.columns and not pd.isna(latest['volatility']):
            vol_value = latest['volatility'] * 100
            st.metric("Volatility", f"{vol_value:.1f}%")
    
    with col4:
        if 'ema_9' in df.columns and not pd.isna(latest['ema_9']):
            ema9 = latest['ema_9']
            ema20 = latest.get('ema_20', 0) if not pd.isna(latest.get('ema_20')) else 0
            ema_status = "Above EMA20" if ema9 > ema20 else "Below EMA20"
            st.metric("EMA 9", f"${ema9:.2f}", ema_status)
        
        if 'bb_width' in df.columns and not pd.isna(latest['bb_width']):
            bb_width = latest['bb_width'] * 100
            st.metric("BB Width", f"{bb_width:.1f}%")

st.subheader("Raw data")
st.dataframe(df[keep if set(keep).issubset(df.columns) else df.columns])