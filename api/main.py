"""
FastAPI application for Stock Pipeline API.
Provides REST endpoints for accessing stock price data and technical indicators.
"""
from datetime import date, datetime, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.orm import Session
import pandas as pd

from .database import get_db, engine
from .models import (
    TickerResponse,
    PriceDataResponse,
    IndicatorDataResponse,
    SummaryResponse,
    HealthResponse
)

app = FastAPI(
    title="Stock Pipeline API",
    description="REST API for accessing stock market data with technical indicators",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API information."""
    return {
        "name": "Stock Pipeline API",
        "version": "1.0.0",
        "docs": "/api/docs",
        "endpoints": {
            "tickers": "/api/v1/tickers",
            "prices": "/api/v1/prices/{ticker}",
            "indicators": "/api/v1/indicators/{ticker}",
            "summary": "/api/v1/summary/{ticker}"
        }
    }


@app.get("/api/health", response_model=HealthResponse, tags=["Health"])
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint - verifies database connectivity."""
    try:
        db.execute(text("SELECT 1"))
        return HealthResponse(
            status="healthy",
            database="connected",
            timestamp=datetime.utcnow()
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")


@app.get("/api/v1/tickers", response_model=TickerResponse, tags=["Data"])
async def get_tickers(db: Session = Depends(get_db)):
    """
    Get list of all available stock tickers.
    
    Returns:
        List of unique ticker symbols with metadata
    """
    try:
        query = text("""
            SELECT 
                ticker,
                MIN(date) as first_date,
                MAX(date) as last_date,
                COUNT(*) as record_count
            FROM price_metrics
            GROUP BY ticker
            ORDER BY ticker
        """)
        result = db.execute(query)
        
        tickers = []
        for row in result:
            tickers.append({
                "ticker": row[0],
                "first_date": row[1],
                "last_date": row[2],
                "record_count": row[3]
            })
        
        return TickerResponse(tickers=tickers, count=len(tickers))
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching tickers: {str(e)}")


@app.get("/api/v1/prices/{ticker}", response_model=PriceDataResponse, tags=["Data"])
async def get_prices(
    ticker: str,
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records"),
    db: Session = Depends(get_db)
):
    """
    Get OHLCV price data and technical indicators for a specific ticker.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Optional start date filter
        end_date: Optional end date filter
        limit: Maximum number of records to return (default: 1000, max: 10000)
    
    Returns:
        Price data with all technical indicators
    """
    ticker = ticker.upper()
    
    # Default date range if not provided
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=365)
    
    try:
        query = text("""
            SELECT 
                date, ticker, open, high, low, close, adj_close, volume,
                return, volatility,
                ema_9, ema_20, ema_50,
                macd, macd_signal, macd_histogram,
                bb_middle, bb_upper, bb_lower, bb_width, bb_position,
                rsi
            FROM price_metrics
            WHERE ticker = :ticker
                AND date BETWEEN :start_date AND :end_date
            ORDER BY date DESC
            LIMIT :limit
        """)
        
        result = db.execute(
            query,
            {"ticker": ticker, "start_date": start_date, "end_date": end_date, "limit": limit}
        )
        
        data = []
        for row in result:
            data.append({
                "date": row[0],
                "ticker": row[1],
                "open": float(row[2]) if row[2] is not None else None,
                "high": float(row[3]) if row[3] is not None else None,
                "low": float(row[4]) if row[4] is not None else None,
                "close": float(row[5]) if row[5] is not None else None,
                "adj_close": float(row[6]) if row[6] is not None else None,
                "volume": int(row[7]) if row[7] is not None else None,
                "return": float(row[8]) if row[8] is not None else None,
                "volatility": float(row[9]) if row[9] is not None else None,
                "ema_9": float(row[10]) if row[10] is not None else None,
                "ema_20": float(row[11]) if row[11] is not None else None,
                "ema_50": float(row[12]) if row[12] is not None else None,
                "macd": float(row[13]) if row[13] is not None else None,
                "macd_signal": float(row[14]) if row[14] is not None else None,
                "macd_histogram": float(row[15]) if row[15] is not None else None,
                "bb_middle": float(row[16]) if row[16] is not None else None,
                "bb_upper": float(row[17]) if row[17] is not None else None,
                "bb_lower": float(row[18]) if row[18] is not None else None,
                "bb_width": float(row[19]) if row[19] is not None else None,
                "bb_position": float(row[20]) if row[20] is not None else None,
                "rsi": float(row[21]) if row[21] is not None else None,
            })
        
        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for ticker {ticker} in the specified date range"
            )
        
        return PriceDataResponse(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            count=len(data),
            data=data
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching price data: {str(e)}")


@app.get("/api/v1/indicators/{ticker}", response_model=IndicatorDataResponse, tags=["Data"])
async def get_indicators(
    ticker: str,
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records"),
    db: Session = Depends(get_db)
):
    """
    Get technical indicators only for a specific ticker.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Optional start date filter
        end_date: Optional end date filter
        limit: Maximum number of records to return (default: 1000, max: 10000)
    
    Returns:
        Technical indicators data (EMAs, MACD, Bollinger Bands, RSI)
    """
    ticker = ticker.upper()
    
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=365)
    
    try:
        query = text("""
            SELECT 
                date, ticker,
                ema_9, ema_20, ema_50,
                macd, macd_signal, macd_histogram,
                bb_middle, bb_upper, bb_lower, bb_width, bb_position,
                rsi, volatility
            FROM price_metrics
            WHERE ticker = :ticker
                AND date BETWEEN :start_date AND :end_date
            ORDER BY date DESC
            LIMIT :limit
        """)
        
        result = db.execute(
            query,
            {"ticker": ticker, "start_date": start_date, "end_date": end_date, "limit": limit}
        )
        
        data = []
        for row in result:
            data.append({
                "date": row[0],
                "ticker": row[1],
                "ema_9": float(row[2]) if row[2] is not None else None,
                "ema_20": float(row[3]) if row[3] is not None else None,
                "ema_50": float(row[4]) if row[4] is not None else None,
                "macd": float(row[5]) if row[5] is not None else None,
                "macd_signal": float(row[6]) if row[6] is not None else None,
                "macd_histogram": float(row[7]) if row[7] is not None else None,
                "bb_middle": float(row[8]) if row[8] is not None else None,
                "bb_upper": float(row[9]) if row[9] is not None else None,
                "bb_lower": float(row[10]) if row[10] is not None else None,
                "bb_width": float(row[11]) if row[11] is not None else None,
                "bb_position": float(row[12]) if row[12] is not None else None,
                "rsi": float(row[13]) if row[13] is not None else None,
                "volatility": float(row[14]) if row[14] is not None else None,
            })
        
        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for ticker {ticker} in the specified date range"
            )
        
        return IndicatorDataResponse(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            count=len(data),
            data=data
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching indicators: {str(e)}")


@app.get("/api/v1/summary/{ticker}", response_model=SummaryResponse, tags=["Data"])
async def get_summary(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    Get latest summary statistics and technical analysis for a ticker.
    
    Args:
        ticker: Stock ticker symbol
    
    Returns:
        Latest price, indicators, and trading signals
    """
    ticker = ticker.upper()
    
    try:
        query = text("""
            SELECT 
                date, close, volume, return, volatility,
                ema_9, ema_20, ema_50,
                macd, macd_signal, macd_histogram,
                bb_middle, bb_upper, bb_lower, bb_position,
                rsi
            FROM price_metrics
            WHERE ticker = :ticker
            ORDER BY date DESC
            LIMIT 1
        """)
        
        result = db.execute(query, {"ticker": ticker}).fetchone()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for ticker {ticker}"
            )
        
        # Generate trading signals
        rsi = float(result[15]) if result[15] is not None else None
        macd = float(result[8]) if result[8] is not None else None
        macd_signal = float(result[9]) if result[9] is not None else None
        bb_position = float(result[14]) if result[14] is not None else None
        ema_9 = float(result[5]) if result[5] is not None else None
        ema_20 = float(result[6]) if result[6] is not None else None
        
        signals = {
            "rsi_signal": (
                "Overbought" if rsi and rsi > 70 else
                "Oversold" if rsi and rsi < 30 else
                "Neutral"
            ) if rsi else None,
            "macd_signal": (
                "Bullish" if macd and macd_signal and macd > macd_signal else
                "Bearish" if macd and macd_signal and macd < macd_signal else
                "Neutral"
            ) if macd and macd_signal else None,
            "bb_signal": (
                "Above Upper Band" if bb_position and bb_position > 1 else
                "Below Lower Band" if bb_position and bb_position < 0 else
                "Within Bands"
            ) if bb_position is not None else None,
            "ema_trend": (
                "Bullish" if ema_9 and ema_20 and ema_9 > ema_20 else
                "Bearish" if ema_9 and ema_20 and ema_9 < ema_20 else
                "Neutral"
            ) if ema_9 and ema_20 else None
        }
        
        return SummaryResponse(
            ticker=ticker,
            date=result[0],
            current_price=float(result[1]) if result[1] is not None else None,
            volume=int(result[2]) if result[2] is not None else None,
            daily_return=float(result[3]) if result[3] is not None else None,
            volatility=float(result[4]) if result[4] is not None else None,
            ema_9=ema_9,
            ema_20=ema_20,
            ema_50=float(result[7]) if result[7] is not None else None,
            macd=macd,
            macd_signal=macd_signal,
            macd_histogram=float(result[10]) if result[10] is not None else None,
            bb_middle=float(result[11]) if result[11] is not None else None,
            bb_upper=float(result[12]) if result[12] is not None else None,
            bb_lower=float(result[13]) if result[13] is not None else None,
            bb_position=bb_position,
            rsi=rsi,
            signals=signals
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching summary: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
