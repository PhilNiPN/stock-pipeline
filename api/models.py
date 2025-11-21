"""
Pydantic models for request/response validation and API documentation.
"""
from datetime import date, datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    database: str
    timestamp: datetime


class TickerInfo(BaseModel):
    """Individual ticker metadata."""
    ticker: str
    first_date: date
    last_date: date
    record_count: int


class TickerResponse(BaseModel):
    """Response model for /tickers endpoint."""
    tickers: List[TickerInfo]
    count: int


class PriceData(BaseModel):
    """Complete price and indicator data for a single date."""
    date: date
    ticker: str
    
    # OHLCV data
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    adj_close: Optional[float] = None
    volume: Optional[int] = None
    
    # Basic metrics
    return_: Optional[float] = Field(None, alias="return")
    volatility: Optional[float] = None
    
    # EMAs
    ema_9: Optional[float] = None
    ema_20: Optional[float] = None
    ema_50: Optional[float] = None
    
    # MACD
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    
    # Bollinger Bands
    bb_middle: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_width: Optional[float] = None
    bb_position: Optional[float] = None
    
    # RSI
    rsi: Optional[float] = None
    
    class Config:
        populate_by_name = True


class PriceDataResponse(BaseModel):
    """Response model for /prices endpoint."""
    ticker: str
    start_date: date
    end_date: date
    count: int
    data: List[PriceData]


class IndicatorData(BaseModel):
    """Technical indicators only for a single date."""
    date: date
    ticker: str
    
    # EMAs
    ema_9: Optional[float] = None
    ema_20: Optional[float] = None
    ema_50: Optional[float] = None
    
    # MACD
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    
    # Bollinger Bands
    bb_middle: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_width: Optional[float] = None
    bb_position: Optional[float] = None
    
    # RSI
    rsi: Optional[float] = None
    
    # Volatility
    volatility: Optional[float] = None


class IndicatorDataResponse(BaseModel):
    """Response model for /indicators endpoint."""
    ticker: str
    start_date: date
    end_date: date
    count: int
    data: List[IndicatorData]


class SummaryResponse(BaseModel):
    """Response model for /summary endpoint with trading signals."""
    ticker: str
    date: date
    
    # Latest prices
    current_price: Optional[float] = None
    volume: Optional[int] = None
    daily_return: Optional[float] = None
    volatility: Optional[float] = None
    
    # EMAs
    ema_9: Optional[float] = None
    ema_20: Optional[float] = None
    ema_50: Optional[float] = None
    
    # MACD
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    
    # Bollinger Bands
    bb_middle: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_position: Optional[float] = None
    
    # RSI
    rsi: Optional[float] = None
    
    # Trading signals
    signals: Dict[str, Optional[str]] = Field(
        default_factory=dict,
        description="Trading signals based on technical indicators"
    )
