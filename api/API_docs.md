# Stock Pipeline API Documentation

## Overview

REST API for accessing stock market data with technical indicators. Built with FastAPI, this service provides programmatic access to historical OHLCV data and computed technical indicators stored in the PostgreSQL database.

## Base URL

When running locally with Docker Compose:
```
http://localhost:8000
```

## Interactive API Documentation

FastAPI automatically generates interactive API documentation:

- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc

## Authentication

Currently, the API is open access. For production deployments, consider adding:
- API key authentication
- Rate limiting
- JWT tokens for user-based access

## Endpoints

### Health Check

#### GET `/api/health`

Check API and database connectivity.

**Response:**
```json
{
  "status": "healthy",
  "database": "connected",
  "timestamp": "2025-10-14T12:34:56Z"
}
```

**Status Codes:**
- `200`: Service is healthy
- `503`: Service is unhealthy (database connection failed)

---

### Get All Tickers

#### GET `/api/v1/tickers`

Retrieve list of all available stock tickers with metadata.

**Response:**
```json
{
  "tickers": [
    {
      "ticker": "AAPL",
      "first_date": "2019-01-01",
      "last_date": "2025-10-14",
      "record_count": 1685
    }
  ],
  "count": 1
}
```

**Example:**
```bash
curl http://localhost:8000/api/v1/tickers
```

---

### Get Price Data

#### GET `/api/v1/prices/{ticker}`

Retrieve OHLCV price data with all technical indicators for a specific ticker.

**Path Parameters:**
- `ticker` (string, required): Stock ticker symbol (e.g., AAPL, NVDA)

**Query Parameters:**
- `start_date` (date, optional): Start date in YYYY-MM-DD format (default: 365 days ago)
- `end_date` (date, optional): End date in YYYY-MM-DD format (default: today)
- `limit` (integer, optional): Maximum records to return (default: 1000, max: 10000)

**Response:**
```json
{
  "ticker": "AAPL",
  "count": 252,
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "data": [
    {
      "date": "2024-01-02",
      "ticker": "AAPL",
      "open": 187.15,
      "high": 188.44,
      "low": 183.89,
      "close": 185.64,
      "adj_close": 185.64,
      "volume": 82488200,
      "return": -0.0085,
      "volatility": 0.2345,
      "ema_9": 186.32,
      "ema_20": 184.56,
      "ema_50": 182.91,
      "macd": 1.2345,
      "macd_signal": 1.1234,
      "macd_histogram": 0.1111,
      "bb_middle": 185.00,
      "bb_upper": 190.50,
      "bb_lower": 179.50,
      "bb_width": 0.0594,
      "bb_position": 0.56,
      "rsi": 58.34
    }
  ]
}
```

**Examples:**
```bash
# Get last 365 days of AAPL data
curl http://localhost:8000/api/v1/prices/AAPL

# Get specific date range
curl "http://localhost:8000/api/v1/prices/NVDA?start_date=2024-01-01&end_date=2024-12-31"

# Get last 100 days with limit
curl "http://localhost:8000/api/v1/prices/MSFT?limit=100"
```

**Status Codes:**
- `200`: Success
- `404`: Ticker not found or no data in date range
- `422`: Invalid parameters
- `500`: Server error

---

### Get Technical Indicators

#### GET `/api/v1/indicators/{ticker}`

Retrieve only technical indicators for a specific ticker (excludes OHLCV data for lighter response).

**Path Parameters:**
- `ticker` (string, required): Stock ticker symbol

**Query Parameters:**
- `start_date` (date, optional): Start date in YYYY-MM-DD format
- `end_date` (date, optional): End date in YYYY-MM-DD format
- `limit` (integer, optional): Maximum records to return (default: 1000, max: 10000)

**Response:**
```json
{
  "ticker": "NVDA",
  "count": 252,
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "data": [
    {
      "date": "2024-01-02",
      "ticker": "NVDA",
      "volatility": 0.3456,
      "ema_9": 495.23,
      "ema_20": 487.56,
      "ema_50": 475.12,
      "macd": 2.3456,
      "macd_signal": 2.1234,
      "macd_histogram": 0.2222,
      "bb_middle": 490.00,
      "bb_upper": 510.00,
      "bb_lower": 470.00,
      "bb_width": 0.0816,
      "bb_position": 0.63,
      "rsi": 62.45
    }
  ]
}
```

**Examples:**
```bash
# Get indicators for last 365 days
curl http://localhost:8000/api/v1/indicators/NVDA

# Get indicators for specific date range
curl "http://localhost:8000/api/v1/indicators/GOOGL?start_date=2024-06-01&end_date=2024-09-30"
```

---

### Get Summary

#### GET `/api/v1/summary/{ticker}`

Get the latest summary metrics with trading signals for a specific ticker.

**Path Parameters:**
- `ticker` (string, required): Stock ticker symbol

**Response:**
```json
{
  "ticker": "AAPL",
  "date": "2025-10-14",
  "current_price": 178.56,
  "volume": 82488200,
  "daily_return": 0.0045,
  "volatility": 0.2134,
  "ema_9": 180.12,
  "ema_20": 176.34,
  "ema_50": 170.00,
  "macd": 1.2345,
  "macd_signal": 1.0000,
  "macd_histogram": 0.2345,
  "bb_middle": 176.00,
  "bb_upper": 190.00,
  "bb_lower": 162.00,
  "bb_position": 0.56,
  "rsi": 58.34,
  "signals": {
    "rsi_signal": "Neutral",
    "macd_signal": "Bullish",
    "bb_signal": "Within Bands",
    "ema_trend": "Bullish"
  }
}
```

**Trading Signals:**
These are exposed in the `signals` field of the summary response:
- **RSI Signal**: "Overbought" (>70), "Oversold" (<30), or "Neutral"
- **MACD Signal**: "Bullish" (MACD > Signal) or "Bearish" (MACD < Signal)
- **BB Signal**: "Above Upper Band", "Below Lower Band", or "Within Bands"
- **EMA Trend**: "Bullish" (EMA9 > EMA20) or "Bearish" (EMA9 < EMA20)

**Examples:**
```bash
# Get latest summary for ticker
curl http://localhost:8000/api/v1/summary/AAPL

# Pretty print JSON
curl http://localhost:8000/api/v1/summary/NVDA | python -m json.tool
```

**Status Codes:**
- `200`: Success
- `404`: Ticker not found
- `500`: Server error

---

## Technical Indicators Reference

### EMAs (Exponential Moving Averages)
- **ema_9**: 9-period EMA - short-term trend
- **ema_20**: 20-period EMA - medium-term trend
- **ema_50**: 50-period EMA - long-term trend

### MACD (Moving Average Convergence Divergence)
- **macd**: MACD line (12-EMA - 26-EMA)
- **macd_signal**: Signal line (9-EMA of MACD)
- **macd_histogram**: Histogram (MACD - Signal)
- **Interpretation**: Bullish when MACD > Signal, bearish when MACD < Signal

### Bollinger Bands
- **bb_middle**: 20-period Simple Moving Average
- **bb_upper**: Upper band (Middle + 2*std dev)
- **bb_lower**: Lower band (Middle - 2*std dev)
- **bb_width**: Band width as percentage
- **bb_position**: Price position within bands (typically 0-1; values <0 or >1 indicate price outside the bands)
- **Interpretation**: Price near upper band = overbought, near lower band = oversold

### RSI (Relative Strength Index)
- **rsi**: 14-period RSI (0-100 scale)
- **Interpretation**: >70 = overbought, <30 = oversold, 50 = neutral

### Volatility Metrics
- **return**: Daily fractional return (e.g., 0.01 == 1%)
- **volatility**: 21-day rolling annualized volatility

---

## Error Handling

All endpoints return structured error responses:

```json
{
  "detail": "No data found for ticker XYZ in the specified date range"
}
```

**Common Status Codes:**
- `200`: Success
- `404`: Resource not found
- `422`: Validation error (invalid parameters)
- `500`: Internal server error

---

## Rate Limiting

Currently not implemented. For production:
- Implement token bucket or sliding window rate limiting
- Suggested limits: 100 requests/minute per IP
- Return `429 Too Many Requests` when exceeded

---

## Usage Examples

### Python (requests)

```python
import requests

# Get tickers
response = requests.get("http://localhost:8000/api/v1/tickers")
tickers = response.json()

# Get price data with date range
params = {
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "limit": 500
}
response = requests.get(
    "http://localhost:8000/api/v1/prices/AAPL",
    params=params
)
data = response.json()

# Get summary
response = requests.get("http://localhost:8000/api/v1/summary/NVDA")
summary = response.json()
print(f"Current Price: ${summary['current_price']:.2f}")
print(f"RSI: {summary['rsi']:.2f} ({summary['signals']['rsi_signal']})")
```

### JavaScript (fetch)

```javascript
// Get tickers
const response = await fetch('http://localhost:8000/api/v1/tickers');
const tickers = await response.json();

// Get price data
const params = new URLSearchParams({
    start_date: '2024-01-01',
    end_date: '2024-12-31',
    limit: 500
});
const priceResponse = await fetch(
    `http://localhost:8000/api/v1/prices/AAPL?${params}`
);
const priceData = await priceResponse.json();

// Get summary
const summaryResponse = await fetch('http://localhost:8000/api/v1/summary/NVDA');
const summary = await summaryResponse.json();
console.log(`RSI: ${summary.rsi} (${summary.signals.rsi_signal})`);
```

### curl

```bash
# Get all tickers
curl http://localhost:8000/api/v1/tickers | jq

# Get price data for AAPL
curl "http://localhost:8000/api/v1/prices/AAPL?start_date=2024-01-01&limit=100" | jq

# Get indicators only
curl http://localhost:8000/api/v1/indicators/NVDA | jq '.data[0]'

# Get summary
curl http://localhost:8000/api/v1/summary/MSFT | jq
```

---

## Development

### Running Locally

```bash
# Start all services including API
docker compose up -d --build

# Check API health
curl http://localhost:8000/api/health

# View API logs
docker compose logs -f api

# Access interactive docs
open http://localhost:8000/api/docs
```

### Environment Variables

Configure in `.env` file:

```bash
# API Configuration
API_PORT=8000

# Database (automatically configured in docker-compose)
DB_HOST=db
DB_PORT=5432
DB_NAME=stockdb
DB_USER=postgres
DB_PASSWORD=mysecretpassword
```

---

## Production Considerations

### Security
- [ ] Add API key authentication
- [ ] Implement rate limiting
- [ ] Enable HTTPS/TLS
- [ ] Configure CORS for specific origins
- [ ] Add request validation and sanitization

### Performance
- [ ] Add Redis caching layer
- [ ] Implement connection pooling
- [ ] Add response compression
- [ ] Set up CDN for static assets
- [ ] Database query optimization

### Monitoring
- [ ] Add Prometheus metrics endpoint
- [ ] Implement structured logging
- [ ] Set up error tracking (Sentry)
- [ ] Add request tracing
- [ ] Configure health check alerts

### Deployment
- [ ] Set up CI/CD pipeline
- [ ] Configure auto-scaling
- [ ] Add load balancer
- [ ] Implement blue-green deployment
- [ ] Set up backup and recovery

---

## Support

For issues or questions:
- Check interactive docs at `/api/docs`
- Review error messages in response body
- Check API logs: `docker compose logs api`
- Verify database connectivity: `GET /api/health`

## License

MIT License - Same as parent project




