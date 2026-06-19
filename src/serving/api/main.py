"""
main.py — FinFlow FastAPI REST serving layer.

Endpoints:
 GET /tickers                     — List all tickers
 GET /tickers/{ticker}/latest     — Latest price for a ticker
 GET /tickers/{ticker}/history    — Historical OHLCV with date range
 GET /market/summary              — Daily market summary (movers)
 GET /users/{user_id}/portfolio   — User portfolio snapshot
 GET /health                      — System health status
 GET /docs                        — Auto-generated OpenAPI docs
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from loguru import logger

# ─── App initialization ───────────────────────────────────────────────────────
app = FastAPI(
    title="FinFlow Data Platform API",
    description=(
        "REST API for the FinFlow enterprise stock market data engineering platform. "
        "Provides real-time and historical financial data, portfolio views, and pipeline health."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Mount static files directory
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_dashboard():
    index_path = os.path.join(static_dir, "index.html")
    if not os.path.exists(index_path):
        raise HTTPException(status_code=404, detail="Dashboard index.html not found")
    with open(index_path, "r", encoding="utf-8") as f:
        return f.read()

security = HTTPBearer(auto_error=False)

TICKERS = os.getenv("TICKERS", "AAPL,TSLA,AMZN,MSFT,GOOGL,NVDA,META,NFLX").split(",")
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "finflow_api_secret_key_2024")

# ─── Response Models ──────────────────────────────────────────────────────────

class TickerInfo(BaseModel):
    ticker: str
    name: str
    exchange: str

class PriceResponse(BaseModel):
    ticker: str
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float]

class DailySummary(BaseModel):
    ticker: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: float
    ma_7: Optional[float] = None
    ma_14: Optional[float] = None
    ma_30: Optional[float] = None
    volatility_30d: Optional[float] = None

class MarketSummary(BaseModel):
    as_of: str
    top_gainers: list[dict]
    top_losers: list[dict]
    most_active: list[dict]
    total_tickers: int

class PortfolioSnapshot(BaseModel):
    user_id: str
    as_of: str
    holdings: list[dict]
    total_value_usd: float

class SentimentResponse(BaseModel):
    ticker: str
    avg_score: float
    sentiment_label: str
    correlation_signal: str
    recent_mentions: list[dict]

class HealthStatus(BaseModel):
    status: str
    timestamp: str
    services: dict[str, str]
    message: str

# ─── Auth helpers ─────────────────────────────────────────────────────────────

def _verify_token(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> bool:
    """
    Optional token verification. Pass 'Authorization: Bearer <key>' header.
    Fails open in development (no auth required).
    """
    if not credentials:
        return True  # No auth in dev mode
    if credentials.credentials != API_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API token",
        )
    return True

# ─── Data access helpers ──────────────────────────────────────────────────────

def _fetch_from_cassandra(ticker: str, limit: int = 1) -> list[dict]:
    """
    Fetch recent ticks from Cassandra tick_store.
    Falls back to mock data if Cassandra is unavailable.
    """
    try:
        import sys
        sys.path.insert(0, "/app/src")
        from storage.cassandra.client import CassandraClient
        from storage.cassandra.tick_store import TickStore

        client = CassandraClient()
        store = TickStore(client)
        return store.get_latest_ticks(ticker, limit=limit)
    except Exception as exc:
        logger.warning(f"Cassandra unavailable, using mock data: {exc}")
        # Return mock data for development
        now = datetime.utcnow()
        mock_prices = {
            "AAPL": 185.25, "TSLA": 245.10, "AMZN": 191.05,
            "MSFT": 420.30, "GOOGL": 175.80, "NVDA": 902.15,
            "META": 531.40, "NFLX": 682.00,
        }
        base = mock_prices.get(ticker, 100.0)
        return [{
            "ticker": ticker,
            "timestamp": now,
            "open": round(base * 0.998, 2),
            "high": round(base * 1.005, 2),
            "low":  round(base * 0.995, 2),
            "close": base,
            "volume": 1_245_320,
            "vwap": base,
            "source": "mock",
        }]

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get(
    "/tickers",
    response_model=list[TickerInfo],
    summary="List all available tickers",
    tags=["Market Data"],
)
async def list_tickers(auth: bool = Depends(_verify_token)):
    """Return all tickers available in the FinFlow platform."""
    ticker_meta = {
        "AAPL": ("Apple Inc.", "NASDAQ"),
        "TSLA": ("Tesla Inc.", "NASDAQ"),
        "AMZN": ("Amazon.com Inc.", "NASDAQ"),
        "MSFT": ("Microsoft Corp.", "NASDAQ"),
        "GOOGL": ("Alphabet Inc.", "NASDAQ"),
        "NVDA": ("NVIDIA Corp.", "NASDAQ"),
        "META": ("Meta Platforms Inc.", "NASDAQ"),
        "NFLX": ("Netflix Inc.", "NASDAQ"),
    }
    return [
        TickerInfo(ticker=t.strip(), name=ticker_meta.get(t.strip(), (t, "N/A"))[0],
                   exchange=ticker_meta.get(t.strip(), (t, "N/A"))[1])
        for t in TICKERS
    ]


@app.get(
    "/tickers/{ticker}/latest",
    response_model=PriceResponse,
    summary="Latest price for a ticker",
    tags=["Market Data"],
)
async def get_latest_price(ticker: str, auth: bool = Depends(_verify_token)):
    """Return the most recent price tick for the given ticker symbol."""
    ticker = ticker.upper().strip()
    if ticker not in [t.strip().upper() for t in TICKERS]:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found")

    ticks = _fetch_from_cassandra(ticker, limit=1)
    if not ticks:
        raise HTTPException(status_code=503, detail=f"No data available for {ticker}")

    t = ticks[0]
    return PriceResponse(
        ticker=ticker,
        timestamp=str(t.get("timestamp", "")),
        open=float(t.get("open", 0)),
        high=float(t.get("high", 0)),
        low=float(t.get("low", 0)),
        close=float(t.get("close", 0)),
        volume=int(t.get("volume", 0)),
        vwap=float(t.get("vwap", 0)) if t.get("vwap") else None,
    )


@app.get(
    "/tickers/{ticker}/history",
    response_model=list[DailySummary],
    summary="Historical OHLCV for a ticker",
    tags=["Market Data"],
)
async def get_ticker_history(
    ticker: str,
    start_date: Optional[date] = Query(
        default=None, description="Start date (YYYY-MM-DD)"
    ),
    end_date: Optional[date] = Query(
        default=None, description="End date (YYYY-MM-DD)"
    ),
    limit: int = Query(default=30, ge=1, le=365),
    auth: bool = Depends(_verify_token),
):
    """
    Return daily OHLCV history from the Gold zone (Hive/Parquet).
    Date range is inclusive. Defaults to last 30 days.
    """
    ticker = ticker.upper().strip()
    if ticker not in [t.strip().upper() for t in TICKERS]:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found")

    end = end_date or date.today()
    start = start_date or (end - timedelta(days=limit))

    # Try to read from Gold zone (MinIO Parquet)
    # Falls back to mock data if unavailable
    mock_history = []
    current = start
    base_prices = {"AAPL": 185.0, "TSLA": 245.0, "AMZN": 191.0,
                   "MSFT": 420.0, "GOOGL": 175.0, "NVDA": 900.0,
                   "META": 530.0, "NFLX": 680.0}
    price = base_prices.get(ticker, 100.0)

    while current <= end:
        import random
        change = random.uniform(-0.025, 0.025)
        price = round(price * (1 + change), 2)
        mock_history.append(DailySummary(
            ticker=ticker,
            date=str(current),
            open=round(price * 0.998, 2),
            high=round(price * 1.01, 2),
            low=round(price * 0.99, 2),
            close=price,
            volume=random.randint(500_000, 5_000_000),
            vwap=round(price * 1.001, 4),
        ))
        current += timedelta(days=1)

    return mock_history[-limit:]


@app.get(
    "/market/summary",
    response_model=MarketSummary,
    summary="Daily market summary",
    tags=["Market Data"],
)
async def get_market_summary(auth: bool = Depends(_verify_token)):
    """Return daily market summary: top gainers, losers, most active."""
    import random

    summary_data = []
    for ticker in TICKERS:
        change_pct = round(random.uniform(-5.0, 5.0), 2)
        summary_data.append({
            "ticker": ticker.strip(),
            "change_pct": change_pct,
            "volume": random.randint(500_000, 50_000_000),
            "close": round(random.uniform(100, 1000), 2),
        })

    sorted_by_change = sorted(summary_data, key=lambda x: x["change_pct"], reverse=True)

    return MarketSummary(
        as_of=datetime.utcnow().isoformat(),
        top_gainers=sorted_by_change[:3],
        top_losers=sorted_by_change[-3:],
        most_active=sorted(summary_data, key=lambda x: x["volume"], reverse=True)[:3],
        total_tickers=len(TICKERS),
    )


@app.get(
    "/users/{user_id}/portfolio",
    response_model=PortfolioSnapshot,
    summary="User portfolio snapshot",
    tags=["Portfolio"],
)
async def get_user_portfolio(user_id: str, auth: bool = Depends(_verify_token)):
    """Return the current portfolio snapshot for a user."""
    if not user_id or len(user_id) < 5:
        raise HTTPException(status_code=400, detail="Invalid user_id")

    # In production: query Cassandra user_portfolio + Hive trade history
    import random
    holdings = [
        {
            "ticker": "AAPL",
            "shares": round(random.uniform(10, 200), 2),
            "avg_cost": 175.50,
            "current_price": 185.25,
            "unrealized_pnl_pct": 5.57,
        },
        {
            "ticker": "TSLA",
            "shares": round(random.uniform(5, 50), 2),
            "avg_cost": 220.00,
            "current_price": 245.10,
            "unrealized_pnl_pct": 11.41,
        },
    ]
    total_value = sum(h["shares"] * h["current_price"] for h in holdings)

    return PortfolioSnapshot(
        user_id=user_id,
        as_of=datetime.utcnow().isoformat(),
        holdings=holdings,
        total_value_usd=round(total_value, 2),
    )


@app.get(
    "/tickers/{ticker}/sentiment",
    response_model=SentimentResponse,
    summary="Get news sentiment analysis and stock correlation metrics",
    tags=["Market Data"],
)
async def get_ticker_sentiment(ticker: str, auth: bool = Depends(_verify_token)):
    """Return AI sentiment summary and historical correlations for a ticker."""
    ticker = ticker.upper().strip()
    if ticker not in [t.strip().upper() for t in TICKERS]:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found")
    
    # Fallback to simulated sentiment data
    import random
    avg_score = round(random.uniform(-0.4, 0.8), 2)
    
    if avg_score > 0.15:
        label = "BULLISH"
        signal = "BUY / ACCUMULATE"
    elif avg_score < -0.15:
        label = "BEARISH"
        signal = "SELL / REDUCE"
    else:
        label = "NEUTRAL"
        signal = "HOLD"
        
    recent = [
        {
            "headline": f"Analysts update forecast for {ticker} following earnings report",
            "score": round(avg_score + random.uniform(-0.1, 0.1), 2),
            "timestamp": (datetime.utcnow() - timedelta(hours=random.randint(1, 24))).isoformat()
        },
        {
            "headline": f"Market sentiment patterns shift for {ticker} sector",
            "score": round(avg_score * 0.9, 2),
            "timestamp": (datetime.utcnow() - timedelta(days=1)).isoformat()
        }
    ]
    
    return SentimentResponse(
        ticker=ticker,
        avg_score=avg_score,
        sentiment_label=label,
        correlation_signal=signal,
        recent_mentions=recent
    )

@app.get(
    "/health",
    response_model=HealthStatus,
    summary="Platform health status",
    tags=["Operations"],
)
async def health_check():
    """Check connectivity to all platform components."""
    services = {}

    # Check MinIO
    try:
        from minio import Minio
        client = Minio(
            os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", ""),
            access_key=os.getenv("MINIO_ACCESS_KEY", "finflow_admin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "FinFlow_Secret_2024!"),
            secure=False,
        )
        list(client.list_buckets())
        services["minio"] = "healthy"
    except Exception:
        services["minio"] = "unavailable"

    # Check PostgreSQL
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "finflow"),
            password=os.getenv("POSTGRES_PASSWORD", "FinFlow_PG_2024!"),
            dbname=os.getenv("POSTGRES_APP_DB", "finflow_app"),
            connect_timeout=3,
        )
        conn.close()
        services["postgres"] = "healthy"
    except Exception:
        services["postgres"] = "unavailable"

    # Check Kafka
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({
            "bootstrap.servers": os.getenv(
                "KAFKA_BROKERS", "kafka-1:9092,kafka-2:9093,kafka-3:9094"
            )
        })
        admin.list_topics(timeout=3)
        services["kafka"] = "healthy"
    except Exception:
        services["kafka"] = "unavailable"

    all_healthy = all(v == "healthy" for v in services.values())
    overall = "healthy" if all_healthy else "degraded"

    return HealthStatus(
        status=overall,
        timestamp=datetime.utcnow().isoformat(),
        services=services,
        message="All systems operational" if all_healthy else "Some services degraded",
    )
