"""
test_api.py — Integration tests for the FastAPI serving layer.
Uses FastAPI TestClient — no real service dependencies required.
"""
import pytest
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, "src")

from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    # Mock services to prevent connection blocks during testing
    with patch("psycopg2.connect") as mock_pg_connect, \
         patch("minio.Minio") as mock_minio_class:
        
        # Mock MinIO list_buckets
        mock_minio_inst = MagicMock()
        mock_minio_inst.list_buckets.return_value = []
        mock_minio_class.return_value = mock_minio_inst
        
        # Mock Postgres connection
        mock_pg_conn = MagicMock()
        mock_pg_connect.return_value = mock_pg_conn
        
        from serving.api.main import app
        yield TestClient(app)



class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_body_has_required_fields(self, client):
        data = client.get("/health").json()
        assert "status" in data
        assert "timestamp" in data
        assert "services" in data
        assert data["status"] in ("healthy", "degraded")

    def test_health_services_keys(self, client):
        data = client.get("/health").json()
        # Should have service keys
        assert isinstance(data["services"], dict)


class TestTickersEndpoint:
    def test_list_tickers(self, client):
        resp = client.get("/tickers")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_tickers_have_required_fields(self, client):
        data = client.get("/tickers").json()
        for ticker in data:
            assert "ticker" in ticker
            assert "name" in ticker
            assert "exchange" in ticker

    def test_ticker_symbols_are_uppercase(self, client):
        data = client.get("/tickers").json()
        for ticker in data:
            assert ticker["ticker"] == ticker["ticker"].upper()


class TestLatestPriceEndpoint:
    def test_valid_ticker(self, client):
        resp = client.get("/tickers/AAPL/latest")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ticker"] == "AAPL"
        assert "close" in data
        assert data["close"] > 0

    def test_lowercase_ticker_works(self, client):
        resp = client.get("/tickers/aapl/latest")
        assert resp.status_code == 200
        assert resp.json()["ticker"] == "AAPL"

    def test_invalid_ticker_404(self, client):
        resp = client.get("/tickers/INVALID_XYZ/latest")
        assert resp.status_code == 404

    def test_response_has_ohlcv(self, client):
        data = client.get("/tickers/TSLA/latest").json()
        for field in ["open", "high", "low", "close", "volume"]:
            assert field in data
            assert data[field] is not None


class TestHistoryEndpoint:
    def test_default_history(self, client):
        resp = client.get("/tickers/AAPL/history")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_history_limit(self, client):
        resp = client.get("/tickers/AAPL/history?limit=5")
        assert resp.status_code == 200
        assert len(resp.json()) <= 5

    def test_history_records_have_required_fields(self, client):
        data = client.get("/tickers/AAPL/history?limit=3").json()
        for record in data:
            for field in ["ticker", "date", "open", "high", "low", "close", "volume"]:
                assert field in record

    def test_invalid_ticker_404(self, client):
        resp = client.get("/tickers/FAKE123/history")
        assert resp.status_code == 404

    def test_limit_max_365(self, client):
        resp = client.get("/tickers/AAPL/history?limit=500")
        assert resp.status_code == 422  # Validation error


class TestMarketSummaryEndpoint:
    def test_market_summary_returns_200(self, client):
        resp = client.get("/market/summary")
        assert resp.status_code == 200

    def test_market_summary_structure(self, client):
        data = resp = client.get("/market/summary").json()
        assert "top_gainers" in data
        assert "top_losers" in data
        assert "most_active" in data
        assert "total_tickers" in data

    def test_total_tickers_count(self, client):
        data = client.get("/market/summary").json()
        assert data["total_tickers"] > 0


class TestPortfolioEndpoint:
    def test_valid_user_portfolio(self, client):
        resp = client.get("/users/user_12345/portfolio")
        assert resp.status_code == 200
        data = resp.json()
        assert data["user_id"] == "user_12345"
        assert "holdings" in data
        assert "total_value_usd" in data

    def test_invalid_user_id_400(self, client):
        resp = client.get("/users/ab/portfolio")
        assert resp.status_code == 400

    def test_portfolio_total_value_positive(self, client):
        data = client.get("/users/user_12345/portfolio").json()
        assert data["total_value_usd"] > 0


class TestStaticServing:
    def test_root_serves_html(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "FinFlow" in resp.text

    def test_static_asset_style_css(self, client):
        resp = client.get("/static/style.css")
        assert resp.status_code == 200
        assert "text/css" in resp.headers["content-type"] or "application/octet-stream" in resp.headers["content-type"]

