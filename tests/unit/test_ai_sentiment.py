"""
test_ai_sentiment.py — Unit and integration tests for AI Sentiment Engine and API.
"""
import pytest
import sys
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

sys.path.insert(0, "src")

from ai.sentiment_engine import SentimentEngine
from transformation.spark.sentiment_aggregator import SentimentAggregator


class TestSentimentEngine:
    def setup_method(self):
        # Instantiate without API key to force local lexicon-based mode
        self.engine = SentimentEngine(api_key="mock_key")

    def test_local_bullish_headline(self):
        headline = "Apple reports strong earnings beat and outlook upgrade"
        res = self.engine._analyze_locally(headline)
        assert res["sentiment"] == "BULLISH"
        assert res["score"] > 0.0
        assert "AAPL" in res["tickers"]

    def test_local_bearish_headline(self):
        headline = "Tesla stock declines following severe production warning and downgrade"
        res = self.engine._analyze_locally(headline)
        assert res["sentiment"] == "BEARISH"
        assert res["score"] < 0.0
        assert "TSLA" in res["tickers"]

    def test_local_neutral_headline(self):
        headline = "Amazon stock price stays flat ahead of retail reports"
        res = self.engine._analyze_locally(headline)
        assert res["sentiment"] == "NEUTRAL"
        assert res["score"] == 0.0

    @patch("requests.post")
    def test_gemini_api_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "candidates": [{
                "content": {
                    "parts": [{
                        "text": '{"sentiment": "BULLISH", "score": 0.85, "tickers": ["AAPL", "NVDA"]}'
                    }]
                }
            }]
        }
        mock_post.return_value = mock_response

        # Enable Gemini analyzer
        engine = SentimentEngine(api_key="real_key_simulated")
        res = engine.analyze_headline("Apple and Nvidia rally strongly on AI hardware demand")
        
        assert res["sentiment"] == "BULLISH"
        assert res["score"] == 0.85
        assert "AAPL" in res["tickers"]
        assert "NVDA" in res["tickers"]


class TestSentimentAggregator:
    def test_aggregator_path_fallback(self):
        agg = SentimentAggregator(bronze_path="s3a://finflow/bronze/sentiment/", gold_path="s3a://finflow/gold/")
        out_dir = agg.run_aggregation()
        assert "sentiment_correlations/" in out_dir


class TestSentimentApiIntegration:
    @pytest.fixture(scope="class")
    def client(self):
        with patch("psycopg2.connect") as mock_connect, \
             patch("minio.Minio") as mock_minio:
            mock_minio_inst = MagicMock()
            mock_minio_inst.list_buckets.return_value = []
            mock_minio.return_value = mock_minio_inst
            mock_connect.return_value = MagicMock()
            
            from serving.api.main import app
            return TestClient(app)

    def test_sentiment_endpoint_valid(self, client):
        resp = client.get("/tickers/AAPL/sentiment")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ticker"] == "AAPL"
        assert "avg_score" in data
        assert "sentiment_label" in data
        assert "correlation_signal" in data
        assert len(data["recent_mentions"]) > 0

    def test_sentiment_endpoint_invalid_ticker(self, client):
        resp = client.get("/tickers/FAKE_SYM/sentiment")
        assert resp.status_code == 404
