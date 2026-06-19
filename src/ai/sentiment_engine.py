"""
sentiment_engine.py — AI sentiment analyzer using Gemini API with fallback mechanisms.
"""
import os
import re
from typing import Dict, List, Any
from loguru import logger
import requests

class SentimentEngine:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        self.endpoint = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
        
        # Financial sentiment lexicon for robust local fallback
        self.positive_keywords = {
            "upgrade", "bullish", "beat", "outperform", "buy", "growth", 
            "positive", "gain", "profit", "surge", "higher", "rally", "success"
        }
        self.negative_keywords = {
            "downgrade", "bearish", "miss", "underperform", "sell", "decline", 
            "negative", "loss", "drop", "plunge", "lower", "fall", "warning"
        }

    def analyze_headline(self, headline: str) -> Dict[str, Any]:
        """
        Analyze a news headline and return classification, score, and targeted ticker.
        Score ranges from -1.0 (extremely bearish) to +1.0 (extremely bullish).
        """
        if not headline:
            return {"sentiment": "NEUTRAL", "score": 0.0, "tickers": []}

        # Try using Gemini API if configured
        if self.api_key:
            try:
                return self._analyze_with_gemini(headline)
            except Exception as exc:
                logger.warning(f"Gemini API analysis failed, falling back: {exc}")
                
        return self._analyze_locally(headline)

    def _analyze_with_gemini(self, headline: str) -> Dict[str, Any]:
        """Call Gemini API for structured sentiment analysis."""
        headers = {"Content-Type": "application/json"}
        prompt = (
            f"Analyze the following financial headline: \"{headline}\"\n\n"
            "Respond ONLY with a JSON object in this exact schema (no markdown blocks, no code fences):\n"
            "{\n"
            '  "sentiment": "BULLISH" | "BEARISH" | "NEUTRAL",\n'
            '  "score": float between -1.0 and 1.0,\n'
            '  "tickers": [list of stock tickers mentioned, uppercase]\n'
            "}"
        )
        
        payload = {
            "contents": [{
                "parts": [{"text": prompt}]
            }]
        }
        
        url = f"{self.endpoint}?key={self.api_key}"
        res = requests.post(url, headers=headers, json=payload, timeout=8)
        res.raise_for_status()
        
        data = res.json()
        text_resp = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        
        # Clean potential markdown fences
        text_resp = re.sub(r"```json|```", "", text_resp).strip()
        
        import json
        result = json.loads(text_resp)
        return {
            "sentiment": str(result.get("sentiment", "NEUTRAL")).upper(),
            "score": float(result.get("score", 0.0)),
            "tickers": [str(t).upper().strip() for t in result.get("tickers", [])]
        }

    def _analyze_locally(self, headline: str) -> Dict[str, Any]:
        """Rule-based local fallback analyzer using financial sentiment lexicon."""
        words = re.findall(r"\w+", headline.lower())
        
        pos_count = sum(1 for w in words if w in self.positive_keywords)
        neg_count = sum(1 for w in words if w in self.negative_keywords)
        
        total = pos_count + neg_count
        if total == 0:
            score = 0.0
        else:
            score = (pos_count - neg_count) / total

        # Map score to label
        if score > 0.15:
            sentiment = "BULLISH"
        elif score < -0.15:
            sentiment = "BEARISH"
        else:
            sentiment = "NEUTRAL"

        # Simple regex to extract common stock symbols mentioned (e.g. AAPL, TSLA)
        tickers = []
        known_tickers = ["AAPL", "TSLA", "AMZN", "MSFT", "GOOGL", "NVDA", "META", "NFLX"]
        
        company_to_ticker = {
            "apple": "AAPL", "tesla": "TSLA", "amazon": "AMZN", 
            "microsoft": "MSFT", "google": "GOOGL", "alphabet": "GOOGL",
            "nvidia": "NVDA", "meta": "META", "netflix": "NFLX"
        }

        # Check for direct ticker mentions
        for word in re.findall(r"\b[A-Z]{2,5}\b", headline):
            if word in known_tickers and word not in tickers:
                tickers.append(word)
                
        # Check for company name mentions
        for word in words:
            if word in company_to_ticker:
                ticker = company_to_ticker[word]
                if ticker not in tickers:
                    tickers.append(ticker)

        return {
            "sentiment": sentiment,
            "score": round(score, 2),
            "tickers": tickers
        }
