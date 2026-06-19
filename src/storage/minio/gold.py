"""
gold.py — Gold zone: business-level aggregations ready for the warehouse.

Aggregations computed here:
 - Daily OHLCV summary per ticker
 - 7/14/30-day moving averages
 - Daily VWAP
 - Daily volatility (std dev of daily returns)
"""
from __future__ import annotations

import io
import os
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from loguru import logger

from storage.minio.client import MinIOClient


GOLD_PREFIX = os.getenv("MINIO_GOLD_PREFIX", "gold")
BUCKET = os.getenv("MINIO_BUCKET", "finflow")


def _build_gold_path(dataset: str, ticker: str, ts: datetime) -> str:
    return (
        f"{GOLD_PREFIX}/{dataset}/"
        f"ticker={ticker.upper()}/"
        f"year={ts.year}/month={ts.month:02d}/"
    )


class GoldWriter:
    """Computes aggregations and writes them to the Gold zone as Parquet."""

    def __init__(self, client: Optional[MinIOClient] = None) -> None:
        self._client = client or MinIOClient()
        self._client.ensure_bucket(BUCKET)

    def compute_daily_ohlcv(self, ticks: list[dict]) -> dict:
        """
        Compute daily OHLCV summary from a list of Silver-cleaned ticks.

        Returns: { open, high, low, close, volume, vwap, date }
        """
        if not ticks:
            raise ValueError("Cannot compute OHLCV from empty tick list")

        prices = [float(t["close"]) for t in ticks]
        highs = [float(t["high"]) for t in ticks]
        lows = [float(t["low"]) for t in ticks]
        volumes = [int(t["volume"]) for t in ticks]

        # VWAP = sum(price * volume) / sum(volume)
        pv_sum = sum(p * v for p, v in zip(prices, volumes))
        total_volume = sum(volumes)
        vwap = round(pv_sum / total_volume, 4) if total_volume > 0 else prices[-1]

        return {
            "open": ticks[0]["open"],
            "high": max(highs),
            "low": min(lows),
            "close": prices[-1],
            "volume": total_volume,
            "vwap": vwap,
            "tick_count": len(ticks),
        }

    def compute_moving_averages(
        self,
        daily_closes: list[float],
        windows: list[int] = [7, 14, 30],
    ) -> dict[str, Optional[float]]:
        """
        Compute simple moving averages for each window.

        Returns: { "ma_7": float|None, "ma_14": float|None, "ma_30": float|None }
        """
        result = {}
        for w in windows:
            if len(daily_closes) >= w:
                result[f"ma_{w}"] = round(
                    float(np.mean(daily_closes[-w:])), 4
                )
            else:
                result[f"ma_{w}"] = None
        return result

    def compute_volatility(
        self,
        daily_closes: list[float],
        window: int = 30,
    ) -> Optional[float]:
        """
        Annualized volatility = std(daily_returns) * sqrt(252).

        Returns None if insufficient data.
        """
        if len(daily_closes) < 2:
            return None
        closes = daily_closes[-window:]
        returns = np.diff(np.log(closes))
        std_daily = float(np.std(returns, ddof=1))
        return round(std_daily * (252 ** 0.5), 6)  # annualized

    def write_daily_summary(
        self,
        ticker: str,
        date_str: str,  # YYYY-MM-DD
        ohlcv: dict,
        moving_avgs: dict,
        volatility: Optional[float],
        batch_id: str,
    ) -> str:
        """Write a daily Gold summary to MinIO as Parquet."""
        ts = datetime.now(timezone.utc)
        row = {
            "ticker": ticker,
            "date": date_str,
            **ohlcv,
            **moving_avgs,
            "volatility_annualized": volatility,
            "computed_at": ts.isoformat(),
        }

        schema = pa.schema([
            pa.field("ticker", pa.string()),
            pa.field("date", pa.string()),
            pa.field("open", pa.float64()),
            pa.field("high", pa.float64()),
            pa.field("low", pa.float64()),
            pa.field("close", pa.float64()),
            pa.field("volume", pa.int64()),
            pa.field("vwap", pa.float64()),
            pa.field("tick_count", pa.int64()),
            pa.field("ma_7", pa.float64()),
            pa.field("ma_14", pa.float64()),
            pa.field("ma_30", pa.float64()),
            pa.field("volatility_annualized", pa.float64()),
            pa.field("computed_at", pa.string()),
        ])

        table = pa.Table.from_pylist([row], schema=schema)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        prefix = _build_gold_path(dataset="daily-summary", ticker=ticker, ts=ts)
        object_path = f"{prefix}{date_str}_{batch_id}.parquet"

        self._client.put_object(
            object_path=object_path,
            data=buf.read(),
            content_type="application/octet-stream",
        )

        logger.info(f"Gold daily summary written → s3a://{BUCKET}/{object_path}")
        return f"s3a://{BUCKET}/{object_path}"
