"""
client.py — MinIO client wrapper with retry logic and bucket management.
"""
from __future__ import annotations

import io
import os
from typing import Optional, BinaryIO, Iterator

from minio import Minio
from minio.error import S3Error
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential


def _get_minio_client() -> Minio:
    """Build a MinIO client from environment variables."""
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", ""),
        access_key=os.getenv("MINIO_ACCESS_KEY", "finflow_admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "FinFlow_Secret_2024!"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


class MinIOClient:
    """
    Thin wrapper around the official MinIO Python client.
    Handles bucket initialization, object operations, and listing.
    """

    def __init__(
        self,
        client: Optional[Minio] = None,
        default_bucket: str = "finflow",
    ) -> None:
        self._client = client or _get_minio_client()
        self._default_bucket = default_bucket
        logger.info(f"MinIOClient ready. Default bucket: {default_bucket}")

    def ensure_bucket(self, bucket: str) -> None:
        """Create bucket if it does not exist."""
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)
            logger.info(f"Created MinIO bucket: {bucket}")
        else:
            logger.debug(f"Bucket already exists: {bucket}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
    def put_object(
        self,
        object_path: str,
        data: bytes,
        bucket: Optional[str] = None,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload bytes as an object to MinIO."""
        bucket = bucket or self._default_bucket
        self.ensure_bucket(bucket)
        size = len(data)
        self._client.put_object(
            bucket_name=bucket,
            object_name=object_path,
            data=io.BytesIO(data),
            length=size,
            content_type=content_type,
        )
        logger.debug(f"PUT s3a://{bucket}/{object_path} ({size:,} bytes)")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
    def put_file(
        self,
        object_path: str,
        file_path: str,
        bucket: Optional[str] = None,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload a local file to MinIO."""
        bucket = bucket or self._default_bucket
        self.ensure_bucket(bucket)
        self._client.fput_object(
            bucket_name=bucket,
            object_name=object_path,
            file_path=file_path,
            content_type=content_type,
        )
        logger.info(f"Uploaded file → s3a://{bucket}/{object_path}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
    def get_object(self, object_path: str, bucket: Optional[str] = None) -> bytes:
        """Download an object and return its bytes."""
        bucket = bucket or self._default_bucket
        response = self._client.get_object(bucket_name=bucket, object_name=object_path)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def list_objects(
        self,
        prefix: str = "",
        bucket: Optional[str] = None,
        recursive: bool = True,
    ) -> list[dict]:
        """List objects under a prefix, returning name + size + last_modified."""
        bucket = bucket or self._default_bucket
        objects = self._client.list_objects(
            bucket_name=bucket, prefix=prefix, recursive=recursive
        )
        result = []
        for obj in objects:
            result.append({
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified,
                "etag": obj.etag,
            })
        return result

    def object_exists(self, object_path: str, bucket: Optional[str] = None) -> bool:
        """Check if an object exists."""
        bucket = bucket or self._default_bucket
        try:
            self._client.stat_object(bucket_name=bucket, object_name=object_path)
            return True
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return False
            raise

    def delete_object(self, object_path: str, bucket: Optional[str] = None) -> None:
        """Delete an object from MinIO."""
        bucket = bucket or self._default_bucket
        self._client.remove_object(bucket_name=bucket, object_name=object_path)
        logger.info(f"Deleted s3a://{bucket}/{object_path}")

    def get_presigned_url(
        self,
        object_path: str,
        expires_hours: int = 1,
        bucket: Optional[str] = None,
    ) -> str:
        """Generate a time-limited presigned GET URL."""
        from datetime import timedelta
        bucket = bucket or self._default_bucket
        url = self._client.presigned_get_object(
            bucket_name=bucket,
            object_name=object_path,
            expires=timedelta(hours=expires_hours),
        )
        return url

    def is_healthy(self) -> bool:
        """Quick health check by listing buckets."""
        try:
            list(self._client.list_buckets())
            return True
        except Exception:
            return False
