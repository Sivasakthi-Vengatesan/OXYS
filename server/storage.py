"""
OXYS Object Storage & MinIO Client
Manages:
  - Poison-pill Parquet / JSON quarantine isolation (s3://oxys-quarantine/)
  - Checkpoint storage snapshots (s3://oxys-checkpoints/)
  - Primary lakehouse landing objects (s3://oxys-lakehouse/)
"""
import os
import io
import json
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger("oxys.server.storage")


class MinIOStorageManager:
    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: bool = False
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.secure = secure or os.getenv("MINIO_SECURE", "false").lower() == "true"
        
        self.quarantine_bucket = os.getenv("MINIO_QUARANTINE_BUCKET", "oxys-quarantine")
        self.checkpoint_bucket = os.getenv("MINIO_CHECKPOINT_BUCKET", "oxys-checkpoints")
        self.lakehouse_bucket = os.getenv("MINIO_LAKEHOUSE_BUCKET", "oxys-lakehouse")

        self.client = None
        self._connected = False
        
        import tempfile
        is_serverless = bool(
            os.getenv("VERCEL") or 
            os.getenv("AWS_LAMBDA_FUNCTION_NAME") or 
            os.getenv("SERVERLESS") or
            not os.access(os.path.dirname(__file__), os.W_OK)
        )
        if is_serverless:
            self._local_storage_dir = os.path.join(tempfile.gettempdir(), ".oxys_storage")
        else:
            try:
                candidate = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".oxys_storage"))
                os.makedirs(candidate, exist_ok=True)
                self._local_storage_dir = candidate
            except (OSError, PermissionError):
                self._local_storage_dir = os.path.join(tempfile.gettempdir(), ".oxys_storage")
        
        try:
            os.makedirs(self._local_storage_dir, exist_ok=True)
        except Exception:
            pass

        self.connect()

    def connect(self) -> bool:
        try:
            import socket
            host_part = self.endpoint.split(":")[0]
            port_part = int(self.endpoint.split(":")[1]) if ":" in self.endpoint else 9000
            with socket.create_connection((host_part, port_part), timeout=0.1):
                pass
            from minio import Minio
            self.client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            # Ensure buckets exist
            for b in [self.quarantine_bucket, self.checkpoint_bucket, self.lakehouse_bucket]:
                if not self.client.bucket_exists(b):
                    self.client.make_bucket(b)
            self._connected = True
            logger.info(f"MinIO storage connected at {self.endpoint}")
            return True
        except Exception as e:
            logger.info(f"MinIO storage at {self.endpoint} not reachable. Using local file storage vault.")
            self._connected = False
            return False

    @property
    def is_connected(self) -> bool:
        return self._connected

    def save_quarantine_batch(self, stream_name: str, batch_id: str, data: Dict[str, Any]) -> str:
        object_name = f"{stream_name}/batch_{batch_id}.json"
        content_bytes = json.dumps(data, indent=2).encode("utf-8")

        if self._connected and self.client:
            try:
                self.client.put_object(
                    bucket_name=self.quarantine_bucket,
                    object_name=object_name,
                    data=io.BytesIO(content_bytes),
                    length=len(content_bytes),
                    content_type="application/json"
                )
                return f"s3://{self.quarantine_bucket}/{object_name}"
            except Exception as e:
                logger.error(f"Failed to write to MinIO ({e}). Falling back to local storage.")
                self._connected = False

        # Local storage fallback
        try:
            local_path = os.path.join(self._local_storage_dir, self.quarantine_bucket, stream_name)
            os.makedirs(local_path, exist_ok=True)
            file_path = os.path.join(local_path, f"batch_{batch_id}.json")
            with open(file_path, "wb") as f:
                f.write(content_bytes)
        except Exception as e:
            logger.warning(f"Could not persist local quarantine batch: {e}")
        return f"s3://{self.quarantine_bucket}/{object_name}"

    def save_checkpoint(self, stream_name: str, checkpoint_id: str, metadata: Dict[str, Any]) -> str:
        object_name = f"prod/{stream_name}/{checkpoint_id}.chk"
        content_bytes = json.dumps(metadata, indent=2).encode("utf-8")

        if self._connected and self.client:
            try:
                self.client.put_object(
                    bucket_name=self.checkpoint_bucket,
                    object_name=object_name,
                    data=io.BytesIO(content_bytes),
                    length=len(content_bytes),
                    content_type="application/json"
                )
                return f"s3://{self.checkpoint_bucket}/{object_name}"
            except Exception:
                self._connected = False

        try:
            local_path = os.path.join(self._local_storage_dir, self.checkpoint_bucket, "prod", stream_name)
            os.makedirs(local_path, exist_ok=True)
            file_path = os.path.join(local_path, f"{checkpoint_id}.chk")
            with open(file_path, "wb") as f:
                f.write(content_bytes)
        except Exception as e:
            logger.warning(f"Could not persist local checkpoint: {e}")
        return f"s3://{self.checkpoint_bucket}/{object_name}"


storage = MinIOStorageManager()
