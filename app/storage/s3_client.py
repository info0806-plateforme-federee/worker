import json
import logging
from pathlib import Path

import boto3
from botocore.config import Config

from app.core.config import S3Config

logger = logging.getLogger(__name__)


class S3Client:
    """Uploads job results and artifacts to S3 (MinIO) and generates presigned URLs."""

    def __init__(self, config: S3Config):
        self._config = config
        self._client = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            config=Config(signature_version="s3v4"),
        )
        # Separate client for presigned URLs using the externally reachable endpoint
        self._presign_client = boto3.client(
            "s3",
            endpoint_url=config.external_endpoint_url,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            config=Config(signature_version="s3v4"),
        )
        self._bucket = config.bucket
        self._presign_expiry = config.presign_expiry_s

    def upload_result(self, job_id: str, payload: dict) -> str:
        """Upload result_payload as JSON to S3. Returns the object key."""
        key = f"results/{job_id}/result.json"
        body = json.dumps(payload, ensure_ascii=False)
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body.encode(),
            ContentType="application/json",
        )
        logger.info("Uploaded result payload to s3://%s/%s", self._bucket, key)
        return key

    def upload_artifact(self, job_id: str, file_path: Path) -> str:
        """Upload an artifact file to S3. Returns the object key."""
        key = f"results/{job_id}/{file_path.name}"
        self._client.upload_file(str(file_path), self._bucket, key)
        logger.info("Uploaded artifact to s3://%s/%s", self._bucket, key)
        return key

    def presign(self, key: str) -> str:
        """Generate a presigned GET URL for an S3 object."""
        url = self._presign_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=self._presign_expiry,
        )
        return url
