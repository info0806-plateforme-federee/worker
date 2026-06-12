import json
import logging
from pathlib import Path

import boto3
from botocore.config import Config

from app.core.config import S3Config

logger = logging.getLogger(__name__)


class S3Client:
    """Gère l'upload des résultats et artefacts vers MinIO (compatible S3)
    et génère des URLs presignées pour y accéder sans exposer les credentials."""

    def __init__(self, config: S3Config):
        self._config = config
        # Client interne pour les uploads : utilise l'endpoint réseau privé (plus rapide)
        self._client = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            config=Config(signature_version="s3v4"),
        )
        # Client séparé pour les presigned URLs : l'URL générée doit être accessible
        # depuis l'extérieur du réseau Docker, d'où l'endpoint externe distinct
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
        """Sérialise le payload JSON du job et l'uploade dans S3.
        Retourne la clé de l'objet pour générer une presigned URL ensuite."""
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
        """Uploade un fichier artefact dans S3 sous le même préfixe que le résultat JSON.
        Retourne la clé de l'objet."""
        key = f"results/{job_id}/{file_path.name}"
        self._client.upload_file(str(file_path), self._bucket, key)
        logger.info("Uploaded artifact to s3://%s/%s", self._bucket, key)
        return key

    def presign(self, key: str) -> str:
        """Génère une URL GET temporaire (presignée) pour accéder à un objet S3 sans credentials.
        L'URL expire après presign_expiry_s secondes (7 jours par défaut)."""
        url = self._presign_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=self._presign_expiry,
        )
        return url
