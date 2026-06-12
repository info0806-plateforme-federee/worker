import json
import socket
import uuid
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class GrpcConfig(BaseModel):
    """Adresse du scheduler gRPC auquel ce worker se connecte."""
    scheduler_url: str = "node:50051"


class NatsConfig(BaseModel):
    """Connexion NATS pour recevoir les notifications de jobs disponibles."""
    url: str = "nats://node:4222"
    # Sujet sur lequel le scheduler publie quand un nouveau job est prêt
    subject_jobs_available: str = "jobs.available"


class WorkerConfig(BaseModel):
    """Identité et capacité de ce nœud worker."""
    # ID unique par défaut : hostname + suffixe aléatoire pour éviter les collisions
    id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}")
    tags: list[str] = ["cpu", "docker"]
    total_cpu: int = 4
    total_mem_mb: int = 8192
    total_gpu: int = 0
    # Nombre maximum de jobs pouvant tourner simultanément sur ce worker
    max_slots: int = 2

    @field_validator("tags", mode="before")
    @classmethod
    def parse_tags(cls, v: Any) -> Any:
        """Accepte les tags sous forme de liste Python, de JSON ou de chaîne CSV.
        Nécessaire car les variables d'environnement sont toujours des chaînes."""
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
            # Fallback : "cpu,docker" → ["cpu", "docker"]
            return [t.strip() for t in v.split(",") if t.strip()]
        return v


class TimingConfig(BaseModel):
    """Intervalles de temps pour les boucles de fond (en secondes)."""
    heartbeat_interval_s: int = 5
    pull_interval_s: int = 3       # Polling de fallback si NATS est indisponible
    register_retry_s: int = 5      # Délai entre deux tentatives d'enregistrement
    register_max_retries: int = 10


class ArtifactsConfig(BaseModel):
    """Dossier local où les artefacts produits par les jobs sont temporairement stockés."""
    root_path: str = "/artifacts"


class S3Config(BaseModel):
    """Configuration du stockage objet S3 (MinIO) pour les résultats et artefacts."""
    endpoint_url: str = "http://minio:9000"
    # URL accessible depuis l'extérieur du réseau interne, utilisée pour les presigned URLs
    external_endpoint_url: str = ""
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "job-results"
    presign_expiry_s: int = 604800  # 7 jours — aligné sur la politique de lifecycle du bucket

    @model_validator(mode="after")
    def set_external_url(self) -> "S3Config":
        """Si aucune URL externe n'est définie, on réutilise l'URL interne par défaut."""
        if not self.external_endpoint_url:
            self.external_endpoint_url = self.endpoint_url
        return self


class Settings(BaseSettings):
    """Configuration globale du worker, chargée depuis les variables d'environnement.
    Le délimiteur __ permet de surcharger les sous-modèles (ex: WORKER__TOTAL_CPU=8)."""
    grpc: GrpcConfig = GrpcConfig()
    nats: NatsConfig = NatsConfig()
    worker: WorkerConfig = WorkerConfig()
    timing: TimingConfig = TimingConfig()
    artifacts: ArtifactsConfig = ArtifactsConfig()
    s3: S3Config = S3Config()

    model_config = {"env_file": ".env", "env_nested_delimiter": "__", "extra": "ignore"}


# Instance singleton importée partout dans l'application
settings = Settings()
