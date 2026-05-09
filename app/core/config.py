import json
import socket
import uuid
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class GrpcConfig(BaseModel):
    scheduler_url: str = "scheduler:50051"


class NatsConfig(BaseModel):
    url: str = "nats://nats:4222"
    subject_jobs_available: str = "jobs.available"


class WorkerConfig(BaseModel):
    id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}")
    tags: list[str] = ["cpu", "docker"]
    total_cpu: int = 4
    total_mem_mb: int = 8192
    total_gpu: int = 0
    max_slots: int = 2

    @field_validator("tags", mode="before")
    @classmethod
    def parse_tags(cls, v: Any) -> Any:
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
            return [t.strip() for t in v.split(",") if t.strip()]
        return v


class TimingConfig(BaseModel):
    heartbeat_interval_s: int = 5
    pull_interval_s: int = 3
    register_retry_s: int = 5
    register_max_retries: int = 10


class ArtifactsConfig(BaseModel):
    root_path: str = "/artifacts"


class S3Config(BaseModel):
    endpoint_url: str = "http://minio:9000"
    external_endpoint_url: str = ""
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "job-results"
    presign_expiry_s: int = 604800  # 7 days (matches lifecycle policy)

    @model_validator(mode="after")
    def set_external_url(self) -> "S3Config":
        if not self.external_endpoint_url:
            self.external_endpoint_url = self.endpoint_url
        return self


class Settings(BaseSettings):
    grpc: GrpcConfig = GrpcConfig()
    nats: NatsConfig = NatsConfig()
    worker: WorkerConfig = WorkerConfig()
    timing: TimingConfig = TimingConfig()
    artifacts: ArtifactsConfig = ArtifactsConfig()
    s3: S3Config = S3Config()

    model_config = {"env_file": ".env", "env_nested_delimiter": "__", "extra": "ignore"}


settings = Settings()
