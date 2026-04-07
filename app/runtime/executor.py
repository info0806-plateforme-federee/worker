import asyncio
import json
import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from app.runtime.docker_runner import DockerRunner, ExecutionResult
from app.runtime.resources import ResourceTracker
from app.utils import safe_rmtree

logger = logging.getLogger(__name__)


@dataclass
class JobSpec:
    job_id: str
    name: str
    job_type: str
    image: str | None = None
    code: str | None = None
    command: str | None = None
    args: dict = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    min_cpu: int = 1
    min_mem_mb: int = 512
    min_gpu: int = 0

    @classmethod
    def from_pull_response(cls, resp) -> "JobSpec":
        env_dict = {}
        if resp.env and resp.env.fields:
            env_dict = {k: str(v) for k, v in dict(resp.env).items()}

        args_dict = {}
        if resp.args and resp.args.fields:
            args_dict = dict(resp.args)

        return cls(
            job_id=resp.job_id,
            name=resp.name or "",
            job_type=resp.job_type or "",
            image=resp.image if resp.HasField("image") else None,
            code=resp.code if resp.HasField("code") else None,
            command=resp.command if resp.HasField("command") else None,
            args=args_dict,
            env=env_dict,
            min_cpu=resp.min_cpu if resp.HasField("min_cpu") else 1,
            min_mem_mb=resp.min_mem_mb if resp.HasField("min_mem_mb") else 512,
            min_gpu=resp.min_gpu if resp.HasField("min_gpu") else 0,
        )


class Executor:
    def __init__(
        self,
        resources: ResourceTracker,
        scheduler_client=None,
        worker_id: str = "",
        artifact_root: str = "/artifacts",
    ):
        self._resources = resources
        self._runner = DockerRunner()
        self._client = scheduler_client
        self._worker_id = worker_id
        self._artifact_root = Path(artifact_root)

    def _load_result_payload(self, workdir: Path, logs: str) -> dict | None:
        result_file = workdir / "result.json"
        if result_file.is_file():
            try:
                payload = json.loads(result_file.read_text())
                if isinstance(payload, dict):
                    return payload
            except (OSError, json.JSONDecodeError):
                logger.warning("Failed to parse result.json for job workspace %s", workdir)

        for line in reversed(logs.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload

        return None

    def _resolve_artifact_source(self, workdir: Path, candidate: str | None) -> Path | None:
        if candidate:
            if candidate.startswith("/workspace/"):
                path = workdir / candidate.removeprefix("/workspace/")
            else:
                source = Path(candidate)
                path = source if source.is_absolute() else workdir / source

            try:
                resolved = path.resolve(strict=True)
            except OSError:
                logger.warning("Artifact candidate %s does not exist", candidate)
                return None

            try:
                resolved.relative_to(workdir.resolve())
            except ValueError:
                logger.warning("Ignoring artifact path outside workspace: %s", candidate)
                return None

            if resolved.is_file():
                return resolved

        artifact_dir = workdir / "artifacts"
        if artifact_dir.is_dir():
            files = sorted(path for path in artifact_dir.rglob("*") if path.is_file())
            if files:
                return files[0]

        files = sorted(
            path for path in workdir.iterdir()
            if path.is_file() and path.name not in {"run.py", "result.json"}
        )
        if len(files) == 1:
            return files[0]

        return None

    def _persist_artifact(self, job_id: str, source: Path | None) -> Path | None:
        if source is None:
            return None

        target_dir = self._artifact_root / job_id
        safe_rmtree(str(target_dir))
        target_dir.mkdir(parents=True, exist_ok=True)

        target = target_dir / source.name
        shutil.copy2(source, target)
        return target

    def _collect_outputs(self, job_id: str, result: ExecutionResult) -> tuple[dict | None, Path | None]:
        workdir = Path(result.workdir)
        result_payload = self._load_result_payload(workdir, result.logs)
        artifact_candidate = None
        if isinstance(result_payload, dict):
            output_file = result_payload.get("output_file")
            artifact_path = result_payload.get("artifact_path")
            artifact_candidate = artifact_path if isinstance(artifact_path, str) else None
            if artifact_candidate is None and isinstance(output_file, str):
                artifact_candidate = output_file

        artifact = self._persist_artifact(
            job_id,
            self._resolve_artifact_source(workdir, artifact_candidate),
        )

        if artifact is not None and isinstance(result_payload, dict):
            if isinstance(result_payload.get("output_file"), str):
                result_payload["output_file"] = artifact.name
            if isinstance(result_payload.get("artifact_path"), str):
                result_payload["artifact_path"] = artifact.name

        return result_payload, artifact

    async def execute(self, job: JobSpec) -> None:
        allocated = self._resources.allocate(
            job.job_id, job.min_cpu, job.min_mem_mb, job.min_gpu
        )
        if not allocated:
            logger.warning("Cannot allocate resources for job %s, skipping", job.job_id)
            return

        started_at = datetime.now(timezone.utc)
        try:
            result = await asyncio.to_thread(self._runner.run, job)
            ended_at = datetime.now(timezone.utc)
            status = "SUCCESS" if result.success else "FAILED"
            result_payload, artifact = self._collect_outputs(job.job_id, result)
            logger.info(
                "Job %s finished: status=%s exit_code=%d duration=%.1fs",
                job.job_id, status, result.exit_code, result.duration_s,
            )
            if result.logs:
                for line in result.logs.splitlines()[:50]:
                    logger.info("  [%s] %s", job.job_id, line)
            if artifact is not None:
                logger.info("Persisted artifact for job %s at %s", job.job_id, artifact)
            safe_rmtree(result.workdir)

            # Report result to scheduler
            if self._client:
                try:
                    await self._client.report_job_result(
                        job_id=job.job_id,
                        worker_id=self._worker_id,
                        success=result.success,
                        logs=result.logs[:10000] if result.logs else "",
                        error_message="" if result.success else (result.logs or "")[:2000],
                        result_payload=result_payload,
                        started_at=started_at.isoformat(),
                        ended_at=ended_at.isoformat(),
                    )
                except Exception:
                    logger.exception("Failed to report result for job %s", job.job_id)
        except Exception:
            logger.exception("Job %s execution failed", job.job_id)
            if self._client:
                try:
                    await self._client.report_job_result(
                        job_id=job.job_id,
                        worker_id=self._worker_id,
                        success=False,
                        error_message="Execution exception",
                        started_at=started_at.isoformat(),
                        ended_at=datetime.now(timezone.utc).isoformat(),
                    )
                except Exception:
                    logger.exception("Failed to report failure for job %s", job.job_id)
        finally:
            self._resources.release(job.job_id)
