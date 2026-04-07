import json
import os
import tempfile
import time
import logging
from dataclasses import dataclass, field

import docker
from docker.types import DeviceRequest

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    success: bool
    exit_code: int
    logs: str
    duration_s: float
    workdir: str


class DockerRunner:
    def __init__(self) -> None:
        self._client = docker.from_env()

    def run(self, job) -> ExecutionResult:
        workdir = tempfile.mkdtemp(prefix=f"job-{job.job_id}-")
        started_at = time.time()
        container = None

        try:
            image = job.image
            command = job.command
            env = dict(job.env) if job.env else {}
            device_requests = None

            if job.code:
                code_path = os.path.join(workdir, "run.py")
                with open(code_path, "w") as f:
                    f.write(job.code)

            if job.args:
                args_path = os.path.join(workdir, "args.json")
                with open(args_path, "w") as f:
                    json.dump(job.args, f)
                env.setdefault("JOB_ARGS_PATH", "/workspace/args.json")

            # Code mode: write code to file, run in python container
            if not image and job.code:
                image = "python:3.14-slim"
            if job.code and not command:
                command = "python /workspace/run.py"

            if not image:
                return ExecutionResult(
                    success=False,
                    exit_code=-1,
                    logs="No image or code provided",
                    duration_s=time.time() - started_at,
                    workdir=workdir,
                )

            logger.info("Pulling image %s for job %s", image, job.job_id)
            self._client.images.pull(image)

            mem_limit = f"{job.min_mem_mb}m" if job.min_mem_mb else None
            nano_cpus = job.min_cpu * 1_000_000_000 if job.min_cpu else None
            if job.min_gpu:
                device_requests = [
                    DeviceRequest(count=job.min_gpu, capabilities=[["gpu"]]),
                ]
                env.setdefault("NVIDIA_VISIBLE_DEVICES", "all")

            logger.info("Starting container for job %s", job.job_id)
            container = self._client.containers.run(
                image=image,
                command=command,
                environment=env,
                working_dir="/workspace",
                volumes={workdir: {"bind": "/workspace", "mode": "rw"}},
                detach=True,
                mem_limit=mem_limit,
                nano_cpus=nano_cpus,
                device_requests=device_requests,
                auto_remove=False,
            )

            result = container.wait(timeout=600)
            logs = container.logs(stdout=True, stderr=True).decode(errors="replace")
            exit_code = int(result.get("StatusCode", 1))
            duration = time.time() - started_at

            return ExecutionResult(
                success=exit_code == 0,
                exit_code=exit_code,
                logs=logs,
                duration_s=duration,
                workdir=workdir,
            )

        except Exception as exc:
            return ExecutionResult(
                success=False,
                exit_code=-1,
                logs=str(exc),
                duration_s=time.time() - started_at,
                workdir=workdir,
            )
        finally:
            if container is not None:
                try:
                    container.remove(force=True)
                except Exception:
                    pass
