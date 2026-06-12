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
    """Résultat brut de l'exécution d'un conteneur Docker."""
    success: bool
    exit_code: int
    logs: str
    duration_s: float
    workdir: str   # Chemin du répertoire temporaire (à nettoyer par l'appelant)


class DockerRunner:
    """Exécute un job dans un conteneur Docker isolé.
    Cette classe est synchrone et doit être appelée via asyncio.to_thread()."""

    def __init__(self) -> None:
        # Connexion au daemon Docker local via le socket Unix monté dans compose.yaml
        self._client = docker.from_env()

    def run(self, job) -> ExecutionResult:
        """Lance le conteneur, attend sa fin et retourne les résultats.
        Le workdir temporaire est créé ici mais c'est l'Executor qui le supprime
        après avoir collecté les sorties."""
        # Répertoire partagé entre le worker et le conteneur via volume bind
        workdir = tempfile.mkdtemp(prefix=f"job-{job.job_id}-")
        started_at = time.time()
        container = None

        try:
            image = job.image
            command = job.command
            env = dict(job.env) if job.env else {}
            device_requests = None

            if job.code:
                # Mode "code" : on écrit le script Python dans le workspace
                # pour qu'il soit accessible via /workspace/run.py dans le conteneur
                code_path = os.path.join(workdir, "run.py")
                with open(code_path, "w") as f:
                    f.write(job.code)

            if job.args:
                # Les arguments sont sérialisés en JSON et passés via variable d'environnement
                # pour éviter les problèmes d'échappement shell avec les args complexes
                args_path = os.path.join(workdir, "args.json")
                with open(args_path, "w") as f:
                    json.dump(job.args, f)
                env.setdefault("JOB_ARGS_PATH", "/workspace/args.json")

            # Mode "code" : si aucune image n'est fournie, on utilise Python par défaut
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

            # Pull systématique pour garantir d'avoir la dernière version de l'image
            logger.info("Pulling image %s for job %s", image, job.job_id)
            self._client.images.pull(image)

            # Conversion des ressources dans les formats attendus par l'API Docker
            mem_limit = f"{job.min_mem_mb}m" if job.min_mem_mb else None
            nano_cpus = job.min_cpu * 1_000_000_000 if job.min_cpu else None  # 1 CPU = 1e9 nano_cpus
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
                # Le workdir local est monté en lecture/écriture : le job peut y écrire ses sorties
                volumes={workdir: {"bind": "/workspace", "mode": "rw"}},
                detach=True,       # On démarre le conteneur en arrière-plan et on attend manuellement
                mem_limit=mem_limit,
                nano_cpus=nano_cpus,
                device_requests=device_requests,
                auto_remove=False,  # On supprime manuellement dans le finally pour pouvoir lire les logs
            )

            # Attente de la fin du conteneur (timeout 10 min)
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
            # On retourne un résultat d'échec plutôt que de lever l'exception
            # pour que l'Executor puisse toujours reporter l'échec au scheduler
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
                    # force=True pour tuer le conteneur s'il tourne encore (timeout dépassé)
                    container.remove(force=True)
                except Exception:
                    pass
