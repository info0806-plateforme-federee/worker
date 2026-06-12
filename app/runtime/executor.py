import asyncio
import json
import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from app.runtime.docker_runner import DockerRunner, ExecutionResult
from app.runtime.resources import ResourceTracker
from app.storage.s3_client import S3Client
from app.utils import safe_rmtree

logger = logging.getLogger(__name__)


@dataclass
class JobSpec:
    """Description complète d'un job reçu du scheduler, prête à être exécutée."""
    job_id: str
    name: str
    job_type: str
    image: str | None = None    # Image Docker à utiliser
    code: str | None = None     # Code Python à exécuter (alternative à image+command)
    command: str | None = None  # Commande à lancer dans le conteneur
    args: dict = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    min_cpu: int = 1
    min_mem_mb: int = 512
    min_gpu: int = 0

    @classmethod
    def from_pull_response(cls, resp) -> "JobSpec":
        """Convertit la réponse protobuf PullJobResponse en JobSpec Python.
        Les champs optionnels protobuf nécessitent HasField() pour distinguer
        "non renseigné" de "vide" (chaîne vide ou 0)."""
        env_dict = {}
        if resp.env and resp.env.fields:
            # Les valeurs protobuf Struct sont des Value objects, on les force en str
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
    """Orchestre l'exécution d'un job de bout en bout :
    allocation de ressources -> exécution Docker -> collecte des sorties -> upload S3 -> rapport au scheduler."""

    def __init__(
        self,
        resources: ResourceTracker,
        scheduler_client=None,
        worker_id: str = "",
        artifact_root: str = "/artifacts",
        s3_client: S3Client | None = None,
    ):
        self._resources = resources
        self._runner = DockerRunner()
        self._client = scheduler_client
        self._worker_id = worker_id
        self._artifact_root = Path(artifact_root)
        self._s3 = s3_client

    def _load_result_payload(self, workdir: Path, logs: str) -> dict | None:
        """Tente de lire le résultat JSON produit par le job, avec deux stratégies :
        1. Fichier result.json dans le workspace (méthode préférentielle).
        2. Dernière ligne JSON valide des logs (fallback pour les jobs qui affichent leur résultat)."""
        result_file = workdir / "result.json"
        if result_file.is_file():
            try:
                payload = json.loads(result_file.read_text())
                if isinstance(payload, dict):
                    return payload
            except (OSError, json.JSONDecodeError):
                logger.warning("Failed to parse result.json for job workspace %s", workdir)

        # On parcourt les logs à l'envers pour trouver la dernière ligne JSON en premier
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
        """Détermine le fichier artefact à uploader, avec une cascade de stratégies :
        1. Chemin explicite fourni dans result_payload (output_file ou artifact_path).
        2. Dossier artifacts/ dans le workspace (convention).
        3. Fichier unique dans le workspace (cas simple, hors run.py et result.json)."""
        if candidate:
            # Traduit les chemins /workspace/... en chemins locaux du workdir
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

            # Sécurité : on refuse les chemins qui sortent du workspace (path traversal)
            try:
                resolved.relative_to(workdir.resolve())
            except ValueError:
                logger.warning("Ignoring artifact path outside workspace: %s", candidate)
                return None

            if resolved.is_file():
                return resolved

        # Convention : si le job crée un dossier artifacts/, on prend le premier fichier
        artifact_dir = workdir / "artifacts"
        if artifact_dir.is_dir():
            files = sorted(path for path in artifact_dir.rglob("*") if path.is_file())
            if files:
                return files[0]

        # Dernier recours : s'il n'y a qu'un seul fichier produit, c'est forcément l'artefact
        files = sorted(
            path for path in workdir.iterdir()
            if path.is_file() and path.name not in {"run.py", "result.json"}
        )
        if len(files) == 1:
            return files[0]

        return None

    def _persist_artifact(self, job_id: str, source: Path | None) -> Path | None:
        """Copie l'artefact dans le dossier permanent des artefacts (hors workdir temporaire)."""
        if source is None:
            return None

        target_dir = self._artifact_root / job_id
        safe_rmtree(str(target_dir))
        target_dir.mkdir(parents=True, exist_ok=True)

        target = target_dir / source.name
        shutil.copy2(source, target)
        return target

    def _collect_outputs(self, job_id: str, result: ExecutionResult) -> tuple[dict | None, Path | None]:
        """Collecte les sorties du job (payload JSON + fichier artefact) depuis le workspace.
        Normalise les noms de fichiers dans le payload pour qu'ils correspondent à l'artefact réel."""
        workdir = Path(result.workdir)
        result_payload = self._load_result_payload(workdir, result.logs)
        artifact_candidate = None
        if isinstance(result_payload, dict):
            output_file = result_payload.get("output_file")
            artifact_path = result_payload.get("artifact_path")
            # artifact_path a priorité sur output_file
            artifact_candidate = artifact_path if isinstance(artifact_path, str) else None
            if artifact_candidate is None and isinstance(output_file, str):
                artifact_candidate = output_file

        artifact_source = self._resolve_artifact_source(workdir, artifact_candidate)

        if artifact_source is not None and isinstance(result_payload, dict):
            # On remplace le chemin complet par le nom de fichier seul dans le payload
            # pour que le scheduler puisse construire des URLs propres
            if isinstance(result_payload.get("output_file"), str):
                result_payload["output_file"] = artifact_source.name
            if isinstance(result_payload.get("artifact_path"), str):
                result_payload["artifact_path"] = artifact_source.name

        return result_payload, artifact_source

    async def execute(self, job: JobSpec) -> None:
        """Exécute un job de bout en bout. Toujours dans une tâche asyncio distincte.
        Le bloc finally garantit que les ressources sont libérées même en cas d'exception."""
        allocated = self._resources.allocate(
            job.job_id, job.min_cpu, job.min_mem_mb, job.min_gpu
        )
        if not allocated:
            logger.warning("Cannot allocate resources for job %s, skipping", job.job_id)
            return

        started_at = datetime.now(timezone.utc)
        try:
            # DockerRunner.run() est bloquant (attente du conteneur) : on l'exécute dans
            # un thread pour ne pas bloquer la boucle d'événements asyncio
            result = await asyncio.to_thread(self._runner.run, job)
            ended_at = datetime.now(timezone.utc)
            status = "SUCCESS" if result.success else "FAILED"
            result_payload, artifact = self._collect_outputs(job.job_id, result)
            logger.info(
                "Job %s finished: status=%s exit_code=%d duration=%.1fs",
                job.job_id, status, result.exit_code, result.duration_s,
            )
            if result.logs:
                # On limite à 50 lignes pour ne pas inonder les logs du worker
                for line in result.logs.splitlines()[:50]:
                    logger.info("  [%s] %s", job.job_id, line)

            # Upload sur S3 et génération des URLs presignées pour le scheduler
            result_url = ""
            artifact_url = ""
            if self._s3:
                try:
                    if result_payload:
                        result_key = self._s3.upload_result(job.job_id, result_payload)
                        result_url = self._s3.presign(result_key)
                    if artifact is not None:
                        artifact_key = self._s3.upload_artifact(job.job_id, artifact)
                        artifact_url = self._s3.presign(artifact_key)
                except Exception:
                    logger.exception("Failed to upload results to S3 for job %s", job.job_id)
            elif artifact is not None:
                logger.info("Persisted artifact for job %s at %s", job.job_id, artifact)

            # Nettoyage du workspace temporaire après upload (ou après persistence locale)
            safe_rmtree(result.workdir)

            if self._client:
                try:
                    # En cas d'échec d'exécution, on informe quand même le scheduler pour éviter les timeouts.
                    await self._client.report_job_result(
                        job_id=job.job_id,
                        worker_id=self._worker_id,
                        success=result.success,
                        # Tronqué pour respecter les limites de taille des messages gRPC
                        logs=result.logs[:10000] if result.logs else "",
                        error_message="" if result.success else (result.logs or "")[:2000],
                        result_payload=result_payload,
                        started_at=started_at.isoformat(),
                        ended_at=ended_at.isoformat(),
                        result_url=result_url,
                        artifact_url=artifact_url,
                    )
                except Exception:
                    logger.exception("Failed to report result for job %s", job.job_id)
        except Exception:
            logger.exception("Job %s execution failed", job.job_id)
            if self._client:
                try:
                    # En cas d'échec d'exécution, on informe quand même le scheduler pour éviter les timeouts.
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
            # Toujours libérer les ressources, même si tout a planté
            self._resources.release(job.job_id)
