import logging

import grpc
from google.protobuf.struct_pb2 import Struct

from grpc_generated import worker_pb2, worker_pb2_grpc

logger = logging.getLogger(__name__)


class SchedulerClient:
    """Façade autour du stub gRPC généré. Masque la construction des messages protobuf
    et expose une interface Python claire avec des types natifs."""

    def __init__(self, channel: grpc.aio.Channel):
        self._stub = worker_pb2_grpc.WorkerServiceStub(channel)

    async def register(
        self,
        worker_id: str,
        tags: list[str],
        total_cpu: int,
        total_mem_mb: int,
        total_gpu: int,
    ) -> worker_pb2.RegisterWorkerResponse:
        """Déclare ce worker auprès du scheduler avec ses capacités totales.
        Appelé une seule fois au démarrage, avant d'accepter des jobs."""
        request = worker_pb2.RegisterWorkerRequest(
            worker_id=worker_id,
            tags=tags,
            total_cpu=total_cpu,
            total_mem_mb=total_mem_mb,
            total_gpu=total_gpu,
        )
        return await self._stub.RegisterWorker(request)

    async def heartbeat(
        self,
        worker_id: str,
        free_cpu: int,
        free_mem_mb: int,
        free_gpu: int,
        running_jobs: int,
        available_slots: int,
        status: str,
    ) -> worker_pb2.HeartbeatWorkerResponse:
        """Envoie l'état courant des ressources libres au scheduler.
        Permet au scheduler de savoir si ce worker peut recevoir de nouveaux jobs
        et de détecter les workers morts (absence de heartbeat)."""
        request = worker_pb2.HeartbeatWorkerRequest(
            worker_id=worker_id,
            free_cpu=free_cpu,
            free_mem_mb=free_mem_mb,
            free_gpu=free_gpu,
            running_jobs=running_jobs,
            available_slots=available_slots,
            status=status,
        )
        return await self._stub.HeartbeatWorker(request)

    async def pull_job(
        self,
        worker_id: str,
        free_cpu: int,
        free_mem_mb: int,
        free_gpu: int,
        available_slots: int,
        tags: list[str],
    ) -> worker_pb2.PullJobResponse:
        """Demande un job au scheduler en précisant les ressources disponibles et les tags.
        Le scheduler choisit le job le plus adapté parmi la file d'attente, ou répond found=False
        si aucun job ne correspond aux capacités du worker."""
        request = worker_pb2.PullJobRequest(
            worker_id=worker_id,
            free_cpu=free_cpu,
            free_mem_mb=free_mem_mb,
            free_gpu=free_gpu,
            available_slots=available_slots,
            tags=tags,
        )
        return await self._stub.PullJob(request)

    async def report_job_result(
        self,
        job_id: str,
        worker_id: str,
        success: bool,
        logs: str = "",
        error_message: str = "",
        result_payload: dict | None = None,
        started_at: str = "",
        ended_at: str = "",
        result_url: str = "",
        artifact_url: str = "",
    ) -> worker_pb2.ReportJobResultResponse:
        """Remonte le résultat d'un job terminé (succès ou échec) vers le scheduler.
        Les URLs S3 presignées permettent au scheduler de donner accès aux résultats
        sans exposer les credentials MinIO."""
        request = worker_pb2.ReportJobResultRequest(
            job_id=job_id,
            worker_id=worker_id,
            success=success,
            logs=logs,
            error_message=error_message,
            started_at=started_at,
            ended_at=ended_at,
            result_url=result_url,
            artifact_url=artifact_url,
        )
        if result_payload:
            # protobuf ne supporte pas les dicts Python directement :
            # on les convertit en google.protobuf.Struct (équivalent JSON générique)
            payload_struct = Struct()
            payload_struct.update(result_payload)
            request.result_payload.CopyFrom(payload_struct)
        return await self._stub.ReportJobResult(request)
