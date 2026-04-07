import logging

import grpc
from google.protobuf.struct_pb2 import Struct

from grpc_generated import worker_pb2, worker_pb2_grpc

logger = logging.getLogger(__name__)


class SchedulerClient:
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
    ) -> worker_pb2.ReportJobResultResponse:
        request = worker_pb2.ReportJobResultRequest(
            job_id=job_id,
            worker_id=worker_id,
            success=success,
            logs=logs,
            error_message=error_message,
            started_at=started_at,
            ended_at=ended_at,
        )
        if result_payload:
            payload_struct = Struct()
            payload_struct.update(result_payload)
            request.result_payload.CopyFrom(payload_struct)
        return await self._stub.ReportJobResult(request)
