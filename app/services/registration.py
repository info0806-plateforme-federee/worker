import asyncio
import logging

import grpc

from app.grpc.scheduler_client import SchedulerClient
from app.core.config import Settings

logger = logging.getLogger(__name__)


async def register_worker(client: SchedulerClient, settings: Settings) -> None:
    for attempt in range(1, settings.timing.register_max_retries + 1):
        try:
            resp = await client.register(
                worker_id=settings.worker.id,
                tags=settings.worker.tags,
                total_cpu=settings.worker.total_cpu,
                total_mem_mb=settings.worker.total_mem_mb,
                total_gpu=settings.worker.total_gpu,
            )
            logger.info(
                "Registered with scheduler: worker_id=%s status=%s registered_at=%s",
                resp.worker_id, resp.status, resp.registered_at,
            )
            return
        except grpc.RpcError as e:
            logger.warning(
                "Registration attempt %d/%d failed: %s",
                attempt, settings.timing.register_max_retries, e,
            )
            if attempt < settings.timing.register_max_retries:
                await asyncio.sleep(settings.timing.register_retry_s)

    raise RuntimeError(
        f"Failed to register worker after {settings.timing.register_max_retries} attempts"
    )
