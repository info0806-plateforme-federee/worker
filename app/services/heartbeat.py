import asyncio
import logging

from app.grpc.scheduler_client import SchedulerClient
from app.runtime.resources import ResourceTracker

logger = logging.getLogger(__name__)


async def heartbeat_loop(
    client: SchedulerClient,
    resources: ResourceTracker,
    worker_id: str,
    interval_s: int,
) -> None:
    while True:
        try:
            resp = await client.heartbeat(
                worker_id=worker_id,
                free_cpu=resources.free_cpu,
                free_mem_mb=resources.free_mem_mb,
                free_gpu=resources.free_gpu,
                running_jobs=resources.running_jobs,
                available_slots=resources.available_slots,
                status=resources.status(),
            )
            logger.debug("Heartbeat acknowledged: %s", resp.acknowledged)
        except Exception:
            logger.exception("Heartbeat failed")
        await asyncio.sleep(interval_s)
