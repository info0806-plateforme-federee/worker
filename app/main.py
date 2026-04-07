import asyncio
import logging
import signal

import grpc

from app.core.config import settings
from app.core.logging import setup_logging
from app.grpc.scheduler_client import SchedulerClient
from app.runtime.resources import ResourceTracker
from app.runtime.executor import Executor
from app.services.registration import register_worker
from app.services.heartbeat import heartbeat_loop
from app.services.pull_loop import PullLoop

logger = logging.getLogger(__name__)


async def main() -> None:
    setup_logging()

    logger.info(
        "Worker %s starting (cpu=%d mem=%dMB gpu=%d slots=%d tags=%s)",
        settings.worker.id,
        settings.worker.total_cpu,
        settings.worker.total_mem_mb,
        settings.worker.total_gpu,
        settings.worker.max_slots,
        settings.worker.tags,
    )

    channel = grpc.aio.insecure_channel(settings.grpc.scheduler_url)
    client = SchedulerClient(channel)

    resources = ResourceTracker(
        total_cpu=settings.worker.total_cpu,
        total_mem_mb=settings.worker.total_mem_mb,
        total_gpu=settings.worker.total_gpu,
        max_slots=settings.worker.max_slots,
    )

    await register_worker(client, settings)

    executor = Executor(
        resources,
        scheduler_client=client,
        worker_id=settings.worker.id,
        artifact_root=settings.artifacts.root_path,
    )
    pull_loop = PullLoop(client, executor, resources, settings)

    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        done, pending = await asyncio.wait(
            [
                asyncio.create_task(heartbeat_loop(
                    client, resources, settings.worker.id,
                    settings.timing.heartbeat_interval_s,
                )),
                asyncio.create_task(pull_loop.start()),
                asyncio.create_task(stop_event.wait()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
    finally:
        logger.info("Shutting down...")
        await pull_loop.stop()
        await channel.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
