import asyncio
import logging

import grpc
import nats

from app.core.config import Settings
from app.grpc.scheduler_client import SchedulerClient
from app.runtime.executor import Executor, JobSpec
from app.runtime.resources import ResourceTracker

logger = logging.getLogger(__name__)


class PullLoop:
    def __init__(
        self,
        client: SchedulerClient,
        executor: Executor,
        resources: ResourceTracker,
        settings: Settings,
    ):
        self._client = client
        self._executor = executor
        self._resources = resources
        self._settings = settings
        self._trigger = asyncio.Event()
        self._nc: nats.NATS | None = None
        self._sub = None
        self._tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        try:
            self._nc = await nats.connect(
                self._settings.nats.url,
                reconnected_cb=self._on_reconnect,
                disconnected_cb=self._on_disconnect,
            )
            logger.info("Connected to NATS at %s", self._settings.nats.url)

            self._sub = await self._nc.subscribe(
                self._settings.nats.subject_jobs_available,
                cb=self._on_nats_message,
            )
            logger.info("Subscribed to %s", self._settings.nats.subject_jobs_available)
        except Exception:
            logger.warning("NATS connection failed, falling back to polling only")

        # Initial trigger to pull any jobs that were created before we started
        self._trigger.set()

        await asyncio.gather(
            self._fallback_timer(),
            self._main_loop(),
        )

    async def _on_nats_message(self, msg) -> None:
        logger.debug("NATS %s: %s", msg.subject, msg.data.decode(errors="replace"))
        self._trigger.set()

    async def _on_reconnect(self) -> None:
        logger.info("Reconnected to NATS")

    async def _on_disconnect(self) -> None:
        logger.warning("Disconnected from NATS")

    async def _fallback_timer(self) -> None:
        while True:
            await asyncio.sleep(self._settings.timing.pull_interval_s)
            self._trigger.set()

    async def _main_loop(self) -> None:
        while True:
            await self._trigger.wait()
            self._trigger.clear()

            if self._resources.available_slots <= 0:
                continue

            try:
                resp = await self._client.pull_job(
                    worker_id=self._settings.worker.id,
                    free_cpu=self._resources.free_cpu,
                    free_mem_mb=self._resources.free_mem_mb,
                    free_gpu=self._resources.free_gpu,
                    available_slots=self._resources.available_slots,
                    tags=self._settings.worker.tags,
                )

                if not resp.found:
                    continue

                job = JobSpec.from_pull_response(resp)
                logger.info("Pulled job %s (%s)", job.job_id, job.name)

                task = asyncio.create_task(self._executor.execute(job))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

                # If slots remain, immediately try to pull another
                if self._resources.available_slots > 0:
                    self._trigger.set()

            except grpc.RpcError as e:
                logger.warning("PullJob RPC failed: %s", e)
                await asyncio.sleep(self._settings.timing.pull_interval_s)
            except Exception:
                logger.exception("Pull loop error")
                await asyncio.sleep(self._settings.timing.pull_interval_s)

    async def stop(self) -> None:
        if self._sub:
            await self._sub.unsubscribe()
        if self._nc and self._nc.is_connected:
            await self._nc.drain()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
