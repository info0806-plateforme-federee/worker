import asyncio
import logging

import grpc

from app.grpc.scheduler_client import SchedulerClient
from app.core.config import Settings

logger = logging.getLogger(__name__)


async def register_worker(client: SchedulerClient, settings: Settings) -> None:
    """Enregistre ce worker auprès du scheduler avec réessai automatique.

    Le worker attend que le scheduler soit disponible avant de continuer :
    le scheduler peut démarrer après le worker (ordre de démarrage Docker non garanti).
    Lève RuntimeError si toutes les tentatives échouent."""
    for attempt in range(1, settings.timing.register_max_retries + 1):
        try:
            # Requête d'enregistrement gRPC avec ressources + tags
            resp = await client.register(
                worker_id=settings.worker.id,
                tags=settings.worker.tags,
                total_cpu=settings.worker.total_cpu,
                total_mem_mb=settings.worker.total_mem_mb,
                total_gpu=settings.worker.total_gpu,
            )
            # Affichage si succès
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
            # On n'attend pas après la dernière tentative pour échouer immédiatement
            if attempt < settings.timing.register_max_retries:
                await asyncio.sleep(settings.timing.register_retry_s)

    raise RuntimeError(
        f"Failed to register worker after {settings.timing.register_max_retries} attempts"
    )
