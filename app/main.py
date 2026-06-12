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
from app.storage.s3_client import S3Client

logger = logging.getLogger(__name__)


async def main() -> None:
    """Point d'entrée principal du worker. Initialise tous les composants
    et lance les boucles (heartbeat et pull_jobs) en parallèle."""
    # Initialisation du logging
    setup_logging()

    # Affichage des informations de démarrage du worker : ID, ressources, tags, URL du scheduler
    logger.info(
        "Worker %s starting (scheduler=%s cpu=%d mem=%dMB gpu=%d slots=%d tags=%s)",
        settings.worker.id,
        settings.grpc.scheduler_url,
        settings.worker.total_cpu,
        settings.worker.total_mem_mb,
        settings.worker.total_gpu,
        settings.worker.max_slots,
        settings.worker.tags,
    )

    # Canal gRPC non chiffré pour l'enregistrement, heartbeat, pullJob, reportJob
    channel = grpc.aio.insecure_channel(settings.grpc.scheduler_url)
    client = SchedulerClient(channel)

    # Suivi des ressources disponibles sur ce nœud (CPU, RAM, GPU, slots)
    resources = ResourceTracker(
        total_cpu=settings.worker.total_cpu,
        total_mem_mb=settings.worker.total_mem_mb,
        total_gpu=settings.worker.total_gpu,
        max_slots=settings.worker.max_slots,
    )

    # Enregistrement bloquant : le worker ne peut pas accepter de jobs avant d'être connu du scheduler
    await register_worker(client, settings)

    # Initialisation du client S3 pour la gestion des artefacts (logs, outputs)
    s3_client = S3Client(settings.s3)
    logger.info("S3 client configured (endpoint=%s, bucket=%s)", settings.s3.endpoint_url, settings.s3.bucket)

    # Executor pour gérer l'exécution des jobs : allocation des ressources, lancement des processus, gestion des artefacts
    executor = Executor(
        resources,
        scheduler_client=client,
        worker_id=settings.worker.id,
        artifact_root=settings.artifacts.root_path,
        s3_client=s3_client,
    )
    # Boucle principale pour récupérer les jobs à exécuter. Elle tourne en parallèle du heartbeat.
    pull_loop = PullLoop(client, executor, resources, settings)

    # Événement déclenché par les signaux OS pour initier un arrêt propre
    stop_event = asyncio.Event()

    # Handler pour les signaux SIGTERM et SIGINT (Ctrl+C) : déclenche l'événement d'arrêt
    def _signal_handler():
        logger.info("Shutdown signal received")
        stop_event.set()

    # Enregistrement des handlers de signal dans la boucle asyncio courante
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        # Les trois tâches tournent en parallèle. Dès que l'une se termine
        # (typiquement stop_event sur signal), on annule les autres.
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
        # Arrêt propre : on attend la fin des jobs en cours avant de fermer le canal gRPC
        logger.info("Shutting down...")
        await pull_loop.stop()
        await channel.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
