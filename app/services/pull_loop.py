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
    """Boucle principale de consommation de jobs.

    Combine deux mécanismes de déclenchement :
    - NATS (event-driven) : réaction quasi-immédiate quand un job est publié.
    - Timer de fallback : polling toutes les N secondes si NATS est indisponible.

    Les deux convergent vers le même asyncio.Event (_trigger), ce qui simplifie
    la logique : peu importe la source, on essaie de tirer un job dès que le trigger est activé."""

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
        # Événement central : déclenche une tentative de pull dès qu'il est set
        self._trigger = asyncio.Event()
        self._nc: nats.NATS | None = None
        self._sub = None
        # Suivi des tâches d'exécution en cours pour pouvoir les attendre à l'arrêt
        self._tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Démarre la connexion NATS (best-effort) puis lance les deux boucles en parallèle."""
        try:
            # Connexion à NATS avec gestion des reconnexions et déconnexions
            self._nc = await nats.connect(
                self._settings.nats.url,
                reconnected_cb=self._on_reconnect,
                disconnected_cb=self._on_disconnect,
            )
            logger.info("Connected to NATS at %s", self._settings.nats.url)
            # Abonnement au notif de jobs dispo avec callback
            self._sub = await self._nc.subscribe(
                self._settings.nats.subject_jobs_available,
                cb=self._on_nats_message,
            )
            logger.info("Subscribed to %s", self._settings.nats.subject_jobs_available)
        except Exception:
            # NATS est optionnel : le worker reste fonctionnel en mode polling uniquement
            logger.warning("NATS connection failed, falling back to polling only")

        # Trigger initial pour traiter les jobs qui existaient avant le démarrage du worker
        self._trigger.set()

        await asyncio.gather(
            self._fallback_timer(),
            self._main_loop(),
        )

    async def _on_nats_message(self, msg) -> None:
        """Callback NATS : le contenu du message n'est pas utilisé,
        seul le signal "quelque chose est disponible" importe."""
        logger.debug("NATS %s: %s", msg.subject, msg.data.decode(errors="replace"))
        self._trigger.set()

    async def _on_reconnect(self) -> None:
        logger.info("Reconnected to NATS")

    async def _on_disconnect(self) -> None:
        logger.warning("Disconnected from NATS")

    async def _fallback_timer(self) -> None:
        """Active le trigger périodiquement pour garantir qu'aucun job ne reste bloqué
        en file d'attente si NATS est hors ligne ou si un message a été raté."""
        while True:
            await asyncio.sleep(self._settings.timing.pull_interval_s)
            self._trigger.set()

    async def _main_loop(self) -> None:
        """Boucle principale : attend le trigger, puis tente de tirer un job via gRPC.
        Si un job est trouvé, il est soumis à l'executor en tâche asyncio indépendante,
        ce qui permet d'exécuter plusieurs jobs en parallèle (dans la limite des slots)."""
        while True:
            await self._trigger.wait()
            self._trigger.clear()

            # Vérification rapide avant l'appel réseau pour éviter un round-trip inutile
            if self._resources.available_slots <= 0:
                continue

            try:
                # Pull d'un job depuis le scheduler via gRPC
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

                # job trouvé : transformation du gRPC en JobSpec + envoie à l'executor
                job = JobSpec.from_pull_response(resp)
                logger.info("Pulled job %s (%s)", job.job_id, job.name)

                # Le job tourne en arrière-plan : on ne bloque pas la boucle
                task = asyncio.create_task(self._executor.execute(job))
                self._tasks.add(task)
                # Nettoyage automatique du set quand la tâche se termine
                task.add_done_callback(self._tasks.discard)

                # Si des slots sont encore libres, on enchaîne immédiatement sur un autre job
                if self._resources.available_slots > 0:
                    self._trigger.set()

            except grpc.RpcError as e:
                logger.warning("PullJob RPC failed: %s", e)
                await asyncio.sleep(self._settings.timing.pull_interval_s)
            except Exception:
                logger.exception("Pull loop error")
                await asyncio.sleep(self._settings.timing.pull_interval_s)

    async def stop(self) -> None:
        """Arrêt propre : désabonnement NATS, drain des messages en attente,
        puis attente de la fin de tous les jobs en cours avant de rendre la main."""
        if self._sub:
            await self._sub.unsubscribe()
        if self._nc and self._nc.is_connected:
            # drain() attend que les messages en cours de traitement soient terminés
            await self._nc.drain()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
