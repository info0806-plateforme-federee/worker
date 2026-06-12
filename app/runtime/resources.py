import threading
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class JobAllocation:
    """Représente les ressources réservées pour un job en cours d'exécution."""
    job_id: str
    cpu: int
    mem_mb: int
    gpu: int


class ResourceTracker:
    """Gestion thread-safe des ressources disponibles sur ce worker.

    DockerRunner.run() tourne dans un thread séparé (asyncio.to_thread), donc
    plusieurs jobs peuvent accéder à cette classe concurremment — d'où le verrou."""

    def __init__(self, total_cpu: int, total_mem_mb: int, total_gpu: int, max_slots: int):
        self._total_cpu = total_cpu
        self._total_mem_mb = total_mem_mb
        self._total_gpu = total_gpu
        self._max_slots = max_slots
        # Dictionnaire job_id pour allocation : source de vérité sur les jobs actifs
        self._allocations: dict[str, JobAllocation] = {}
        self._lock = threading.Lock()

    @property
    def free_cpu(self) -> int:
        # Calcul du CPU libre en soustrayant les ressources allouées de la capacité totale
        with self._lock:
            used = sum(a.cpu for a in self._allocations.values())
            return max(0, self._total_cpu - used)

    @property
    def free_mem_mb(self) -> int:
        # Calcul de la RAM libre en soustrayant les ressources allouées de la capacité totale
        with self._lock:
            used = sum(a.mem_mb for a in self._allocations.values())
            return max(0, self._total_mem_mb - used)

    @property
    def free_gpu(self) -> int:
        # Calcul du GPU libre en soustrayant les ressources allouées de la capacité totale
        with self._lock:
            used = sum(a.gpu for a in self._allocations.values())
            return max(0, self._total_gpu - used)

    @property
    def running_jobs(self) -> int:
        # retourne nb jobs en cours d'exécution
        with self._lock:
            return len(self._allocations)

    @property
    def available_slots(self) -> int:
        """Nombre de jobs supplémentaires que ce worker peut accepter.
        Limité par max_slots indépendamment de la capacité CPU/RAM/GPU restante."""
        with self._lock:
            return max(0, self._max_slots - len(self._allocations))

    def status(self) -> str:
        # retourne l'état du worker : "busy" si des jobs sont en cours, "idle" sinon
        with self._lock:
            return "busy" if self._allocations else "idle"

    def allocate(self, job_id: str, cpu: int, mem_mb: int, gpu: int) -> bool:
        """Tente de réserver les ressources demandées pour un job.
        Retourne False si les ressources ou les slots sont insuffisants,
        True si l'allocation a réussi. Atomique grâce au verrou."""
        with self._lock:
            if len(self._allocations) >= self._max_slots:
                return False
            used_cpu = sum(a.cpu for a in self._allocations.values())
            used_mem = sum(a.mem_mb for a in self._allocations.values())
            used_gpu = sum(a.gpu for a in self._allocations.values())
            if used_cpu + cpu > self._total_cpu:
                return False
            if used_mem + mem_mb > self._total_mem_mb:
                return False
            if used_gpu + gpu > self._total_gpu:
                return False
            # allocation si toutes les conditions sont remplies
            self._allocations[job_id] = JobAllocation(job_id, cpu, mem_mb, gpu)
            logger.info("Allocated resources for job %s: cpu=%d mem=%dMB gpu=%d", job_id, cpu, mem_mb, gpu)
            return True

    def release(self, job_id: str) -> None:
        """Libère les ressources d'un job terminé. Toujours appelé dans le bloc finally
        de l'Executor pour garantir qu'aucun job ne reste bloqué en allocation."""
        with self._lock:
            alloc = self._allocations.pop(job_id, None)
            if alloc:
                logger.info("Released resources for job %s: cpu=%d mem=%dMB gpu=%d", job_id, alloc.cpu, alloc.mem_mb, alloc.gpu)
