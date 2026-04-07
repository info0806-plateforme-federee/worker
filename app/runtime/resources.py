import threading
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class JobAllocation:
    job_id: str
    cpu: int
    mem_mb: int
    gpu: int


class ResourceTracker:
    def __init__(self, total_cpu: int, total_mem_mb: int, total_gpu: int, max_slots: int):
        self._total_cpu = total_cpu
        self._total_mem_mb = total_mem_mb
        self._total_gpu = total_gpu
        self._max_slots = max_slots
        self._allocations: dict[str, JobAllocation] = {}
        self._lock = threading.Lock()

    @property
    def free_cpu(self) -> int:
        with self._lock:
            used = sum(a.cpu for a in self._allocations.values())
            return max(0, self._total_cpu - used)

    @property
    def free_mem_mb(self) -> int:
        with self._lock:
            used = sum(a.mem_mb for a in self._allocations.values())
            return max(0, self._total_mem_mb - used)

    @property
    def free_gpu(self) -> int:
        with self._lock:
            used = sum(a.gpu for a in self._allocations.values())
            return max(0, self._total_gpu - used)

    @property
    def running_jobs(self) -> int:
        with self._lock:
            return len(self._allocations)

    @property
    def available_slots(self) -> int:
        with self._lock:
            return max(0, self._max_slots - len(self._allocations))

    def status(self) -> str:
        with self._lock:
            return "busy" if self._allocations else "idle"

    def allocate(self, job_id: str, cpu: int, mem_mb: int, gpu: int) -> bool:
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
            self._allocations[job_id] = JobAllocation(job_id, cpu, mem_mb, gpu)
            logger.info("Allocated resources for job %s: cpu=%d mem=%dMB gpu=%d", job_id, cpu, mem_mb, gpu)
            return True

    def release(self, job_id: str) -> None:
        with self._lock:
            alloc = self._allocations.pop(job_id, None)
            if alloc:
                logger.info("Released resources for job %s: cpu=%d mem=%dMB gpu=%d", job_id, alloc.cpu, alloc.mem_mb, alloc.gpu)
