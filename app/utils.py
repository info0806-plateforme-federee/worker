import shutil
from pathlib import Path


def safe_rmtree(path: str) -> None:
    """Supprime un répertoire récursivement sans lever d'exception s'il n'existe pas.
    Utilisé pour nettoyer les workdirs temporaires des jobs après exécution."""
    p = Path(path)
    if p.exists() and p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
