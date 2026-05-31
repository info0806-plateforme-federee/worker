import shutil
from pathlib import Path


def safe_rmtree(path: str) -> None:
    p = Path(path)
    if p.exists() and p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
