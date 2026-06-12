import logging
import sys


def setup_logging() -> None:
    """Configure le logging global de l'application.
    Les logs applicatifs sont émis en INFO vers stdout (requis par Docker/Kubernetes).
    Les libs tierces verbeuses sont bridées à WARNING pour ne pas noyer les logs métier."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # Ces trois libs génèrent beaucoup de bruit en DEBUG/INFO (connexions HTTP, retries, etc.)
    logging.getLogger("docker").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
