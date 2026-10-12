"""Code commun aux trois workers : connexion, métriques, arrêt propre.

Tout le dimensionnement passe par des variables d'environnement : on ajuste
chaque déploiement (Kubernetes, docker-compose...) sans toucher au code.
"""

import asyncio
import logging
import os
import signal

from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


async def connect(metrics_port: int) -> Client:
    """Connexion au serveur Temporal, avec un endpoint Prometheus.

    Chaque worker expose ses métriques sur son propre port, pour pouvoir
    lancer les trois workers sur la même machine.

    Les métriques à surveiller pour le dimensionnement :
      - temporal_activity_schedule_to_start_latency : attente dans la queue
      - temporal_workflow_task_schedule_to_start_latency
      - temporal_worker_task_slots_available : slots libres du worker
      - temporal_sticky_cache_size : workflows en cache
    """
    runtime = Runtime(
        telemetry=TelemetryConfig(
            metrics=PrometheusConfig(bind_address=f"0.0.0.0:{metrics_port}")
        )
    )
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        runtime=runtime,
    )


async def run_until_sigterm(worker: Worker) -> None:
    """Lance le worker et l'arrête proprement sur SIGTERM / SIGINT.

    À la sortie du `async with`, le worker arrête de poller, puis attend que
    les activités en cours se terminent, jusqu'à graceful_shutdown_timeout.
    Côté Kubernetes, terminationGracePeriodSeconds doit être supérieur à ce délai.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async with worker:
        logging.info("Worker démarré sur la queue %r", worker.task_queue)
        await stop.wait()
        logging.info("Arrêt demandé, fin des tâches en cours...")
