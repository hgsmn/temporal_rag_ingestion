"""Worker de la queue "ingestion" : workflows + activités CPU légères.

Profil : CPU/mémoire. On utilise le tuner à base de ressources, qui ajuste
le nombre de slots selon l'utilisation réelle du CPU et de la mémoire.
Scaling : plusieurs réplicas identiques de ce worker.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.worker import PollerBehaviorAutoscaling, ResourceBasedSlotConfig, Worker, WorkerTuner

from rag_ingestion.activities.cpu import ACTIVITIES
from rag_ingestion.shared import INGESTION_QUEUE
from rag_ingestion.workers.common import connect, env_float, env_int, run_until_sigterm
from rag_ingestion.workflows import WORKFLOWS


async def main() -> None:
    client = await connect(env_int("INGESTION_METRICS_PORT", 9464))

    max_activity_slots = env_int("MAX_ACTIVITY_SLOTS", 50)

    worker = Worker(
        client,
        task_queue=INGESTION_QUEUE,
        workflows=WORKFLOWS,
        activities=ACTIVITIES,
        # Les activités synchrones (chunk) tournent dans ce pool de threads.
        # Il doit être au moins aussi grand que le nombre max de slots.
        activity_executor=ThreadPoolExecutor(max_workers=max_activity_slots),
        # Slots dynamiques : le worker accepte de nouvelles tâches tant que
        # CPU et mémoire restent sous ces cibles.
        tuner=WorkerTuner.create_resource_based(
            target_memory_usage=env_float("TARGET_MEMORY_USAGE", 0.8),
            target_cpu_usage=env_float("TARGET_CPU_USAGE", 0.9),
            activity_config=ResourceBasedSlotConfig(minimum_slots=2, maximum_slots=max_activity_slots),
            workflow_config=ResourceBasedSlotConfig(minimum_slots=2, maximum_slots=env_int("MAX_WORKFLOW_SLOTS", 100)),
        ),
        # Nombre de requêtes de polling ouvertes, ajusté automatiquement.
        workflow_task_poller_behavior=PollerBehaviorAutoscaling(minimum=2, maximum=20),
        activity_task_poller_behavior=PollerBehaviorAutoscaling(minimum=2, maximum=20),
        # Workflows gardés en mémoire pour éviter de rejouer leur historique.
        # À dimensionner selon la RAM du pod.
        max_cached_workflows=env_int("MAX_CACHED_WORKFLOWS", 1000),
        graceful_shutdown_timeout=timedelta(seconds=env_int("GRACEFUL_SHUTDOWN_SECONDS", 60)),
    )
    await run_until_sigterm(worker)


if __name__ == "__main__":
    asyncio.run(main())
