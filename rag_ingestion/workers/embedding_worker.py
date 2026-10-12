"""Worker de la queue "embedding-api" : uniquement l'activité d'embedding.

Profil : I/O réseau, limité par le quota de l'API externe.
- Activités async : beaucoup de slots possibles pour peu de CPU.
- max_task_queue_activities_per_second : limite GLOBALE appliquée par le
  serveur Temporal à toute la queue, quel que soit le nombre de réplicas.
  En cas de valeurs différentes, la dernière reçue par le serveur l'emporte :
  on la fixe donc à l'identique sur tous les workers de la queue. La fixer
  désactive aussi l'exécution "eager", qui contournerait la limite.

Ajouter des réplicas au-delà de ce qu'il faut pour atteindre le quota ne
sert à rien : le quota reste le plafond.
"""

import asyncio
from datetime import timedelta

from temporalio.worker import Worker

from rag_ingestion.activities.embedding import ACTIVITIES
from rag_ingestion.shared import EMBEDDING_QUEUE
from rag_ingestion.workers.common import connect, env_float, env_int, run_until_sigterm


async def main() -> None:
    client = await connect()

    worker = Worker(
        client,
        task_queue=EMBEDDING_QUEUE,
        activities=ACTIVITIES,
        max_concurrent_activities=env_int("MAX_ACTIVITY_SLOTS", 50),
        # Juste sous le quota de l'API (en lots par seconde), pour garder
        # une marge face aux retries.
        max_task_queue_activities_per_second=env_float("EMBEDDING_BATCHES_PER_SECOND", 8.0),
        graceful_shutdown_timeout=timedelta(seconds=env_int("GRACEFUL_SHUTDOWN_SECONDS", 60)),
    )
    await run_until_sigterm(worker)


if __name__ == "__main__":
    asyncio.run(main())
