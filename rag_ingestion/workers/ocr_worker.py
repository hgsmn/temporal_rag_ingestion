"""Worker de la queue "ocr-api" : uniquement l'activité OCR par lots.

Profil : I/O réseau vers une API externe, comme l'embedding. Deux limites
à respecter côté API, réglées de deux façons différentes :

- Débit (lots par seconde) : max_task_queue_activities_per_second, limite
  GLOBALE appliquée par le serveur Temporal à toute la queue. À fixer à
  l'identique sur tous les workers de la queue (la dernière valeur reçue
  l'emporte) ; la fixer désactive aussi l'exécution "eager".
- Concurrence (requêtes simultanées) : max_concurrent_activities est PAR
  worker. Concurrence globale = slots x réplicas. Avec un plafond strict
  côté API, on garde un nombre fixe de réplicas.
"""

import asyncio
from datetime import timedelta

from temporalio.worker import Worker

from rag_ingestion.activities.ocr import ACTIVITIES
from rag_ingestion.shared import OCR_QUEUE
from rag_ingestion.workers.common import connect, env_float, env_int, run_until_sigterm


async def main() -> None:
    client = await connect(env_int("OCR_METRICS_PORT", 9466))

    worker = Worker(
        client,
        task_queue=OCR_QUEUE,
        activities=ACTIVITIES,
        max_concurrent_activities=env_int("OCR_MAX_CONCURRENT_REQUESTS", 8),
        max_task_queue_activities_per_second=env_float("OCR_BATCHES_PER_SECOND", 4.0),
        # Un lot peut prendre plusieurs dizaines de secondes : on laisse le
        # temps aux appels en cours de se terminer avant d'arrêter le worker.
        graceful_shutdown_timeout=timedelta(seconds=env_int("GRACEFUL_SHUTDOWN_SECONDS", 180)),
    )
    await run_until_sigterm(worker)


if __name__ == "__main__":
    asyncio.run(main())
