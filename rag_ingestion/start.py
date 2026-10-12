"""Démarre l'ingestion d'un lot.

Usage : python -m rag_ingestion.start <batch_id> <manifest_ref>
"""

import asyncio
import os
import sys

from temporalio.client import Client

from rag_ingestion.shared import INGESTION_QUEUE, BatchInput
from rag_ingestion.workflows import IngestBatchWorkflow


async def main() -> None:
    batch_id = sys.argv[1] if len(sys.argv) > 1 else "demo"
    manifest_ref = sys.argv[2] if len(sys.argv) > 2 else "s3://manifests/demo.json"

    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    handle = await client.start_workflow(
        IngestBatchWorkflow.run,
        BatchInput(
            batch_id=batch_id,
            manifest_ref=manifest_ref,
            wave_size=int(os.environ.get("WAVE_SIZE", 20)),
            ocr_max_batch=int(os.environ.get("OCR_MAX_BATCH", 16)),
        ),
        id=f"ingest-batch-{batch_id}",
        task_queue=INGESTION_QUEUE,
    )
    print(f"Lot démarré : {handle.id}. Suivi dans le Web UI (http://localhost:8233).")
    print(await handle.result())


if __name__ == "__main__":
    asyncio.run(main())
