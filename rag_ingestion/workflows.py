"""Workflows d'ingestion. Ils tournent tous sur la queue "ingestion".

Rappel : le code d'un workflow doit être déterministe. Pas d'I/O, pas
d'aléatoire, pas d'heure système : tout cela se fait dans les activités.
"""

import asyncio
import dataclasses
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from rag_ingestion.activities.cpu import (
        assemble_ocr,
        chunk,
        delete_stale_chunks,
        detect_needs_ocr,
        list_documents,
        parse_native,
        rasterize_pages,
        upsert_vectors,
    )
    from rag_ingestion.shared import (
        BATCH_TOO_LARGE,
        EMBED_BATCH,
        EMBEDDING_QUEUE,
        OCR_BATCH,
        OCR_QUEUE,
        BatchInput,
        DocumentRef,
        EmbedInput,
        IngestResult,
        OcrBatchInput,
        plan_batches,
    )

# Activités CPU courtes, sur la queue du workflow ("ingestion").
CPU_OPTS = dict(
    start_to_close_timeout=timedelta(minutes=2),
    retry_policy=RetryPolicy(maximum_attempts=5),
)

# Rasterisation : plus longue pour les gros documents.
RASTER_OPTS = dict(
    start_to_close_timeout=timedelta(minutes=15),
    retry_policy=RetryPolicy(maximum_attempts=3),
)

# Appels aux API externes : retries avec backoff pour absorber les 429 et 5xx.
# schedule_to_close_timeout borne l'attente totale (file d'attente comprise) :
# si le quota est saturé trop longtemps, le document échoue proprement.
API_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=10,
    non_retryable_error_types=["InvalidInputError", BATCH_TOO_LARGE],
)

OCR_OPTS = dict(
    task_queue=OCR_QUEUE,
    start_to_close_timeout=timedelta(minutes=5),  # un appel d'API par lot
    schedule_to_close_timeout=timedelta(hours=6),
    retry_policy=API_RETRY,
)

EMBED_OPTS = dict(
    task_queue=EMBEDDING_QUEUE,
    start_to_close_timeout=timedelta(minutes=1),
    schedule_to_close_timeout=timedelta(hours=6),
    retry_policy=API_RETRY,
)


def _is_batch_too_large(err: ActivityError) -> bool:
    return isinstance(err.cause, ApplicationError) and err.cause.type == BATCH_TOO_LARGE


@workflow.defn
class IngestDocumentWorkflow:
    """Ingestion d'un document : parsing (OCR par lots si besoin), chunking, embedding, indexation."""

    def __init__(self) -> None:
        self._ocr_batches: list[int] = []

    async def _ocr_range(self, doc_id: str, pages_prefix: str, first: int, size: int) -> list[str]:
        """OCR d'un lot de pages. Si l'API le refuse car trop gros, on le coupe
        en deux moitiés, qui restent des puissances de deux."""
        try:
            ref = await workflow.execute_activity(
                OCR_BATCH, OcrBatchInput(doc_id, pages_prefix, first, size), result_type=str, **OCR_OPTS
            )
            self._ocr_batches.append(size)
            return [ref]
        except ActivityError as err:
            if not _is_batch_too_large(err) or size == 1:
                raise
            half = size // 2
            workflow.logger.warning("Lot de %d pages refusé pour %s, découpe en 2 x %d", size, doc_id, half)
            left, right = await asyncio.gather(
                self._ocr_range(doc_id, pages_prefix, first, half),
                self._ocr_range(doc_id, pages_prefix, first + half, half),
            )
            return left + right

    async def _parse_with_ocr(self, doc: DocumentRef) -> str:
        raster = await workflow.execute_activity(rasterize_pages, args=[doc.doc_id, doc.object_ref], **RASTER_OPTS)

        # Plan des lots calculé dans le workflow : fonction pure, donc déterministe.
        batches = plan_batches(raster.page_count, doc.ocr_max_batch)

        # Tous les lots sont planifiés d'un coup. Le débit réel est borné par
        # le rate limit de la queue ocr-api, pas par ce code.
        results = await asyncio.gather(
            *(self._ocr_range(doc.doc_id, raster.pages_prefix, first, size) for first, size in batches)
        )
        batch_refs = [ref for refs in results for ref in refs]  # ordre des pages conservé
        return await workflow.execute_activity(assemble_ocr, args=[doc.doc_id, batch_refs], **CPU_OPTS)

    @workflow.run
    async def run(self, doc: DocumentRef) -> IngestResult:
        # 1. Parsing, en n'envoyant à l'API OCR que ce qui en a besoin.
        needs_ocr = await workflow.execute_activity(detect_needs_ocr, doc.object_ref, **CPU_OPTS)
        if needs_ocr:
            parsed_ref = await self._parse_with_ocr(doc)
        else:
            parsed_ref = await workflow.execute_activity(parse_native, args=[doc.doc_id, doc.object_ref], **CPU_OPTS)

        # 2. Chunking : renvoie des références vers des lots, pas les chunks.
        batch_refs = await workflow.execute_activity(chunk, args=[doc.doc_id, parsed_ref], **CPU_OPTS)

        # 3. Embedding + upsert, lots en parallèle.
        async def embed_and_index(batch_ref: str) -> int:
            vectors_ref = await workflow.execute_activity(
                EMBED_BATCH, EmbedInput(doc.doc_id, batch_ref), result_type=str, **EMBED_OPTS
            )
            return await workflow.execute_activity(upsert_vectors, args=[doc.doc_id, vectors_ref], **CPU_OPTS)

        counts = await asyncio.gather(*(embed_and_index(ref) for ref in batch_refs))

        # 4. Nettoyage des chunks d'une ancienne version.
        await workflow.execute_activity(delete_stale_chunks, doc.doc_id, **CPU_OPTS)

        return IngestResult(
            doc_id=doc.doc_id, chunk_count=sum(counts), used_ocr=needs_ocr, ocr_batches=self._ocr_batches
        )


@workflow.defn
class IngestBatchWorkflow:
    """Workflow parent : fan-out par vagues de child workflows.

    - Les vagues bornent le nombre de documents en vol : on ne dépose pas
      50 000 tâches d'un coup dans les queues.
    - continue_as_new redémarre le parent avec un historique vide, en
      transportant l'état (offset, compteurs).
    """

    @workflow.run
    async def run(self, inp: BatchInput) -> dict:
        children_this_run = 0
        while True:
            page = await workflow.execute_activity(
                list_documents, args=[inp.manifest_ref, inp.offset, inp.wave_size], **CPU_OPTS
            )

            results = await asyncio.gather(
                *(
                    workflow.execute_child_workflow(
                        IngestDocumentWorkflow.run,
                        dataclasses.replace(doc, ocr_max_batch=inp.ocr_max_batch),
                        # ID déterministe : relance ciblée, et Temporal refuse
                        # de lancer deux fois le même document en parallèle.
                        id=f"ingest-doc-{doc.doc_id}",
                        task_queue=workflow.info().task_queue,
                    )
                    for doc in page.docs
                ),
                return_exceptions=True,  # un document en échec ne fait pas échouer le lot
            )

            for doc, res in zip(page.docs, results):
                if isinstance(res, BaseException):
                    workflow.logger.warning("Échec de %s : %s", doc.doc_id, res)
                    inp.failed.append(doc.doc_id)
                else:
                    inp.succeeded += 1

            inp.offset += len(page.docs)
            children_this_run += len(page.docs)
            if not page.has_more:
                break

            # Deux garde-fous : la suggestion du serveur (historique trop long)
            # et le plafond d'enfants par exécution recommandé par la doc.
            if (
                workflow.info().is_continue_as_new_suggested()
                or children_this_run + inp.wave_size > inp.max_children_per_run
            ):
                workflow.continue_as_new(inp)

        return {"batch_id": inp.batch_id, "succeeded": inp.succeeded, "failed": inp.failed}


WORKFLOWS = [IngestBatchWorkflow, IngestDocumentWorkflow]
