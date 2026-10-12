"""Activité d'embedding, exécutée sur la queue "embedding-api".

Le débit global est limité côté serveur Temporal par
max_task_queue_activities_per_second (voir workers/embedding_worker.py).
Cette activité gère en plus les erreurs de l'API de façon différenciée.
"""

import os
from datetime import timedelta

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from rag_ingestion.shared import EMBED_BATCH, EmbedInput

EMBEDDING_URL = os.environ.get("EMBEDDING_URL", "https://embeddings.example.internal/v1/embeddings")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "my-embedding-model")

# Un seul client HTTP par processus : réutilise les connexions.
_http = httpx.AsyncClient(timeout=httpx.Timeout(30.0))


@activity.defn(name=EMBED_BATCH)
async def embed_batch(inp: EmbedInput) -> str:
    """Calcule les embeddings d'un lot de chunks et renvoie la clé S3 des vecteurs."""
    # TODO: lire les textes du lot depuis S3.
    texts = [f"chunk {i} de {inp.doc_id}" for i in range(100)]

    if os.environ.get("EMBEDDING_DRY_RUN", "1") == "1":
        return inp.chunk_batch_ref.replace("chunks", "vectors")

    resp = await _http.post(EMBEDDING_URL, json={"model": EMBEDDING_MODEL, "input": texts})

    if resp.status_code == 429:
        # Retryable : on respecte le Retry-After de l'API si elle en donne un.
        retry_after = resp.headers.get("Retry-After")
        raise ApplicationError(
            "Quota de l'API d'embedding atteint",
            type="RateLimited",
            next_retry_delay=timedelta(seconds=float(retry_after)) if retry_after else None,
        )
    if resp.status_code >= 500:
        raise ApplicationError(f"Erreur serveur {resp.status_code}", type="ServerError")
    if resp.status_code >= 400:
        # Une entrée invalide ne passera jamais : inutile de réessayer.
        raise ApplicationError(
            f"Requête refusée ({resp.status_code}) : {resp.text[:200]}",
            type="InvalidInputError",
            non_retryable=True,
        )

    vectors = [item["embedding"] for item in resp.json()["data"]]
    # TODO: écrire `vectors` sur S3.
    _ = vectors
    return inp.chunk_batch_ref.replace("chunks", "vectors")


ACTIVITIES = [embed_batch]
