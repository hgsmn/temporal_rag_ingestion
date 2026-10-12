"""Activité OCR par lots, exécutée sur la queue "ocr-api".

Profil identique à l'embedding : I/O réseau vers une API externe, limité par
son quota. Chaque lot de pages est une activité distincte : en cas d'échec,
seul ce lot est réessayé, sans heartbeat ni reprise page par page.
"""

import asyncio
import os
from datetime import timedelta

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from rag_ingestion.shared import BATCH_TOO_LARGE, OCR_BATCH, OcrBatchInput

OCR_URL = os.environ.get("OCR_URL", "https://ocr.example.internal/v1/ocr")
OCR_MODEL = os.environ.get("OCR_MODEL", "my-ocr-model")
OCR_API_KEY = os.environ.get("OCR_API_KEY", "")

# Un seul client HTTP par processus : réutilise les connexions.
_http = httpx.AsyncClient(
    timeout=httpx.Timeout(float(os.environ.get("OCR_TIMEOUT_SECONDS", "120"))),
    headers={"Authorization": f"Bearer {OCR_API_KEY}"} if OCR_API_KEY else {},
)


def _page_refs(inp: OcrBatchInput) -> list[str]:
    return [f"{inp.pages_prefix}{n:05d}.png" for n in range(inp.first_page, inp.first_page + inp.size)]


def _result_ref(inp: OcrBatchInput) -> str:
    # Clé déterministe : un lot rejoué réécrit le même objet (idempotence).
    return f"s3://ocr/{inp.doc_id}/pages-{inp.first_page:05d}-{inp.size}.json"


@activity.defn(name=OCR_BATCH)
async def ocr_batch(inp: OcrBatchInput) -> str:
    """Envoie un lot de pages à l'API OCR et renvoie la clé S3 du résultat."""
    pages = _page_refs(inp)

    if os.environ.get("OCR_DRY_RUN", "1") == "1":
        # Simulation : latence proportionnelle au lot, et refus des lots
        # au-dessus de OCR_DRY_RUN_MAX_PAGES pour tester le repli.
        limit = int(os.environ.get("OCR_DRY_RUN_MAX_PAGES", "0"))
        if limit and inp.size > limit:
            raise ApplicationError(f"Lot de {inp.size} pages refusé", type=BATCH_TOO_LARGE, non_retryable=True)
        await asyncio.sleep(0.05 * inp.size)
        return _result_ref(inp)

    # TODO: selon l'API, envoyer les images elles-mêmes ou des URL présignées.
    resp = await _http.post(OCR_URL, json={"model": OCR_MODEL, "images": pages})

    if resp.status_code == 413:
        # Lot trop gros pour l'API : inutile de réessayer tel quel,
        # le workflow va le couper en deux.
        raise ApplicationError(f"Lot de {inp.size} pages trop gros", type=BATCH_TOO_LARGE, non_retryable=True)
    if resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After")
        raise ApplicationError(
            "Quota de l'API OCR atteint",
            type="RateLimited",
            next_retry_delay=timedelta(seconds=float(retry_after)) if retry_after else None,
        )
    if resp.status_code >= 500:
        raise ApplicationError(f"Erreur serveur {resp.status_code}", type="ServerError")
    if resp.status_code >= 400:
        raise ApplicationError(
            f"Requête refusée ({resp.status_code}) : {resp.text[:200]}",
            type="InvalidInputError",
            non_retryable=True,
        )

    results = resp.json()
    # TODO: écrire `results` sur S3 à _result_ref(inp).
    _ = results
    return _result_ref(inp)


ACTIVITIES = [ocr_batch]
