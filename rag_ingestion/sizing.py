"""Premier dimensionnement par la loi de Little : concurrence = débit x durée.

Usage : python -m rag_ingestion.sizing
Remplacez les hypothèses ci-dessous par vos chiffres, puis validez-les par un
test de charge en lisant schedule_to_start_latency et les slots disponibles.
"""

import math

from rag_ingestion.shared import plan_batches

# --- Hypothèses (à remplacer par vos mesures) ---
DOCS_PER_HOUR = 2000  # débit visé en régime de croisière
OCR_SHARE = 0.30  # part des documents qui ont besoin d'OCR
PAGES_PER_OCR_DOC = 20  # pages moyennes d'un document scanné
OCR_MAX_BATCH = 16  # taille de lot visée (puissance de deux)
OCR_SECONDS_PER_BATCH = 8.0  # latence moyenne de l'API pour un lot plein
OCR_API_BATCHES_PER_SECOND = 5  # quota de l'API OCR, en requêtes/s
OCR_API_MAX_CONCURRENT = 32  # requêtes simultanées autorisées par l'API
CHUNK_BATCHES_PER_DOC = 3  # lots de chunks par document
EMBED_SECONDS_PER_BATCH = 1.5  # latence moyenne d'un appel d'embedding
EMBED_API_QUOTA_PER_SECOND = 10  # quota de l'API d'embedding, en requêtes/s
CPU_ACTIVITY_SECONDS_PER_DOC = 6  # détection + parsing natif + chunking + upserts
RASTER_SECONDS_PER_PAGE = 0.3  # rendu d'une page en image (CPU)
SLOTS_PER_INGESTION_REPLICA = 25  # capacité observée d'un pod "ingestion"
HEADROOM = 1.3  # marge pour les pics et les retries
API_MARGIN = 0.8  # on vise 80 % du quota d'une API


def main() -> None:
    docs_per_s = DOCS_PER_HOUR / 3600
    print(f"Débit visé : {docs_per_s:.2f} doc/s\n")

    # OCR : le plan des lots d'un document moyen donne le nombre de requêtes.
    plan = plan_batches(PAGES_PER_OCR_DOC, OCR_MAX_BATCH)
    batches_per_doc = len(plan)
    ocr_rate = docs_per_s * OCR_SHARE * batches_per_doc  # requêtes/s nécessaires
    ocr_limit = OCR_API_BATCHES_PER_SECOND * API_MARGIN
    ocr_concurrency = min(ocr_rate, ocr_limit) * OCR_SECONDS_PER_BATCH * HEADROOM
    print(f"[ocr-api]       un document de {PAGES_PER_OCR_DOC} pages = lots {[s for _, s in plan]}")
    print(f"                {ocr_rate:.2f} lots/s nécessaires, limite fixée à {ocr_limit:.1f}/s")
    if ocr_rate > ocr_limit:
        print("                ATTENTION : le quota de l'API OCR est le goulot, un backlog va se former.")
    print(
        f"                {math.ceil(ocr_concurrency)} requêtes simultanées nécessaires "
        f"(plafond de l'API : {OCR_API_MAX_CONCURRENT})"
    )
    if ocr_concurrency > OCR_API_MAX_CONCURRENT:
        print("                ATTENTION : la concurrence de l'API est le goulot.")

    # Embedding.
    embed_rate = docs_per_s * CHUNK_BATCHES_PER_DOC
    embed_limit = EMBED_API_QUOTA_PER_SECOND * API_MARGIN
    embed_concurrency = min(embed_rate, embed_limit) * EMBED_SECONDS_PER_BATCH * HEADROOM
    print(f"[embedding-api] {embed_rate:.2f} lots/s nécessaires, limite fixée à {embed_limit:.1f}/s")
    if embed_rate > embed_limit:
        print("                ATTENTION : le quota de l'API est le goulot, un backlog va se former.")
    print(f"                {math.ceil(embed_concurrency)} requêtes simultanées suffisent")

    # Ingestion : activités CPU + rasterisation des documents scannés.
    raster_s = OCR_SHARE * PAGES_PER_OCR_DOC * RASTER_SECONDS_PER_PAGE
    cpu_concurrency = docs_per_s * (CPU_ACTIVITY_SECONDS_PER_DOC + raster_s) * HEADROOM
    ingestion_replicas = max(2, math.ceil(cpu_concurrency / SLOTS_PER_INGESTION_REPLICA))
    print(
        f"[ingestion]     {cpu_concurrency:.0f} activités CPU en parallèle "
        f"-> {ingestion_replicas} réplicas (min. 2 pour la HA)"
    )


if __name__ == "__main__":
    main()
