"""Constantes, types et fonctions pures partagés par les workflows, les activités et les workers.

Ce module ne doit importer aucune dépendance lourde : il est chargé dans le
sandbox des workflows et par tous les workers.
"""

from dataclasses import dataclass, field

# --- Noms des task queues : centralisés pour éviter les fautes de frappe ---
INGESTION_QUEUE = "ingestion"  # workflows + activités CPU (rasterisation, chunking...)
OCR_QUEUE = "ocr-api"  # appels à l'API OCR externe (quota global)
EMBEDDING_QUEUE = "embedding-api"  # appels à l'API d'embedding (quota global)

# --- Noms des activités exécutées sur d'autres queues que celle du workflow ---
# Le workflow les appelle par leur nom : le worker "ingestion" n'a pas besoin
# des clients HTTP ni des secrets des API externes.
OCR_BATCH = "ocr_batch"
EMBED_BATCH = "embed_batch"

# Type d'erreur renvoyé par l'activité OCR quand l'API refuse un lot trop gros
# (HTTP 413). Le workflow coupe alors le lot en deux.
BATCH_TOO_LARGE = "BatchTooLarge"


@dataclass
class BatchInput:
    """Entrée du workflow parent.

    On passe une référence vers un manifeste (liste de documents stockée sur S3)
    plutôt que la liste elle-même : l'historique Temporal reste léger.
    """

    batch_id: str
    manifest_ref: str
    offset: int = 0
    wave_size: int = 20  # nombre de documents traités en parallèle
    # La doc recommande qu'un parent ne lance pas plus de 1 000 enfants
    # par exécution : on fait un continue_as_new avant.
    max_children_per_run: int = 500
    # Taille maximale d'un lot de pages envoyé à l'API OCR (puissance de deux).
    ocr_max_batch: int = 16
    succeeded: int = 0
    failed: list[str] = field(default_factory=list)


@dataclass
class DocumentRef:
    doc_id: str  # identifiant stable, par exemple le sha256 du fichier
    object_ref: str  # clé S3 du fichier source
    ocr_max_batch: int = 16


@dataclass
class DocumentPage:
    docs: list[DocumentRef]
    has_more: bool


@dataclass
class RasterizedDoc:
    """Pages d'un document converties en images sur S3 : {pages_prefix}{n:05d}.png"""

    pages_prefix: str
    page_count: int


@dataclass
class OcrBatchInput:
    doc_id: str
    pages_prefix: str
    first_page: int
    size: int


@dataclass
class EmbedInput:
    doc_id: str
    chunk_batch_ref: str  # clé S3 d'un lot de chunks


@dataclass
class IngestResult:
    doc_id: str
    chunk_count: int
    used_ocr: bool
    ocr_batches: list[int] = field(default_factory=list)  # tailles des lots envoyés


def is_power_of_two(n: int) -> bool:
    return n > 0 and n & (n - 1) == 0


def plan_batches(page_count: int, max_batch: int) -> list[tuple[int, int]]:
    """Découpe [0, page_count) en lots dont la taille est toujours une puissance de deux.

    Glouton : on prend à chaque fois la plus grande puissance de deux qui tient
    dans le reste, plafonnée à max_batch. Exemple pour 45 pages, max 16 :
    16 + 16 + 8 + 4 + 1. Aucune page de remplissage n'est envoyée.

    Fonction pure et déterministe : utilisable directement dans un workflow.
    Renvoie une liste de (première page, taille).
    """
    if not is_power_of_two(max_batch):
        raise ValueError(f"max_batch doit être une puissance de deux, reçu {max_batch}")
    batches: list[tuple[int, int]] = []
    start = 0
    while start < page_count:
        remaining = page_count - start
        size = min(max_batch, 1 << (remaining.bit_length() - 1))
        batches.append((start, size))
        start += size
    return batches
