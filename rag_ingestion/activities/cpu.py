"""Activités CPU légères, exécutées sur la queue "ingestion".

Les appels au stockage objet et au vector store sont à brancher sur vos
propres clients (boto3, qdrant-client, pgvector...). Les parties à remplacer
sont signalées par TODO.
"""

import hashlib

from temporalio import activity

from rag_ingestion.shared import DocumentPage, DocumentRef, RasterizedDoc

CHUNKING_VERSION = "v1"  # à incrémenter quand la stratégie de chunking change


@activity.defn
async def list_documents(manifest_ref: str, offset: int, limit: int) -> DocumentPage:
    """Renvoie une page du manifeste. Seules des références transitent."""
    # TODO: lire le manifeste depuis S3.
    all_docs = [DocumentRef(doc_id=f"doc-{i}", object_ref=f"s3://raw/doc-{i}.pdf") for i in range(45)]
    page = all_docs[offset : offset + limit]
    return DocumentPage(docs=page, has_more=offset + limit < len(all_docs))


@activity.defn
async def detect_needs_ocr(object_ref: str) -> bool:
    """Vérifie si le PDF a déjà une couche texte. Évite d'envoyer à l'API OCR
    des documents qui n'en ont pas besoin : moins de quota consommé et un
    coût plus faible, c'est souvent le plus gros gain."""
    # TODO: ouvrir le PDF (pypdf, pdfplumber) et mesurer le texte extractible.
    return object_ref.endswith("7.pdf")


@activity.defn
async def parse_native(doc_id: str, object_ref: str) -> str:
    """Extrait le texte d'un PDF natif et renvoie la clé S3 du résultat."""
    # TODO: extraction réelle + écriture sur S3.
    return f"s3://parsed/{doc_id}.json"


@activity.defn
def rasterize_pages(doc_id: str, object_ref: str) -> RasterizedDoc:
    """Convertit chaque page du PDF en image sur S3, avant l'envoi à l'API OCR.

    Activité synchrone car CPU-bound (pypdfium2, pdf2image...). Les images
    restent sur S3 : seuls le préfixe et le nombre de pages transitent.
    """
    # TODO: rendu réel des pages et upload vers f"s3://pages/{doc_id}/{n:05d}.png".
    page_count = 1 + int(hashlib.sha256(doc_id.encode()).hexdigest(), 16) % 45  # 1 à 45 pages en démo
    return RasterizedDoc(pages_prefix=f"s3://pages/{doc_id}/", page_count=page_count)


@activity.defn
async def assemble_ocr(doc_id: str, batch_refs: list[str]) -> str:
    """Fusionne les résultats des lots OCR, dans l'ordre des pages, en un document parsé."""
    # TODO: lire chaque lot, concaténer, écrire le résultat sur S3.
    return f"s3://parsed/{doc_id}.json"


@activity.defn
def chunk(doc_id: str, parsed_ref: str) -> list[str]:
    """Découpe le texte parsé en lots de chunks stockés sur S3.

    Activité synchrone (def) car CPU-bound : elle tourne dans le
    ThreadPoolExecutor du worker, sans bloquer la boucle asyncio.
    Renvoie des références vers des lots, jamais les chunks eux-mêmes.
    """
    # TODO: lire parsed_ref, découper, écrire des lots de ~100 chunks sur S3.
    return [f"s3://chunks/{doc_id}/{CHUNKING_VERSION}/batch-{i}.json" for i in range(3)]


def chunk_id(doc_id: str, index: int) -> str:
    """ID déterministe : un upsert rejoué écrase au lieu de dupliquer."""
    return hashlib.sha256(f"{doc_id}:{CHUNKING_VERSION}:{index}".encode()).hexdigest()


@activity.defn
async def upsert_vectors(doc_id: str, vectors_ref: str) -> int:
    """Upsert idempotent dans le vector store. Renvoie le nombre de chunks."""
    # TODO: lire vectors_ref, upsert avec chunk_id(doc_id, i).
    return 100


@activity.defn
async def delete_stale_chunks(doc_id: str) -> None:
    """Supprime les chunks d'une version de chunking précédente."""
    # TODO: delete where doc_id = ... and chunking_version != CHUNKING_VERSION
    return None


ACTIVITIES = [
    list_documents,
    detect_needs_ocr,
    parse_native,
    rasterize_pages,
    assemble_ocr,
    chunk,
    upsert_vectors,
    delete_stale_chunks,
]
