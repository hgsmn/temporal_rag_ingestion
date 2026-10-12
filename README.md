# Pipeline d'ingestion RAG avec Temporal (Python)

Exemple de référence : trois task queues, trois workers dimensionnés chacun
selon sa contrainte, des métriques Prometheus et un arrêt propre. L'OCR et
l'embedding passent par des API externes ; l'OCR envoie les pages par lots
dont la taille est toujours une puissance de deux.

```
rag_ingestion/
├── shared.py                 # noms des queues, dataclasses, plan_batches() (lots en puissances de deux)
├── workflows.py              # IngestBatchWorkflow (parent) + IngestDocumentWorkflow (enfant)
├── activities/
│   ├── cpu.py                # détection, rasterisation, parsing natif, chunking, upsert -> queue "ingestion"
│   ├── ocr.py                # un appel d'API OCR par lot de pages                     -> queue "ocr-api"
│   └── embedding.py          # un appel d'API d'embedding par lot de chunks            -> queue "embedding-api"
├── workers/
│   ├── common.py             # connexion, Prometheus, arrêt sur SIGTERM
│   ├── ingestion_worker.py   # workflows + activités CPU, tuner basé sur les ressources
│   ├── ocr_worker.py         # rate limit global + concurrence par worker
│   └── embedding_worker.py   # rate limit global
├── start.py                  # lance un lot
└── sizing.py                 # premier dimensionnement par la loi de Little
```

## Flux d'un document scanné

1. `detect_needs_ocr` : seuls les PDF sans couche texte partent à l'OCR.
2. `rasterize_pages` (CPU) : chaque page devient une image sur S3.
3. `plan_batches` (dans le workflow, fonction pure) : 45 pages avec un lot max
   de 16 donnent `16 + 16 + 8 + 4 + 1`. Aucune page de remplissage.
4. `ocr_batch` (queue `ocr-api`) : un appel d'API par lot, tous planifiés en
   parallèle ; le rate limit de la queue fixe le débit réel. Si l'API répond
   413 (lot trop gros), le workflow coupe le lot en deux moitiés, qui restent
   des puissances de deux.
5. `assemble_ocr` : fusion des résultats dans l'ordre des pages.

Chaque lot est une activité indépendante : en cas d'échec, seul ce lot est
réessayé. Les clés S3 des résultats sont déterministes, donc un lot rejoué
réécrit le même objet.

## Lancer en local

```bash
pip install temporalio httpx
temporal server start-dev            # serveur + Web UI sur http://localhost:8233

# un terminal par worker (ports Prometheus distincts en local)
METRICS_PORT=9464 python -m rag_ingestion.workers.ingestion_worker
METRICS_PORT=9465 python -m rag_ingestion.workers.ocr_worker
METRICS_PORT=9466 python -m rag_ingestion.workers.embedding_worker

python -m rag_ingestion.start demo s3://manifests/demo.json
```

Les activités sont des bouchons (`TODO`) : le flux tourne de bout en bout sans
S3 ni API. `OCR_DRY_RUN=0` et `EMBEDDING_DRY_RUN=0` activent les vrais appels
HTTP. `OCR_DRY_RUN_MAX_PAGES=8` sur le worker OCR simule une API qui refuse
les lots de plus de 8 pages, pour voir le repli en action.

Testé avec temporalio 1.34 et Temporal CLI 1.4.1 : un lot de 45 documents
passe ; avec une API simulée limitée à 8 pages, les lots de 16 sont coupés en
2 x 8 et toutes les tailles envoyées restent des puissances de deux.

## Choix de dimensionnement par queue

| Queue | Contrainte | Réglage | Scaling |
|---|---|---|---|
| `ingestion` | CPU / mémoire | `WorkerTuner.create_resource_based` + pollers autoscalés | Réplicas identiques, min. 2 |
| `ocr-api` | Quota et concurrence de l'API OCR | `max_task_queue_activities_per_second` (global) + `max_concurrent_activities` (par worker) | Nombre fixe de réplicas si la concurrence de l'API est plafonnée |
| `embedding-api` | Quota de l'API d'embedding | `max_task_queue_activities_per_second` (global) | 1 à 2 réplicas suffisent |

Une queue par API, même si les deux workers ont le même profil : chaque API a
son propre quota, et le rate limit s'applique à une queue entière. Les deux
workers d'API peuvent en revanche tourner dans le même pod si besoin.

Variables d'environnement (aucune modification de code entre environnements) :

| Variable | Worker | Défaut |
|---|---|---|
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | tous | `localhost:7233`, `default` |
| `METRICS_PORT` | tous | `9464` |
| `GRACEFUL_SHUTDOWN_SECONDS` | tous | 60 (180 pour l'OCR) |
| `MAX_ACTIVITY_SLOTS`, `MAX_WORKFLOW_SLOTS` | ingestion | 50, 100 |
| `TARGET_CPU_USAGE`, `TARGET_MEMORY_USAGE` | ingestion | 0.9, 0.8 |
| `MAX_CACHED_WORKFLOWS` | ingestion | 1000 |
| `OCR_BATCHES_PER_SECOND`, `OCR_MAX_CONCURRENT_REQUESTS` | ocr | 4, 8 |
| `OCR_URL`, `OCR_DRY_RUN`, `OCR_DRY_RUN_MAX_PAGES` | ocr | exemple, `1`, `0` (pas de limite) |
| `MAX_ACTIVITY_SLOTS`, `EMBEDDING_BATCHES_PER_SECOND` | embedding | 50, 8 |
| `WAVE_SIZE`, `OCR_MAX_BATCH` | start | 20 documents par vague, lots de 16 pages |

## Valider le dimensionnement

1. `python -m rag_ingestion.sizing` avec vos hypothèses donne un point de départ.
2. Test de charge, puis lecture des métriques exposées sur `/metrics` :
   - `temporal_activity_schedule_to_start_latency` qui monte : il manque de la capacité sur la queue.
   - `temporal_worker_task_slots_available` à 0 en permanence : worker saturé.
   - slots libres + latence élevée : pas assez de pollers, ou rate limit atteint.
3. `temporal task-queue describe --task-queue ocr-api --task-queue-type activity` pour le backlog.

## Points d'attention en production

- **Kubernetes** : `terminationGracePeriodSeconds` supérieur à `GRACEFUL_SHUTDOWN_SECONDS`.
- **Autoscaling** : sur le backlog ou la schedule-to-start latency (KEDA), pas sur le CPU seul.
  Pour les queues d'API, ajouter des réplicas n'augmente pas le débit au-delà du quota.
- **Même code sur une même queue** : tous les réplicas d'une queue enregistrent les mêmes activités.
- **Rate limit** : même valeur sur tous les workers d'une queue (la dernière reçue par le serveur l'emporte).
- **Taille de lot** : `max_task_queue_activities_per_second` compte des lots, pas des pages.
  Si le quota de l'API est exprimé en pages, divisez-le par la taille de lot.
- **Serveur auto-hébergé** : `numHistoryShards` est fixé à la création du cluster.
