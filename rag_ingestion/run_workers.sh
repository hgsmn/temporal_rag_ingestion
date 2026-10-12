#!/usr/bin/env bash
# Lance les trois workers dans un seul terminal.
#
# Usage :
#   ./run_workers.sh               # les trois workers (serveur Temporal déjà lancé)
#   ./run_workers.sh --dev-server  # démarre aussi un serveur Temporal local
#
# - Le .env à la racine du projet est chargé par chaque worker.
# - Les logs de chaque worker sont préfixés par son nom.
# - Ctrl+C envoie SIGTERM à chaque worker : ils terminent les tâches en cours
#   (graceful shutdown), puis le script se termine.
# - Si un worker s'arrête tout seul (erreur de connexion, crash), les autres
#   sont arrêtés aussi et le script sort en erreur.
set -uo pipefail
cd "$(dirname "$0")"

# Python du venv s'il existe, sinon celui du système.
if [[ -x .venv/bin/python ]]; then PYTHON=.venv/bin/python; else PYTHON=${PYTHON:-python3}; fi
export PYTHONUNBUFFERED=1

if [[ ! -f .env ]]; then
  echo "Avertissement : pas de .env, les valeurs par défaut du code s'appliquent (cp .env.example .env)." >&2
fi

WORKERS=(ingestion ocr embedding)
PIDS=()
NAMES=()
STOPPING=0

start() {  # start <nom> <commande...>
  local name=$1; shift
  # La substitution de processus garde le PID du programme (et non de sed) dans $!.
  "$@" > >(sed -u "s/^/[$name] /") 2>&1 &
  PIDS+=($!)
  NAMES+=("$name")
  echo "[run] $name démarré (pid $!)"
}

stop_all() {
  [[ $STOPPING -eq 1 ]] && return
  STOPPING=1
  echo "[run] Arrêt des workers (fin des tâches en cours)..."
  for pid in "${PIDS[@]}"; do kill -TERM "$pid" 2>/dev/null; done
  wait 2>/dev/null
  echo "[run] Tout est arrêté."
}
trap 'stop_all; exit 0' INT TERM

if [[ "${1:-}" == "--dev-server" ]]; then
  command -v temporal >/dev/null || { echo "CLI temporal introuvable : https://docs.temporal.io/cli" >&2; exit 1; }
  start server temporal server start-dev --log-level error
  echo "[run] Attente du serveur Temporal..."
  for _ in $(seq 1 30); do
    temporal operator cluster health >/dev/null 2>&1 && break
    sleep 1
  done
  temporal operator cluster health >/dev/null 2>&1 || { echo "[run] Le serveur ne répond pas." >&2; stop_all; exit 1; }
  echo "[run] Serveur prêt, Web UI sur http://localhost:8233"
fi

for w in "${WORKERS[@]}"; do
  start "$w" "$PYTHON" -m "rag_ingestion.workers.${w}_worker"
done
echo "[run] Ctrl+C pour tout arrêter."

# Surveillance : si un processus s'arrête de lui-même, on arrête tout.
# (Boucle plutôt que `wait -n`, pour rester compatible avec le bash 3 de macOS.)
while true; do
  for i in "${!PIDS[@]}"; do
    if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
      wait "${PIDS[$i]}"; code=$?
      echo "[run] ${NAMES[$i]} s'est arrêté (code $code)." >&2
      stop_all
      exit 1
    fi
  done
  sleep 1
done