#!/bin/bash
# ============================================================
# Quantedge Backend — Railway Start Script
# ============================================================
set -e

echo "🚀 Starting Quantedge Backend..."

# Run Alembic migrations if alembic.ini exists and migration dir is present
if [ -f "alembic.ini" ] && [ -d "alembic" ]; then
  echo "⚙️  Running database migrations..."
  alembic upgrade head
  echo "✅ Migrations complete."
else
  echo "⚠️  No Alembic config found — skipping migrations."
fi

# Start FastAPI with Uvicorn
echo "🌐 Starting Uvicorn on port ${PORT:-8000}..."
exec uvicorn app.main:app \
  --host 0.0.0.0 \
  --port "${PORT:-8000}" \
  --workers "${WEB_CONCURRENCY:-2}" \
  --log-level info \
  --forwarded-allow-ips "*" \
  --proxy-headers
