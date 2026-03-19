#!/bin/bash
set -euo pipefail

PROJECT_ID="sondreskarsten-d7d14"
REGION="europe-west4"
REPO="losore"
IMAGE="losore-pipeline"
JOB_NAME="losore-agder"
SA_EMAIL="s1sfreracct@sondreskarsten-d7d14.iam.gserviceaccount.com"

GCS_BUCKET="${GCS_BUCKET:-sondre_brreg_data}"
GCS_PREFIX="${GCS_PREFIX:-losore/agder}"
KOMMUNENUMMER="${KOMMUNENUMMER:-4201,4202,4203,4204,4205,4206,4207,4211,4212,4213,4214,4215,4216,4217,4218,4219,4220,4221,4222,4223,4224,4225,4226,4227,4228}"
SEARCH_TERMS="${SEARCH_TERMS:-SPAREBANKEN NORGE,SPAREBANKEN SØR}"

IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE}:latest"

echo "=== Creating Artifact Registry repo (if needed) ==="
gcloud artifacts repositories describe "${REPO}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" 2>/dev/null || \
gcloud artifacts repositories create "${REPO}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --repository-format=docker

echo "=== Building and pushing image ==="
gcloud builds submit \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --tag="${IMAGE_URI}" \
    .

echo "=== Deploying Cloud Run Job ==="
gcloud run jobs create "${JOB_NAME}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --image="${IMAGE_URI}" \
    --service-account="${SA_EMAIL}" \
    --task-timeout=4h \
    --max-retries=1 \
    --memory=1Gi \
    --cpu=1 \
    --set-env-vars="GCS_BUCKET=${GCS_BUCKET},GCS_PREFIX=${GCS_PREFIX},KOMMUNENUMMER=${KOMMUNENUMMER},SEARCH_TERMS=${SEARCH_TERMS}" \
    2>/dev/null || \
gcloud run jobs update "${JOB_NAME}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --image="${IMAGE_URI}" \
    --service-account="${SA_EMAIL}" \
    --task-timeout=4h \
    --max-retries=1 \
    --memory=1Gi \
    --cpu=1 \
    --set-env-vars="GCS_BUCKET=${GCS_BUCKET},GCS_PREFIX=${GCS_PREFIX},KOMMUNENUMMER=${KOMMUNENUMMER},SEARCH_TERMS=${SEARCH_TERMS}"

echo "=== Starting execution ==="
gcloud run jobs execute "${JOB_NAME}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --wait=false

echo ""
echo "Job submitted. Poll status with:"
echo "  gsutil cat gs://${GCS_BUCKET}/${GCS_PREFIX}/status.json | python3 -m json.tool"
echo ""
echo "Or stream logs:"
echo "  gcloud run jobs executions list --job=${JOB_NAME} --project=${PROJECT_ID} --region=${REGION}"
