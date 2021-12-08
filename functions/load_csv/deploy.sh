#!/usr/bin/env bash
gcloud functions deploy load_csv \
    --entry-point http_trigger \
    --runtime python37 \
    --trigger-http \
    --timeout 540s \
    --region europe-west1 \
    --project <project> \
    --quiet
