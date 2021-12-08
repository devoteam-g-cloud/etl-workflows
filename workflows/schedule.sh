#!/usr/bin/env bash
gcloud beta scheduler jobs create http run-etl-cars \
  --schedule="0 8 * * *" \ # every day at 8
  --uri="https://workflowexecutions.googleapis.com/v1/projects/<project>/locations/<zone>/workflows/etl-cars/executions" \
  --time-zone="CET" \
  --oauth-service-account-email=<service-account-email>
