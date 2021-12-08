#!/usr/bin/env bash
gcloud beta workflows deploy etl-cars \
    --source=etl_cars.yaml \
    --location=<zone> \
    --service-account=<service-account-email> \
    --async
