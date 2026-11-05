#!/bin/sh
# Create the buckets, queues, and tables the local adapter expects.
set -eu

awslocal s3 mb s3://emr-sync-raw
awslocal s3 mb s3://emr-sync-curated
awslocal s3 mb s3://emr-sync-quarantine
awslocal s3 mb s3://emr-sync-reports
awslocal s3api put-bucket-versioning \
  --bucket emr-sync-raw \
  --versioning-configuration Status=Enabled

awslocal sqs create-queue --queue-name emr-sync-inbound.fifo --attributes FifoQueue=true
awslocal sqs create-queue --queue-name emr-sync-rejects.fifo --attributes FifoQueue=true
awslocal sqs create-queue --queue-name emr-sync-dlq.fifo --attributes FifoQueue=true

for table in emr-sync-idempotency emr-sync-audit emr-sync-clinical emr-sync-review; do
  awslocal dynamodb create-table \
    --table-name "$table" \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
done
