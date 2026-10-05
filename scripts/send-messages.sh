#!/bin/bash

set -e

QUEUE_URL="${QUEUE_URL:?QUEUE_URL environment variable is required}"
AWS_REGION="${AWS_REGION:-ap-south-2}"
MESSAGE_COUNT="${1:-10}"

echo "Sending ${MESSAGE_COUNT} messages to SQS..."
echo "Queue: ${QUEUE_URL}"
echo "Region: ${AWS_REGION}"

for i in $(seq 1 "$MESSAGE_COUNT"); do
    aws sqs send-message \
        --queue-url "$QUEUE_URL" \
        --message-body "job-$i" \
        --region "$AWS_REGION" \
        >/dev/null

    echo "Sent job-$i"
done

echo "Successfully sent ${MESSAGE_COUNT} messages."
