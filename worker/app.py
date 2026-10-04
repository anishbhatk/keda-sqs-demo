import os
import time
import boto3

QUEUE_URL = os.environ["QUEUE_URL"]
WORK_TIME = int(os.getenv("WORK_TIME", "10"))

sqs = boto3.client("sqs")


def process_message(message):
    body = message["Body"]

    print(f"Received message: {body}", flush=True)
    print(f"Processing {body} for {WORK_TIME} seconds...", flush=True)

    time.sleep(WORK_TIME)

    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=message["ReceiptHandle"]
    )

    print(f"Completed message: {body}", flush=True)


def main():
    print("SQS worker started", flush=True)
    print(f"Queue: {QUEUE_URL}", flush=True)

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            VisibilityTimeout=30
        )

        messages = response.get("Messages", [])

        if not messages:
            continue

        for message in messages:
            try:
                process_message(message)
            except Exception as error:
                print(
                    f"Error processing message: {error}",
                    flush=True
                )


if __name__ == "__main__":
    main()
