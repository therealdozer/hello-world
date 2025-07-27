import os
from google.cloud import pubsub_v1

def receive_messages(project_id, subscription_name):
    """Receives messages from a Pub/Sub subscription."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    def callback(message):
        print(f"Received message: {message.data}")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap the subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, the subscriber listens indefinitely.
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()

if __name__ == "__main__":
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    subscription_name = "my-subscription"
    receive_messages(project_id, subscription_name)
