from google.cloud import pubsub_v1
from google.cloud import storage
# TODO project_id = "Your Google Cloud Project ID"
# TODO subscription_name = "Your Pub/Sub subscription name"
# TODO timeout = 5.0  # "How long the subscriber should listen for
# messages in seconds"
timeout = 10
subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_name}`
subscription_path = subscriber.subscription_path(
    'phonic-silo-266809', 'newsub'
)
def co(data):
    client = storage.Client()
    bucket = client.get_bucket('sushantw')
    blob = bucket.get_blob(data)
    x = blob.download_as_string()
    x = x.decode('utf-8')
    x = x.split('\n')
    l = len(x)-1
    return(l)
def callback(message):
    print("Received message: {}".format(message))
    print(co(message.data))
    message.ack()

streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback
)
print("Listening for messages on {}..\n".format(subscription_path))

# result() in a future will block indefinitely if `timeout` is not set,
# unless an exception is encountered first.
try:
    streaming_pull_future.result(timeout=timeout)
except:  # noqa
    streaming_pull_future.cancel()
