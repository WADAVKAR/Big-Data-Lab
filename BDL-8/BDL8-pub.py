from google.cloud import storage
from google.cloud import pubsub_v1
import time
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('phonic-silo-266809-47f66d1424ac','to-kafka' )
client = storage.Client()
bucket = client.get_bucket('sushantw')


blob = bucket.get_blob('iris_test.csv')
x = blob.download_as_string()
x = x.decode('utf-8')
x = x.split('\n')
futures = dict()


def get_call(f, data):
    def call(f):
        try:
            print(f.result())
            futures.pop(data)
        except:  
            print("Please handle {} for {}.".format(f.exception(), data))

    return call


for i in range(len(x)):
    data = x[i]
    futures.update({data: None})
    future = publisher.publish(topic_path,b'hi',val =data)
    futures[data] = future
    print(x[i])
    future.add_done_call(get_call(future, data))
    time.sleep(10)
   # Wait for all the publish futures to resolve before exiting.


print("Message published")

