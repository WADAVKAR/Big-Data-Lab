import io

from google.cloud import storage
from google.cloud import pubsub_v1
import time


publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/
topic_path = publisher.topic_path('phonic-silo-266809-47f66d1424ac','to-kafka')
client = storage.Client()
bucket = client.get_bucket('sushantw')
blob = bucket.get_blob('small_temp.csv')
print("Loading the Data...")
x = blob.download_as_string()
x = x.decode('utf-8')
x = x.split('\n')

print("Done. Pushing data to the Kafka Server")
for lines in data[I:]:
	if len(lines)--0:
		break
	time.sleep(10)
	publisher.publish(topic_name, lines.encode(), spam=lines)