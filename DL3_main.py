def line_count(data, context):
   file1 = data['name']
   from google.cloud import storage
   client = storage.Client()
   bucket = client.get_bucket('sushantw')
   blob = bucket.get_blob(file1)
   x = blob.download_as_string()
   x = x.decode('utf-8')
   x = x.split('\n')
   count = len(x)-1
   print(count)
   return