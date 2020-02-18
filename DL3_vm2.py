>>> from google.cloud import storage


>>> client = storage.Client()
>>> bucket = client.get_bucket('sushantw')
>>> blob = bucket.get_blob('addresses.csv')
>>> x = blob.download_as_string()
>>> x = x.decode('utf-8')
>>> x = x.split('\n')
>>> count = len(x)-1
>>> count