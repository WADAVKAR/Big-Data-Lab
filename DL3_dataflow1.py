import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'phonic-silo-266809' #Enter your <200b>project<200b> ID google_cloud_options.job_name = <200b>'lab3'
google_cloud_options.job_name = 'lab3'
google_cloud_options.temp_location = "gs://sushantw/tmp" #<200b>This<200b> is to store temp results, format is gs:<200b>//my_bucket/tmp
options.view_as(StandardOptions).runner = 'DataflowRunner'
p = beam.Pipeline(options=options)
lines = p | 'Read' >> beam.io.ReadFromText('gs://iitm/files/file.txt') |beam.combiners.Count.Globally()| 'Write' >> beam.io.WriteToText('gs://sushantw/outputs/')
result = p.run()