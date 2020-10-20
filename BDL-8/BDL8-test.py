from __future__ import print_function 
from pyspark import SparkContext
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression , DecisionTreeClassifier
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, StandardScaler, PolynomialExpansion, RFormula
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import PipelineModel
from pyspark.sql.types import (StructType, StringType, StructField, LongType)
from pyspark.sql import Row
import sys


kafka_topic = 'from-pubsub'
zk = '10.142.0.19:2181'
app_name = "from-pubsub"
sc = SparkContext(appName="KafkaPubsub")
ssc = StreamingContext(sc, 50)
kafkaStream = KafkaUtils.createStream(ssc, zk, app_name, {kafka_topic: 1})
indexer = StringIndexer(inputCol="classn", outputCol="label")
assembler = VectorAssembler(inputCols = ["sepal_length","sepal_width","petal_length","petal_width"], outputCol = "features")
poly = PolynomialExpansion(degree = 3, inputCol = "features",outputCol = "Polyfeatures")
scaler = StandardScaler(inputCol="Polyfeatures", outputCol="scaledFeatures",withStd=True, withMean=False)
lr = LogisticRegression(maxIter = 1000,regParam = 0.3, elasticNetParam = 0.8)
pipe = Pipeline(stages=[indexer,assembler,poly,scaler,lr])
model = PipelineModel.load("gs://sushantw/Model")
spark = SparkSession(sc)


def fun(x):
    df = x.map(lambda r:r[1])
    df1 = df.map(lambda r:r[25:-2].split(','))
    df2 = df1.map(lambda r : Row(sepal_length = float(r[0]),sepal_width = float(r[1]),petal_length = float(r[2]),petal_width = float(r[3]),classn = r[4]))
    df3 = spark.createDataFrame(df2)
    df3.show()
    pred = model.transform(dat3)
    pred.select("Prediction").show()
    

def rdd_empty():
    print("RDD Empty" \n "Wait")

kafkaStream.foreachRDD(lambda rdd: ssc.stop() if rdd.count() == 0 else fun(rdd))

ssc.start() #Computation started

ssc.awaitTermination()