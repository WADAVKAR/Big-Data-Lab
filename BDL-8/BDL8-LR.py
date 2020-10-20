from __future__ import print_function 
from pyspark.context import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression , DecisionTreeClassifier
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, StandardScaler, PolynomialExpansion, RFormula
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


import sys
sc = SparkContext()
spark = SparkSession(sc)

data = spark.read.format("bigquery").option("table","Data8.table8").load().toDF("sepal_length","sepal_width","petal_length","petal_width","classn")
indexer = StringIndexer(inputCol="classn", outputCol="label")


assembler = VectorAssembler(inputCols = ["sepal_length","sepal_width","petal_length","petal_width"], outputCol = "features")
poly = PolynomialExpansion(degree = 3, inputCol = "features",outputCol = "Polyfeatures")
scaler = StandardScaler(inputCol="Polyfeatures", outputCol="scaledFeatures",withStd=True, withMean=False)
lr = LogisticRegression(maxIter = 1000,regParam = 0.3, elasticNetParam = 0.8)
pipe = Pipeline(stages=[indexer,assembler,poly,scaler,lr])
model = pipe.fit(data)
model.save("gs://sushantw/Model")

