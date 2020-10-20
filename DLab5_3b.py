# The imports, above, allow us to access SparkML features specific to linear regression
from __future__ import print_function
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier 
from pyspark.context import SparkContext
from pyspark.ml.feature import StandardScaler, PolynomialExpansion, RFormula
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

sc = SparkContext()
spark = SparkSession(sc)

# Read the data from BigQuery as a Spark Dataframe.
raw_data = spark.read.format("bigquery").option(
    "table", "iris.data_iris").load().toDF("sepal_width", "sepal_length", "petal_length", "petal_width", "class")

index = StringIndexer(inputCol="class", outputCol="classIndex")
raw_data = index.fit(raw_data).transform(raw_data) 

# Set the altitude column as the target label
clean_data = raw_data.withColumn("label", raw_data["classIndex"])

#Split the data
train, test = clean_data.randomSplit([0.8,0.2])

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

assembler = VectorAssembler(inputCols = ["sepal_length","sepal_width","petal_length","petal_width"], outputCol = "features")
poly_fit = PolynomialExpansion(degree = 3, inputCol = "features",outputCol = "Polyfeatures")
scaler_fit = StandardScaler(inputCol="Polyfeatures", outputCol="scaledFeatures",withStd=True, withMean=False)

lr = DecisionTreeClassifier(maxDepth=2)
pipe = Pipeline(stages=[assembler,poly_fit,scaler_fit,lr])
model = pipe.fit(train)
pred_test = model.transform(test)

lr_evaluator = MulticlassClassificationEvaluator(predictionCol = "prediction", labelCol="label",metricName="accuracy")
print(lr_evaluator.evaluate(pred_test))

paramGrid = ParamGridBuilder().addGrid(lr.maxDepth,[2,5,10]).build()
crossval = CrossValidator(estimator=pipe, estimatorParamMaps=paramGrid, evaluator=MulticlassClassificationEvaluator(),numFolds=3)

cvModel = crossval.fit(train)
prediction = cvModel.transform(test)
print(lr_evaluator.evaluate(prediction))