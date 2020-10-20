# The imports, above, allow us to access SparkML features specific to linear regression
from __future__ import print_function
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression 
from pyspark.context import SparkContext
from pyspark.ml.feature import StandardScaler, PolynomialExpansion
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline

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

#Collect the relevant columns into a multi-feature column
assembler = VectorAssembler(inputCols = ['sepal_width', 'sepal_length', 'petal_length', 'petal_width'], outputCol = "features")

poly_fit = PolynomialExpansion(degree = 2, inputCol = "features", outputCol = "poly_features")
scaler_fit = StandardScaler(inputCol = "poly_features", outputCol = "scaled_features")

# Construct a new LinearRegression object
lr = LogisticRegression(maxIter=1000, regParam=0.3, elasticNetParam=0.8)

#Create a pipeline of transforms
pipe = Pipeline(stages=[assembler, poly_fit, scaler_fit, lr])

#Train your model and get the coefficients
model = pipe.fit(train)


#import packages to perform cross validation
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import StringIndexer, StandardScaler, PolynomialExpansion, RFormula 

# Create a grid of multiple values of the hyper-parameter regParam
paramGrid = ParamGridBuilder().addGrid(lr.regParam,[0.001,0.01,0.03,0.1,0.3]).build()

#Create a CrossValidator Object
crossval = CrossValidator(estimator=pipe, estimatorParamMaps=paramGrid, evaluator=MulticlassClassificationEvaluator(),numFolds=3)

#Train the model with the CrossValidator Object 
cvModel = crossval.fit(train)


# Acquire and print the best model summary from the CrossValidator object
print("Coefficients:" + str(cvModel.bestModel.stages[-1].coefficientMatrix))
#print("Intercept:" + str(cvModel.bestModel.stages[-1].intercept))
#print("R^2:" + str(cvModel.bestModel.stages[-1].summary.r2))

#Perform prediction on Test data
prediction = cvModel.transform(test)

#Create an evaluation object for the model using the R^2 metric
lr_evaluator = MulticlassClassificationEvaluator(predictionCol = "prediction", labelCol="label",metricName="accuracy")

#Print the Evaluation Result
print(lr_evaluator.evaluate(prediction))


