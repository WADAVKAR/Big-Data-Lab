from __future__ import print_function
from pyspark.context import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression , DecisionTreeClassifier
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, StandardScaler, PolynomialExpansion, RFormula, Imputer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import sys
#sc = SparkContext()
spark = SparkSession(sc)

location = "gs://sushantw/park_violations.csv"
output_location = "gs://sushantw/data_project"
data = spark.read.format("csv").option("header","true").load(location, inferSchema = True)
raw_data = data.withColumn("Summons Number", data["Summons Number"].cast("double"))\
               .withColumn("Registration State", data["Registration State"])\
               .withColumn("Issuing Agency", data["Issuing Agency"])\
               .withColumn("Street Code1", data["Street Code1"].cast("float"))\
               .withColumn("Street Code2", data["Street Code2"].cast("float"))\
                .withColumn("Street Code3", data["Street Code3"].cast("float"))\
                .withColumn("Violation Location", data["Violation Location"])\
               
columns = ["Summons Number", "Registration State", "Issuing Agency", "Street Code1", "Street Code2", "Street Code3", "Violation Location"]
       
raw_data = raw_data.select(columns)        
       
train, test = raw_data.randomSplit([0.8,0.2])

imputer = Imputer(inputCols = ["Summons Number", "Street Code1", "Street Code2", "Street Code3"], outputCols = ["Summons_Number", "Street_Code1", "Street_Code2", "Street_Code3"])
indexer1 = StringIndexer(inputCol = "Registration State", outputCol = "Registration_State", handleInvalid = "skip")
indexer2 = StringIndexer(inputCol = "Violation Location", outputCol = "Violation_Location", handleInvalid = "skip")
indexer3 = StringIndexer(inputCol = "Issuing Agency", outputCol = "Issuing_Agency", handleInvalid = "skip")
assembler = VectorAssembler(inputCols = ["Summons_Number", "Street_Code1", "Street_Code2", "Street_Code3", "Issuing_Agency"], outputCol = "features")

logreg = LogisticRegression(regParam = 0.01, labelCol = "Violation_Location")
pipe = Pipeline(stages=[imputer,indexer1,indexer2,indexer3,assembler, logreg])
model = pipe.fit(train)
model.save("gs://sushantw/Model")
pred_test = model.transform(test)

lr_evaluator = MulticlassClassificationEvaluator(predictionCol = "prediction", labelCol="label",metricName="accuracy")
print(lr_evaluator.evaluate(pred_test))

#paramGrid = ParamGridBuilder().addGrid(lr.regParam,[0.001,0.01,0.1,0.3,0.5]).build()
#crossval = CrossValidator(estimator=pipe, estimatorParamMaps=paramGrid, evaluator=MulticlassClassificationEvaluator(),numFolds=3)
#cvModel = crossval.fit(train)
#prediction = cvModel.transform(test)
#print(lr_evaluator.evaluate(prediction))