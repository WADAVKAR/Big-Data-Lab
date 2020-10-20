kafka_topic = 'from-pubsub'

z='10.142.0.19:2181'
app_name = 'from-pubsub'
sc=SparkContext(appName='KafkaPubsub')
ssc=StreamingContext(sc,30)
sc.setLogLevel("FATAL")

kafkaStream=KafkaUtils.createStream(ssc,z,app_name,{kafka_topic:1})
def getSparkSessionInstance(sparkConf):
	if("sparkSessionSingletonInstance" not in globals()):
		globals()["sparkSessionSingletonInstance"]=SparkSession \
		.builder \
		.config(conf=sparkConf) \ 
		.getOrCreate()
	return globals()["sparkSessionSingletonInstance"]


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

spark = SparkSession(sc)


accuracy=0
completed=0
def process(rdd):
	start=time.time()
	global accuracy
	global completed
	spark=getSparkSessionInstance(rdd.context.getConf())
	rowRdd=rdd.map(Lamda x:         Row(Summons_Number=str(x[0]),Registration_State=str(x[1]),Plate_Type=str(x[3]),Violation_Code=str(x[4]),Vehicle_Body_Type=str(x[5]),Vehicle_Make=str(x[6]),Issuing_Agency=str(x[7]),Street_Code1=str(x[8]),Street_Code2=str(x[9]),Street_Code3=str(x[10]),Violation_County=str(x[11]),Issuer_Precint=str(x[13]),Issuer_Command=str(x[14]),Issuer_Squad=str(x[17]),Violation_In_Front_Of_Or_Opposite=str(x[21]),Issue_Date=str(x[4]),Violation_Time=str(x[18]),Violation_Location=str(x[20]))) 
	df=spark.createDataFrame(rowRdd) 



def empty_rdd():
    print("The current RDD is empty. Wait for the next complete RDD")
kafkaStream.foreachRDD(lambda rdd: ssc.stop() if rdd.count() == 0 else process(rdd))

ssc.start() # Start the computation
ssc.awaitTermination()




