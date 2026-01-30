from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import split, col
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import MultilayerPerceptronClassifier


# Create a SparkSession
spark = SparkSession.builder.appName("DDoS Detection").getOrCreate()

data_path = "/Users/jejebubu/Desktop/bigproject/envkafka/test_file.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

print("DataFrame Columns:", df.columns)

# Indexing label column
label_indexer = StringIndexer(inputCol="Label", outputCol="label").fit(df)
df = label_indexer.transform(df)

# Indexing IP columns and port columns
for col_name in ['srcport','dstport',"ip_proto", "tcp_state"]:
    indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_indexed")
    df = indexer.fit(df).transform(df)


# Split IP address columns into octets
df = df.withColumn("ip_src_octets", split(col("ip_src"), "\."))
df = df.withColumn("ip_dst_octets", split(col("ip_dst"), "\."))

# Convert octets to integers
for i in range(4):
    df = df.withColumn("ip_src_octet_" + str(i), df["ip_src_octets"][i].cast(IntegerType()))
    df = df.withColumn("ip_dst_octet_" + str(i), df["ip_dst_octets"][i].cast(IntegerType()))
   
# Feature columns
feature_cols = ["ip_src_octet_0", "ip_src_octet_1", "ip_src_octet_2", "ip_src_octet_3",
                "srcport_indexed", 
                "ip_dst_octet_0", "ip_dst_octet_1", "ip_dst_octet_2", "ip_dst_octet_3",
                "dstport_indexed", 
                "ip_proto_indexed", "tcp_state_indexed", 
                "frame_len", "tcp_seq", "tcp_ack", 
                "Packets", "Bytes", "Tx_Packets", 
                "Tx_Bytes", "Rx_Packets", "Rx_Bytes"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")


# Define Logistic Regression model
#lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10, regParam=0.3, elasticNetParam=0.8)


# Define Decision Tree Classifier model
#dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxBins=64541)


'''
num_classes = df.select(col("label")).distinct().count()
layers = [len(feature_cols), 64, 32, num_classes]

# Define the Multilayer Perceptron Classifier model
mlp = MultilayerPerceptronClassifier(labelCol="label", featuresCol="features", layers=layers, seed=1234)

'''

# Random Forest Classifier
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10, maxBins=64541)

# Pipeline
# For using each function: change data
pipeline = Pipeline(stages=[assembler, rf])

# Train and Test Split
train_data, test_data = df.randomSplit([0.7, 0.3])

# Model Training
model = pipeline.fit(train_data)

# Save the trained model
model_path = "/Users/jejebubu/Desktop/bigproject/envkafka/rt_model"
model.write().overwrite().save(model_path)

# Predictions
predictions = model.transform(test_data)

# Define the evaluator for accuracy
evaluator_accuracy = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator_accuracy.evaluate(predictions)
print("Accuracy:", accuracy)

'''
# Define the evaluator for precision
evaluator_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
precision = evaluator_precision.evaluate(predictions)
print("Precision:", precision)

# Define the evaluator for recall
evaluator_recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
recall = evaluator_recall.evaluate(predictions)
print("Recall:", recall)

# Define the evaluator for F1-score
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
f1_score = evaluator_f1.evaluate(predictions)
print("F1-Score:", f1_score)

'''

spark.stop()