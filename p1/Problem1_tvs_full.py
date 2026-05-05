from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, VectorIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
import sys
import time

if len(sys.argv) < 2:
    print("Usage: spark-submit Problem1_tvs_full.py /path/to/heart_2020_cleaned.csv")
    sys.exit(1)

input_path = sys.argv[1]

spark = SparkSession.builder \
    .appName("Problem1DecisionTreeTrainValidationSplitFull") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


start = time.time()

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

df = df.repartition(8).cache()

print("\n=== FULL DATA LOADED ===")
print("Rows:", df.count())
print("Columns:", len(df.columns))
print("Column names:", df.columns)


numeric_cols = [
    "BMI",
    "PhysicalHealth",
    "MentalHealth",
    "SleepTime"
]

label_col = "HeartDisease"

categorical_cols = [
    "Smoking",
    "AlcoholDrinking",
    "Stroke",
    "DiffWalking",
    "Sex",
    "AgeCategory",
    "Race",
    "Diabetic",
    "PhysicalActivity",
    "GenHealth",
    "Asthma",
    "KidneyDisease",
    "SkinCancer"
]


indexers = [
    StringIndexer(
        inputCol=c,
        outputCol=c + "_idx",
        handleInvalid="keep"
    )
    for c in categorical_cols
]

# Important:
# For binary classification, do NOT use handleInvalid="keep" for the label.
# Otherwise Spark may create a third label class.
label_indexer = StringIndexer(
    inputCol=label_col,
    outputCol="label",
    handleInvalid="error"
)

feature_cols = numeric_cols + [c + "_idx" for c in categorical_cols]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="rawFeatures",
    handleInvalid="keep"
)

vector_indexer = VectorIndexer(
    inputCol="rawFeatures",
    outputCol="features",
    maxCategories=20,
    handleInvalid="keep"
)


dt = DecisionTreeClassifier(
    labelCol="label",
    featuresCol="features",
    seed=42
)


pipeline = Pipeline(
    stages=indexers + [
        label_indexer,
        assembler,
        vector_indexer,
        dt
    ]
)



train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

train_df = train_df.cache()
test_df = test_df.cache()

print("\n=== SPLIT ===")
print("Training rows:", train_df.count())
print("Testing rows:", test_df.count())


param_grid = ParamGridBuilder() \
    .addGrid(dt.impurity, ["entropy", "gini"]) \
    .addGrid(dt.maxDepth, [6, 12, 24]) \
    .addGrid(dt.maxBins, [20, 50, 100]) \
    .build()

print("\nParameter combinations:", len(param_grid))
print("TrainValidationSplit trainRatio: 0.8")
print("Total model trainings:", len(param_grid))



evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)


tvs = TrainValidationSplit(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    trainRatio=0.8,
    seed=42,
    parallelism=2
)


print("\n=== TRAINING FULL TRAINVALIDATIONSPLIT ===")

tvs_model = tvs.fit(train_df)

training_runtime = time.time() - start

print("\n=== PREDICTING TEST DATA ===")

predictions = tvs_model.transform(test_df).cache()


tp = predictions.filter((col("label") == 1.0) & (col("prediction") == 1.0)).count()
tn = predictions.filter((col("label") == 0.0) & (col("prediction") == 0.0)).count()
fp = predictions.filter((col("label") == 0.0) & (col("prediction") == 1.0)).count()
fn = predictions.filter((col("label") == 1.0) & (col("prediction") == 0.0)).count()

precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
accuracy = (tp + tn) / (tp + tn + fp + fn)


score_and_labels = predictions.select("probability", "label") \
    .rdd.map(lambda row: (float(row["probability"][1]), float(row["label"])))

metrics = BinaryClassificationMetrics(score_and_labels)

best_model = tvs_model.bestModel
best_dt = best_model.stages[-1]


print("\n=== TRAINVALIDATIONSPLIT RESULTS ===")

print("Best impurity:", best_dt.getImpurity())
print("Best maxDepth:", best_dt.getMaxDepth())
print("Best maxBins:", best_dt.getMaxBins())

print("\nTP:", tp)
print("TN:", tn)
print("FP:", fp)
print("FN:", fn)

print("Precision:", precision)
print("Recall:", recall)
print("Accuracy:", accuracy)
print("AUC ROC:", metrics.areaUnderROC)
print("AUC PR:", metrics.areaUnderPR)

print("\nTraining runtime seconds:", training_runtime)

total_runtime = time.time() - start
print("Total runtime seconds:", total_runtime)

print("\nSaving best model to best_decision_tree_trainvalidationsplit_model")

best_model.write().overwrite().save("best_decision_tree_trainvalidationsplit_model")


print("\nTrainValidationSplit completed successfully.")

spark.stop()