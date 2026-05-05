from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, VectorIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
import sys
import time


# ------------------------------------------------------------
# Input argument
# ------------------------------------------------------------

if len(sys.argv) < 2:
    print("Usage: spark-submit Problem1_cv_full.py /path/to/heart_2020_cleaned.csv")
    sys.exit(1)

input_path = sys.argv[1]


# ------------------------------------------------------------
# Start Spark
# ------------------------------------------------------------

spark = SparkSession.builder \
    .appName("Problem1DecisionTreeCrossValidatorFull") \
    .getOrCreate()

# Reduce Spark log output
spark.sparkContext.setLogLevel("ERROR")


# ------------------------------------------------------------
# Runtime timer
# ------------------------------------------------------------

start = time.time()


# ------------------------------------------------------------
# Load full dataset
# ------------------------------------------------------------

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

# Repartition and cache, similar idea to lecture code
df = df.repartition(8).cache()

print("\n=== FULL DATA LOADED ===")
print("Rows:", df.count())
print("Columns:", len(df.columns))
print("Column names:", df.columns)


# ------------------------------------------------------------
# Define columns
# ------------------------------------------------------------

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


# ------------------------------------------------------------
# Preprocessing stages
# ------------------------------------------------------------

# Feature categorical columns may use handleInvalid="keep"
indexers = [
    StringIndexer(
        inputCol=c,
        outputCol=c + "_idx",
        handleInvalid="keep"
    )
    for c in categorical_cols
]

# Important:
# For the label, do NOT use handleInvalid="keep".
# Otherwise Spark creates a third possible class and BinaryClassificationEvaluator fails.
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


# ------------------------------------------------------------
# Decision Tree classifier
# ------------------------------------------------------------

dt = DecisionTreeClassifier(
    labelCol="label",
    featuresCol="features",
    seed=42
)


# ------------------------------------------------------------
# ML Pipeline
# ------------------------------------------------------------

pipeline = Pipeline(
    stages=indexers + [
        label_indexer,
        assembler,
        vector_indexer,
        dt
    ]
)


# ------------------------------------------------------------
# 80% / 20% train-test split
# ------------------------------------------------------------

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

train_df = train_df.cache()
test_df = test_df.cache()

print("\n=== SPLIT ===")
print("Training rows:", train_df.count())
print("Testing rows:", test_df.count())


# ------------------------------------------------------------
# Full hyperparameter grid required by the exercise
# ------------------------------------------------------------

param_grid = ParamGridBuilder() \
    .addGrid(dt.impurity, ["entropy", "gini"]) \
    .addGrid(dt.maxDepth, [6, 12, 24]) \
    .addGrid(dt.maxBins, [20, 50, 100]) \
    .build()

print("\nParameter combinations:", len(param_grid))
print("With 5 folds, total model trainings:", len(param_grid) * 5)


# ------------------------------------------------------------
# Evaluator for CrossValidator
# ------------------------------------------------------------

# BinaryClassificationEvaluator is the correct Spark ML evaluator for CrossValidator.
# BinaryClassificationMetrics is used afterwards for detailed final reporting.
evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)


# ------------------------------------------------------------
# CrossValidator
# ------------------------------------------------------------

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=5,
    seed=42,
    parallelism=2
)


# ------------------------------------------------------------
# Train CrossValidator
# ------------------------------------------------------------

print("\n=== TRAINING FULL CROSSVALIDATOR ===")

cv_model = cv.fit(train_df)

training_runtime = time.time() - start


# ------------------------------------------------------------
# Predict test data
# ------------------------------------------------------------

print("\n=== PREDICTING TEST DATA ===")

predictions = cv_model.transform(test_df).cache()


# ------------------------------------------------------------
# Compute confusion matrix manually
# ------------------------------------------------------------

tp = predictions.filter((col("label") == 1.0) & (col("prediction") == 1.0)).count()
tn = predictions.filter((col("label") == 0.0) & (col("prediction") == 0.0)).count()
fp = predictions.filter((col("label") == 0.0) & (col("prediction") == 1.0)).count()
fn = predictions.filter((col("label") == 1.0) & (col("prediction") == 0.0)).count()

precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
accuracy = (tp + tn) / (tp + tn + fp + fn)


# ------------------------------------------------------------
# BinaryClassificationMetrics
# ------------------------------------------------------------

score_and_labels = predictions.select("probability", "label") \
    .rdd.map(lambda row: (float(row["probability"][1]), float(row["label"])))

metrics = BinaryClassificationMetrics(score_and_labels)


# ------------------------------------------------------------
# Extract best model and best parameters
# ------------------------------------------------------------

best_model = cv_model.bestModel
best_dt = best_model.stages[-1]


# ------------------------------------------------------------
# Print results
# ------------------------------------------------------------

print("\n=== FULL CROSSVALIDATOR RESULTS ===")

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


# ------------------------------------------------------------
# Save best model
# ------------------------------------------------------------

print("\nSaving best model to best_decision_tree_crossvalidator_model")

best_model.write().overwrite().save("best_decision_tree_crossvalidator_model")


print("\nFull CrossValidator completed successfully.")


# ------------------------------------------------------------
# Stop Spark
# ------------------------------------------------------------

spark.stop()