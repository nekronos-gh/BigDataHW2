from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, VectorIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
import sys
import time


if len(sys.argv) < 2:
    print("Usage: spark-submit Problem1_rf_age_full.py /path/to/heart_2020_cleaned.csv")
    sys.exit(1)

input_path = sys.argv[1]



spark = SparkSession.builder \
    .appName("Problem1RandomForestAgeCategoryFull") \
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

label_col = "AgeCategory"

categorical_cols = [
    "HeartDisease",
    "Smoking",
    "AlcoholDrinking",
    "Stroke",
    "DiffWalking",
    "Sex",
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

# For AgeCategory, use normal label indexing.
# Do not use handleInvalid="keep" for the target label.
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



rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    featureSubsetStrategy="auto",
    seed=42
)


pipeline = Pipeline(
    stages=indexers + [
        label_indexer,
        assembler,
        vector_indexer,
        rf
    ]
)



train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

train_df = train_df.cache()
test_df = test_df.cache()

print("\n=== SPLIT ===")
print("Training rows:", train_df.count())
print("Testing rows:", test_df.count())


param_grid = ParamGridBuilder() \
    .addGrid(rf.impurity, ["entropy", "gini"]) \
    .addGrid(rf.maxDepth, [6, 12, 24]) \
    .addGrid(rf.maxBins, [20, 50, 100]) \
    .addGrid(rf.numTrees, [5, 10, 20]) \
    .build()

print("\nParameter combinations:", len(param_grid))
print("With 5 folds, total model trainings:", len(param_grid) * 5)


evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)


cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=5,
    seed=42,
    parallelism=2
)


print("\n=== TRAINING RANDOM FOREST CROSSVALIDATOR ===")

cv_model = cv.fit(train_df)

training_runtime = time.time() - start

print("\n=== PREDICTING TEST DATA ===")

predictions = cv_model.transform(test_df).cache()


prediction_and_labels = predictions.select("prediction", "label") \
    .rdd.map(lambda row: (float(row["prediction"]), float(row["label"])))

metrics = MulticlassMetrics(prediction_and_labels)

accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

weighted_precision_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

weighted_recall_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedRecall"
)

accuracy = accuracy_evaluator.evaluate(predictions)
f1 = f1_evaluator.evaluate(predictions)
weighted_precision = weighted_precision_evaluator.evaluate(predictions)
weighted_recall = weighted_recall_evaluator.evaluate(predictions)



best_model = cv_model.bestModel
best_rf = best_model.stages[-1]


age_indexer_model = best_model.stages[len(indexers)]

print("\n=== AGE CATEGORY LABEL MAPPING ===")
for idx, label in enumerate(age_indexer_model.labels):
    print(str(idx) + " -> " + label)


print("\n=== RANDOM FOREST AGECATEGORY RESULTS ===")

print("Best impurity:", best_rf.getImpurity())
print("Best maxDepth:", best_rf.getMaxDepth())
print("Best maxBins:", best_rf.getMaxBins())
print("Best numTrees:", best_rf.getNumTrees())

print("\nAccuracy:", accuracy)
print("F1:", f1)
print("Weighted precision:", weighted_precision)
print("Weighted recall:", weighted_recall)

print("\nConfusion matrix:")
print(metrics.confusionMatrix().toArray())

print("\nTraining runtime seconds:", training_runtime)

total_runtime = time.time() - start
print("Total runtime seconds:", total_runtime)

print("\nSaving best model to best_random_forest_agecategory_model")

best_model.write().overwrite().save("best_random_forest_agecategory_model")

print("\nRandomForest AgeCategory CrossValidator completed successfully.")
spark.stop()