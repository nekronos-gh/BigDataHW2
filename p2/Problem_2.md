# Problem 2: Weather Prediction via Linear Regression & Regression Trees


## Part A: Loading the NOAA dataset

To download the dataset, we use the `downloader.sh` bash script (takes 15min to download). 

Run 

```sh
sbatch downloader.sh
```
or 

```sh
bash downloader.sh
```

If you are trying to run it on the HPC:

```sh
module load env/development/2024a devel/Spark
spark-shell
```

Then load the parseNOAA.scala:

```scala
:load parseNOAA.scala
```

***NOTE*** Please run the downloader.sh script directly in the p2 directory of this homework, as scala won't find the files otherwise

## Part B: LinearRegression

```scala
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, lit, sin, cos}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

def toD(x: Any): Double = x match {
  case n: java.lang.Integer => n.toDouble
  case n: java.lang.Long    => n.toDouble
  case n: java.lang.Float   => n.toDouble
  case n: java.lang.Double  => n
  case n: java.lang.Short   => n.toDouble
  case n: java.lang.Byte    => n.toDouble
  case n: Number            => n.doubleValue()
  case s: String            => s.toDouble
  case _ => throw new IllegalArgumentException(s"Cannot convert '${x}' (${x.getClass}) to Double")
}

val schemaLR = StructType(Seq(
  StructField("year", DoubleType, false),
  StructField("month", DoubleType, false),
  StructField("day", DoubleType, false),
  StructField("hour", DoubleType, false),
  StructField("latitude", DoubleType, false),
  StructField("longitude", DoubleType, false),
  StructField("elevationDimension", DoubleType, false),
  StructField("directionAngle", DoubleType, false),
  StructField("speedRate", DoubleType, false),
  StructField("ceilingHeightDimension", DoubleType, false),
  StructField("distanceDimension", DoubleType, false),
  StructField("dewPointTemperature", DoubleType, false),
  StructField("airTemperature", DoubleType, false)
))

val rowsLR = parsedNOAA.map(a => Row(
  toD(a(0)), toD(a(1)), toD(a(2)), toD(a(3)),
  toD(a(4)), toD(a(5)), toD(a(6)), toD(a(7)),
  toD(a(8)), toD(a(9)), toD(a(10)), toD(a(11)), toD(a(12))
))

val dfBaseLR = spark.createDataFrame(rowsLR, schemaLR)

val dfLR_1 = dfBaseLR.withColumn("month_sin", sin(col("month") * lit(2.0 * math.Pi / 12.0)))
val dfLR_2 = dfLR_1.withColumn("month_cos", cos(col("month") * lit(2.0 * math.Pi / 12.0)))
val dfLR_3 = dfLR_2.withColumn("hour_sin",  sin(col("hour") * lit(2.0 * math.Pi / 24.0)))
val dfLR   = dfLR_3.withColumn("hour_cos",  cos(col("hour") * lit(2.0 * math.Pi / 24.0)))

val trainLR = dfLR.filter(col("year").between(1949.0, 2023.0)).cache()
val validLR = dfLR.filter(col("year") === 2024.0).cache()

// sanity check
trainLR.select("month_sin", "month_cos", "hour_sin", "hour_cos").printSchema()

val featureColsLR = Array(
  "year", "day", "latitude", "longitude",
  "elevationDimension", "directionAngle", "speedRate",
  "ceilingHeightDimension", "distanceDimension", "dewPointTemperature",
  "month_sin", "month_cos", "hour_sin", "hour_cos"
)

val assemblerLR = new VectorAssembler()
  .setInputCols(featureColsLR)
  .setOutputCol("features_raw")

val scalerLR = new StandardScaler()
  .setInputCol("features_raw")
  .setOutputCol("features")
  .setWithStd(true)
  .setWithMean(true)

val lr = new LinearRegression()
  .setFeaturesCol("features")
  .setLabelCol("airTemperature")
  .setMaxIter(100)

val pipelineLR = new Pipeline().setStages(Array(assemblerLR, scalerLR, lr))

val gridBuilder = new ParamGridBuilder()
gridBuilder.addGrid(lr.regParam, Array(0.0, 0.01, 0.1))
gridBuilder.addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
val paramMapsLR: Array[org.apache.spark.ml.param.ParamMap] = gridBuilder.build()

val evalRMSE = new RegressionEvaluator()
  .setLabelCol("airTemperature")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val tvsLR = new TrainValidationSplit()
tvsLR.setEstimator(pipelineLR)
tvsLR.setEvaluator(evalRMSE)
tvsLR.setEstimatorParamMaps(paramMapsLR)
tvsLR.setTrainRatio(0.8)
tvsLR.setSeed(42L)

val tunedModelLR = tvsLR.fit(trainLR)
val predLR = tunedModelLR.transform(validLR)

val rmseEval = new RegressionEvaluator()
rmseEval.setLabelCol("airTemperature")
rmseEval.setPredictionCol("prediction")
rmseEval.setMetricName("rmse")
val rmse: Double = rmseEval.evaluate(predLR)

val maeEval = new RegressionEvaluator()
maeEval.setLabelCol("airTemperature")
maeEval.setPredictionCol("prediction")
maeEval.setMetricName("mae")
val mae: Double = maeEval.evaluate(predLR)

val r2Eval = new RegressionEvaluator()
r2Eval.setLabelCol("airTemperature")
r2Eval.setPredictionCol("prediction")
r2Eval.setMetricName("r2")
val r2: Double = r2Eval.evaluate(predLR)

println("\n== Improved LinearRegression (train: 1949-2023, validation: 2024) ==")
println(f"RMSE: $rmse%.4f")
println(f"MAE : $mae%.4f")
println(f"R2  : $r2%.4f")

val testLR = dfLR.filter(col("year") === 2025.0).cache()
val predTestLR = tunedModelLR.transform(testLR)

val mseEval = new RegressionEvaluator()
mseEval.setLabelCol("airTemperature")
mseEval.setPredictionCol("prediction")
mseEval.setMetricName("mse")
val mse: Double = mseEval.evaluate(predTestLR)

println("\n== Test Set (2025) ==")
println(f"MSE : $mse%.4f")
```

To load into `spark-shell`:

```sh
:load LinearRegression.scala
```

This model does get the following values:

```sh
== Improved LinearRegression (train: 1949-2023, validation: 2024) ==
RMSE: 3.8793
MAE : 3.1162
R2  : 0.6909
```

Run time: 28s (With srun -N2 -c12 on AION)

## Part C: RandomForest

```scala
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator

def toD(x: Any): Double = x match {
  case n: java.lang.Integer => n.toDouble
  case n: java.lang.Long    => n.toDouble
  case n: java.lang.Float   => n.toDouble
  case n: java.lang.Double  => n
  case n: java.lang.Short   => n.toDouble
  case n: java.lang.Byte    => n.toDouble
  case n: Number            => n.doubleValue()
  case s: String            => s.toDouble
  case _ => throw new IllegalArgumentException(s"Cannot convert '${x}' (${x.getClass}) to Double")
}

val schemaRF3 = StructType(Seq(
  StructField("year", DoubleType, false),
  StructField("month", DoubleType, false),
  StructField("day", DoubleType, false),
  StructField("hour", DoubleType, false),
  StructField("latitude", DoubleType, false),
  StructField("longitude", DoubleType, false),
  StructField("elevationDimension", DoubleType, false),
  StructField("directionAngle", DoubleType, false),
  StructField("speedRate", DoubleType, false),
  StructField("ceilingHeightDimension", DoubleType, false),
  StructField("distanceDimension", DoubleType, false),
  StructField("dewPointTemperature", DoubleType, false),
  StructField("airTemperature", DoubleType, false)
))

val rowsRF3 = parsedNOAA.map(a => Row(
  toD(a(0)), toD(a(1)), toD(a(2)), toD(a(3)),
  toD(a(4)), toD(a(5)), toD(a(6)), toD(a(7)),
  toD(a(8)), toD(a(9)), toD(a(10)), toD(a(11)), toD(a(12))
))

val dfRF3 = spark.createDataFrame(rowsRF3, schemaRF3)

val featureColsRF3 = Array(
  "year","month","day","hour","latitude","longitude",
  "elevationDimension","directionAngle","speedRate",
  "ceilingHeightDimension","distanceDimension","dewPointTemperature"
)

val trainRawRF3 = dfRF3.filter(col("year").between(1949.0, 2023.0)).cache()
val validRawRF3 = dfRF3.filter(col("year") === 2024.0).cache()

val assemblerRF3 = new VectorAssembler().setInputCols(featureColsRF3).setOutputCol("features")

val trainRF3 = assemblerRF3.transform(trainRawRF3)
  .withColumn("airTemperature", col("airTemperature").cast("double"))
  .cache()

val validRF3 = assemblerRF3.transform(validRawRF3)
  .withColumn("airTemperature", col("airTemperature").cast("double"))
  .cache()

trainRF3.count()
validRF3.count()
// trainRF3.printSchema()

val rf3 = new RandomForestRegressor()
  .setFeaturesCol("features")
  .setLabelCol("airTemperature")
  .setNumTrees(10)
  .setFeatureSubsetStrategy("auto")
  .setMaxDepth(8)
  .setMaxBins(32)
  .setSubsamplingRate(0.7)
  .setSeed(42L)

val modelRF3 = rf3.fit(trainRF3)
val predRF3 = modelRF3.transform(validRF3)

val rmseEvalRF3 = new RegressionEvaluator().setLabelCol("airTemperature").setPredictionCol("prediction").setMetricName("rmse")
val maeEvalRF3  = new RegressionEvaluator().setLabelCol("airTemperature").setPredictionCol("prediction").setMetricName("mae")
val r2EvalRF3   = new RegressionEvaluator().setLabelCol("airTemperature").setPredictionCol("prediction").setMetricName("r2")

val rmse: Double = rmseEvalRF3.evaluate(predRF3)
val mae: Double = maeEvalRF3.evaluate(predRF3)
val r2: Double = r2EvalRF3.evaluate(predRF3)

println("\n== RandomForestRegressor (numTrees=10, auto) ==")
println(f"RMSE: $rmse%.4f")
println(f"MAE : $mae%.4f")
println(f"R2  : $r2%.4f")
val testRawRF3 = dfRF3.filter(col("year") === 2025.0).cache()
val testRF3 = assemblerRF3.transform(testRawRF3)
    .withColumn("airTemperature", col("airTemperature").cast("double"))
    .cache()

val predTestRF3 = modelRF3.transform(testRF3)

val mseEvalRF3 = new RegressionEvaluator().setLabelCol("airTemperature").setPredictionCol("prediction").setMetricName("mse")
val mse: Double = mseEvalRF3.evaluate(predTestRF3)

println("\n== Test Set (2025) ==")
println(f"MSE : $mse%.4f")
```

To run this model, please use: 

```sh
:load RandomForest.scala
```
in spark-shell.

We obtain the following result:

```sh
== RandomForestRegressor (numTrees=10, auto) ==
RMSE: 2.2886
MAE : 1.7128
R2  : 0.8924
```

Run time: 18.36s (on AION, srun with -N2 -c12)

## Part D: Testing both regressors on the test set 2025:

When testing both regression models on the test set of the weather observations of 2025, we obtain the following results:

| Metric | LinearRegression | Random Forest |
| --- | --- | --- |
| **MSE** | 16.0840 | 9.0451 |

Hence, we can see, that the Random forest works better on the dataset and is able to predict the air temperature better.

## Part E: Which attribute may have the highest correlation

The attributes with the highest correlation to the air temperature should be the date, the time and the wind direction. As the location is fixed to Luxembourg, generally winters are colder than summers (generally, winter months lower temperature than summer months). During the day (sun light) it is also generally warmer, with a general temperature peak around 3-4pm.

In Luxembourg, one major factor is also the wind, as easterly winds usually strengthen the effect of the season a lot, as eastern winds cool down the air in winter, while western winds bring warmer air from the gulf stream, and in summer we have the opposing effect. 

