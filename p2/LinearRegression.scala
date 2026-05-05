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