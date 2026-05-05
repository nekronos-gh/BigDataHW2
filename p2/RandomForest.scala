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