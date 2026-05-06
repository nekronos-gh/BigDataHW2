import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object Problem3 {

  // Local AUC helper
  def localAUC(pairs: Array[(Double, Double)]): Double = {
    if (pairs.isEmpty) return 0.5
    val hasPos = pairs.exists(_._2 == 1.0)
    val hasNeg = pairs.exists(_._2 == 0.0)
    if (!hasPos || !hasNeg) return 0.5
    // Sort descending by score
    val sorted = pairs.sortBy(-_._1)
    var tp = 0L; var fp = 0L; var prevTp = 0L; var prevFp = 0L
    var auc = 0.0
    val totPos = pairs.count(_._2 == 1.0).toDouble
    val totNeg = pairs.count(_._2 == 0.0).toDouble
    for ((score, label) <- sorted) {
      if (label == 1.0) tp += 1 else fp += 1
      if (fp != prevFp) {
        auc += (fp - prevFp) * (tp + prevTp) / 2.0
        prevFp = fp; prevTp = tp
      }
    }
    auc += (totNeg - prevFp) * (tp + prevTp) / 2.0
    auc / (totPos * totNeg)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Problem3")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // Load the datasets
    val rawArtistAlias    = sc.textFile("./data/artist_alias.txt")
    val rawArtistData     = sc.textFile("./data/artist_data.txt")
    val rawUserArtistData = sc.textFile("./data/user_artist_data.txt")

    // Artist into tuple (id, name)
    val artistByID = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) None
      else try { Some((id.toInt, name.trim)) }
           catch { case _: NumberFormatException => None }
    }.collectAsMap()

    // Transform aliases to tuple (id, id)    
    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) None
      else Some((tokens(0).toInt, tokens(1).toInt))
    }.collectAsMap()

    val bArtistAlias = sc.broadcast(artistAlias)

    // Use MEMORY_AND_DISK_SER 
    // Transform user data to array of (id, id, count)
    val userArtistData = rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }.repartition(128).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Filter: users with >= 100 distinct artists
    val activeUserIDs = userArtistData
      .map(x => (x.user, x.product)).distinct()
      .mapValues(_ => 1).reduceByKey(_ + _)
      .filter(_._2 >= 100)
      .map { case (user, _) => (user, true) }

    val trainData100 = userArtistData
      .map(r => (r.user, r))
      .join(activeUserIDs)
      .map { case (_, (rating, _)) => rating }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 80/20 split per user
    val splitData = trainData100.map(r => (r.user, r))
      .groupByKey()
      .map { case (user, ratings) =>
        val rnd      = new scala.util.Random(42L)
        val shuffled = rnd.shuffle(ratings.toSeq)
        val split    = math.max(1, (shuffled.size * 0.8).toInt)
        shuffled.splitAt(split)
      }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val trainData = splitData.flatMap(_._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val testData  = splitData.flatMap(_._2).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Broadcast lookup tables 
    val actualArtistsPerUser = trainData100
      .map(r => (r.user, r.product))
      .groupByKey().mapValues(_.toSet).collectAsMap()
    val testArtistsPerUser = testData
      .map(r => (r.user, r.product))
      .groupByKey().mapValues(_.toSet).collectAsMap()
    val bTestArtists = sc.broadcast(testArtistsPerUser)

    val testUserIDs = testData.map(_.user).distinct().map(id => (id, true))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Baseline: Most-Popular
    val artistsTotalCount = userArtistData
      .map(r => (r.product, r.rating.toDouble))
      .reduceByKey(_ + _).sortBy(_._2, ascending = false).collect()

    def predictMostPopular(user: Int, n: Int): Array[Rating] =
      artistsTotalCount.take(n).map { case (a, r) => Rating(user, a, r) }

    // Local AUC per user (no sc.parallelize!)
    def predsAndLabels(userID: Int, recs: Array[Rating]): Array[(Double, Double)] = {
      val testArts = bTestArtists.value.getOrElse(userID, Set.empty[Int])
      recs.map(r => (r.rating.toDouble, if (testArts.contains(r.product)) 1.0 else 0.0))
    }

    val testUserArr = testUserIDs.map(_._1).collect()

    val baselineAUCs = testUserArr.flatMap { uid =>
      val pal = predsAndLabels(uid, predictMostPopular(uid, 50))
      if (pal.exists(_._2 == 1.0) && pal.exists(_._2 == 0.0)) Some(localAUC(pal))
      else None
    }
    val baselineValue = if (baselineAUCs.nonEmpty) baselineAUCs.sum / baselineAUCs.length else 0.0
    println(s"Baseline: Average AUC (Most Popular): $baselineValue")

    // Hyper-parameter grid search
    val ranks   = Array(10, 25, 50)
    val lambdas = Array(1.0, 0.1, 0.01)
    val alphas  = Array(1.0, 10.0, 100.0)
    val numFolds   = 5   
    val iterations = 5 

    // Pre-split folds once
    val indexed = trainData.zipWithIndex().map {
      case (r, idx) => (idx % numFolds, r)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val folds: Array[(RDD[Rating], RDD[Rating])] = (0 until numFolds).map { fold =>
      val tr = indexed.filter(_._1 != fold).map(_._2).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val vl = indexed.filter(_._1 == fold).map(_._2).persist(StorageLevel.MEMORY_AND_DISK_SER)
      (tr, vl)
    }.toArray

    // Fast fold-AUC
    def computeFoldAUC(model: MatrixFactorizationModel, valid: RDD[Rating]): Double = {
      val validPerUser = valid.map(r => (r.user, r.product))
        .groupByKey().collectAsMap()

        val validUserRDD = sc.parallelize(validPerUser.keys.toSeq).map(uid => (uid, true))

        val predsLabels = model.recommendProductsForUsers(50)
          .join(validUserRDD)
          .flatMap { case (uid, (recs, _)) =>
            val posSet = validPerUser.getOrElse(uid, Iterable.empty).toSet
            recs.map(r => (r.rating.toDouble, if (posSet.contains(r.product)) 1.0 else 0.0))
          }.collect()

          localAUC(predsLabels)
    }

    // Grid search
    val grid = for { r <- ranks; l <- lambdas; a <- alphas } yield (r, l, a)

    val results = grid.map { case (rank, lambda, alpha) =>
      val foldAUCs = folds.map { case (tr, vl) =>
        val model = ALS.trainImplicit(tr, rank, iterations, lambda, alpha)
        computeFoldAUC(model, vl)
      }
      val avgAUC = foldAUCs.sum / numFolds
      println(s"rank=$rank lambda=$lambda alpha=$alpha  CV-AUC=$avgAUC")
      ((rank, lambda, alpha), avgAUC)
    }

    val best       = results.maxBy(_._2)
    val bestRank   = best._1._1
    val bestLambda = best._1._2
    val bestAlpha  = best._1._3
    val bestAUC    = best._2

    println(s"\nBEST HYPERPARAMETERS: rank=$bestRank lambda=$bestLambda alpha=$bestAlpha  CV-AUC=$bestAUC")

    // Final model on full trainData
    val bestModel = ALS.trainImplicit(trainData, bestRank, iterations, bestLambda, bestAlpha)

    // Evaluation on testData — batch predict, local metrics
    val recommendations = bestModel.recommendProductsForUsers(50)
      .join(testUserIDs).map { case (uid, (r, _)) => (uid, r) }

    val perUser = recommendations.collect().flatMap { case (uid, recs) =>
      val testArts = bTestArtists.value.getOrElse(uid, Set.empty[Int])
      val pal      = predsAndLabels(uid, recs)
      if (pal.exists(_._2 == 1.0) && pal.exists(_._2 == 0.0)) {
        val auc       = localAUC(pal)
        val tp        = recs.count(r => testArts.contains(r.product)).toDouble
        val fp        = recs.length - tp
        val fn        = testArts.size - tp
        val precision = if (tp + fp > 0) tp / (tp + fp) else 0.0
        val recall    = if (tp + fn > 0) tp / (tp + fn) else 0.0
        val accuracy  = if (tp + fp + fn > 0) tp / (tp + fp + fn) else 0.0
        Some((auc, precision, recall, accuracy))
      } else None
    }

    val n = perUser.length.toDouble
    println(f"\nTest-set metrics (best model):")
    println(f"AUC:       ${perUser.map(_._1).sum / n}%.4f")
    println(f"Precision: ${perUser.map(_._2).sum / n}%.4f")
    println(f"Recall:    ${perUser.map(_._3).sum / n}%.4f")
    println(f"Accuracy:  ${perUser.map(_._4).sum / n}%.4f")

    // New synthetic user 
    val maxUserID = userArtistData.map(_.user).max()
    val newUserID = maxUserID + 1

    val existingArtistIDs = userArtistData.map(_.product).distinct()
      .takeSample(withReplacement = false, 8, seed = 42)

    // Create fake values
    val newUserRatings = sc.parallelize(
      existingArtistIDs.zipWithIndex.map { case (artistID, i) =>
        Rating(newUserID, artistID, (5000 - i * 300).toInt)
      }
    )

    // Include values in data
    val updatedTrainData = trainData.union(newUserRatings).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val newModel         = ALS.trainImplicit(updatedTrainData, bestRank, iterations, bestLambda, bestAlpha)
    val ratedSet         = newUserRatings.map(_.product).collect().toSet

    // Get recommendations
    val top25 = newModel.recommendProducts(newUserID, 100)
      .filter(r => !ratedSet.contains(r.product)).take(25)

    println(s"\nTop-25 recommendations for new user $newUserID:")
    top25.zipWithIndex.foreach { case (r, i) =>
      val name = artistByID.getOrElse(r.product, s"Unknown(${r.product})")
      println(f"${i+1}%2d. $name (artistID=${r.product}, score=${r.rating}%.4f)")
    }

    spark.stop()
  }
}
