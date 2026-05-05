import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext

object Problem3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Problem3")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // Load the datasets
    val rawArtistAlias = sc.textFile("./data/artist_alias.txt")
    val rawArtistData = sc.textFile("./data/artist_data.txt")
    val rawUserArtistData = sc.textFile("./data/user_artist_data.txt")

    // Transform artists to tuple (id, name)
    val artistByID = rawArtistData.flatMap { 
      line => 
        val (id, name) = line.span(_ != '\t')
        if (name.isEmpty) { 
          None 
        } else { 
          try { Some((id.toInt, name.trim)) } 
          catch { case e: NumberFormatException => None } 
        } 
    }.collectAsMap()

    // Transform aliases to tuple (id, id)
    val artistAlias = rawArtistAlias.flatMap { 
      line => 
        val tokens = line.split('\t') 
        if (tokens(0).isEmpty) { 
          None 
        } else {
          Some((tokens(0).toInt, tokens(1).toInt)) 
        } 
    }.collectAsMap()
    // Broadcast the local aliases 
    val bArtistAlias = sc.broadcast(artistAlias)

    // Transform user data to array of (id, id, count)
    val userArtistData = rawUserArtistData.map { 
      line =>
        val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
        val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
        Rating(userID, finalArtistID, count)
    }.repartition(100).cache()

    // Filter by users that listen to at least 100 distinct artists
    val activeUserIDs = userArtistData
      .map(x => (x.user, x.product))
      .distinct()
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .filter(x => x._2 >= 100)
      .map { case (user, _) => (user, true) }

    // Join by user ID
    val trainData100 = userArtistData
      .map(r => (r.user, r))
      .join(activeUserIDs)
      .map { case (_, (rating, _)) => rating }

    // Split 80/20 per user — preserve same users in both splits
    val splitData = trainData100.map(r => (r.user, r))
      .groupByKey()
      .map { case (user, ratings) =>
        val rnd = new scala.util.Random(42L)
        val shuffled = rnd.shuffle(ratings.toSeq)
        val splitIndex = math.max(1, (shuffled.size * 0.8).toInt)
        shuffled.splitAt(splitIndex)
      }.cache()

    val trainData = splitData.flatMap(_._1).cache()
    val testData = splitData.flatMap(_._2).cache()

    // Set of artists per user from trainData100 (scoped to active users)
    // Used to check whether a recommended artist was ever listened to by the user
    val actualArtistsPerUser = trainData100
      .map(r => (r.user, r.product))
      .groupByKey()
      .mapValues(_.toSet)
      .collectAsMap()

    val bActualArtists = sc.broadcast(actualArtistsPerUser)

    // Artists listened to in the TEST split per user (ground truth for evaluation)
    val testArtistsPerUser = testData
      .map(r => (r.user, r.product))
      .groupByKey()
      .mapValues(_.toSet)
      .collectAsMap()

    val bTestArtists = sc.broadcast(testArtistsPerUser)

    val testUserIDs = testData.map(_.user).distinct().map(id => (id, true))

    // All distinct artist IDs (for negative sampling)
    val allArtistIDs = userArtistData.map(_.product).distinct().collect()
    val bAllArtistIDs = sc.broadcast(allArtistIDs)

    // Calculate AUC for Most Popular as a baseline
    val artistsTotalCount = userArtistData
      .map(r => (r.product, r.rating.toDouble))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()

    def predictMostPopular(user: Int, numArtists: Int) = {
      val topArtists = artistsTotalCount.take(numArtists)
      topArtists.map { case (artist, rating) => Rating(user, artist, rating) }
    }

    // Helper: build predictionsAndLabels for a user given a list of recommended Ratings.
    // Positives = artists the user listened to in testData; negatives come from the recs list.
    def predsAndLabels(userID: Int, recs: Array[Rating]): Array[(Double, Double)] = {
      val testArtists = bTestArtists.value.getOrElse(userID, Set.empty[Int])
      recs.map { r =>
        (r.rating.toDouble, if (testArtists.contains(r.product)) 1.0 else 0.0)
      }
    }

    val baselineAUC = testUserIDs.map(_._1).collect().flatMap { userID =>
      val pal = predsAndLabels(userID, predictMostPopular(userID, 50))
      // Need at least one positive and one negative for AUC to be defined
      if (pal.exists(_._2 == 1.0) && pal.exists(_._2 == 0.0)) {
        val metrics = new BinaryClassificationMetrics(sc.parallelize(pal))
        Some(metrics.areaUnderROC())
      } else None
    }
    val baselineValue = if (baselineAUC.nonEmpty) baselineAUC.sum / baselineAUC.length.toDouble else 0.0
    println(s"Baseline: Average AUC (Most Popular): $baselineValue")

    // Train the recommender model
    // Hyperparameters
    val ranks = Array(10, 25, 50)
    val lambdas = Array(1.0, 0.1, 0.01)
    val alphas = Array(1.0, 10.0, 100.0)
    val numFolds = 3
    val iterations = 5

    val ratings = trainData

    // Fold assignment
    val indexed = ratings.zipWithIndex().map {
      case (r, idx) => (idx % numFolds, r)
    }.cache()

    // Pre-split folds once
    val folds: Array[(RDD[Rating], RDD[Rating])] = (0 until numFolds).map { fold =>
      val train = indexed.filter(_._1 != fold).map(_._2).cache()
      val valid = indexed.filter(_._1 == fold).map(_._2).cache()
      (train, valid)
    }.toArray

    // AUC helper: needs both positives and negatives.
    // For cross-validation we use validation-set artists as positives,
    // and a sample of non-listened artists as negatives.
    def computeFoldAUC(
        model: MatrixFactorizationModel,
        valid: RDD[Rating],
        sc: SparkContext): Double = {

      // Collect validation entries per user
      val validPerUser = valid.map(r => (r.user, r.product)).groupByKey().collectAsMap()

      // For each user in validation: predict on their positive artists + equal number of negatives
      val predsLabels: Array[(Double, Double)] = validPerUser.toArray.flatMap { case (user, posArtists) =>
        val posSet    = posArtists.toSet
        val trainSeen = bActualArtists.value.getOrElse(user, Set.empty[Int])
        val allArts   = bAllArtistIDs.value
        // Sample negatives: artists not seen by user at all
        val negArtists = allArts
          .filterNot(a => trainSeen.contains(a) || posSet.contains(a))
          .take(posSet.size)

        val candidates = posSet.toArray.map((user, _, true)) ++
                         negArtists.map((user, _, false))

        if (candidates.isEmpty) Array.empty[(Double, Double)]
        else {
          val pairsRDD = sc.parallelize(candidates.map { case (u, a, _) => (u, a) })
          val preds    = model.predict(pairsRDD).map(r => ((r.user, r.product), r.rating)).collectAsMap()
          candidates.flatMap { case (u, a, isPos) =>
            preds.get((u, a)).map(score => (score, if (isPos) 1.0 else 0.0))
          }
        }
      }

      if (predsLabels.exists(_._2 == 1.0) && predsLabels.exists(_._2 == 0.0)) {
        val metrics = new BinaryClassificationMetrics(sc.parallelize(predsLabels))
        metrics.areaUnderROC()
      } else 0.5
    }

    // Triple loop over hyperparameter grid
    val grid = for {
      r <- ranks
      l <- lambdas
      a <- alphas
    } yield (r, l, a)

    val results = grid.map { case (rank, lambda, alpha) =>
      val foldAUCs = folds.map { case (train, valid) =>
        val model = ALS.trainImplicit(train, rank, iterations, lambda, alpha)
        computeFoldAUC(model, valid, sc)
      }
      val avgAUC = foldAUCs.sum / numFolds
      ((rank, lambda, alpha), avgAUC)
    }

    val best      = results.maxBy(_._2)
    val bestRank   = best._1._1
    val bestLambda = best._1._2
    val bestAlpha  = best._1._3
    val bestAUC    = best._2

    println("\nBEST HYPERPARAMETERS:")
    println(s"rank=${bestRank}, lambda=${bestLambda}, alpha=${bestAlpha}")
    println(s"CV AUC=${bestAUC}")

    // Train the final model with the best hyper parameters
    val bestModel = ALS.trainImplicit(ratings, bestRank, iterations, bestLambda, bestAlpha)

    // Calculate all evaluation metrics over testData users
    val recommendations = bestModel.recommendProductsForUsers(50)
      .join(testUserIDs).map { case (uid, (r, _)) => (uid, r) }

    val perUser = recommendations.collect().flatMap { case (uid, recs) =>
      val testArtists = bTestArtists.value.getOrElse(uid, Set.empty[Int])
      val pal = predsAndLabels(uid, recs)
      if (pal.exists(_._2 == 1.0) && pal.exists(_._2 == 0.0)) {
        val auc       = new BinaryClassificationMetrics(sc.parallelize(pal)).areaUnderROC()
        val tp        = recs.count(r => testArtists.contains(r.product)).toDouble
        val fp        = recs.length - tp
        val fn        = testArtists.size - tp
        val precision = if (tp + fp > 0) tp / (tp + fp) else 0.0
        val recall    = if (tp + fn > 0) tp / (tp + fn) else 0.0
        val accuracy  = if (tp + fp + fn > 0) tp / (tp + fp + fn) else 0.0
        Some((auc, precision, recall, accuracy))
      } else None
    }

    val n = perUser.length.toDouble
    println(f"AUC:       ${perUser.map(_._1).sum / n}%.4f")
    println(f"Precision: ${perUser.map(_._2).sum / n}%.4f")
    println(f"Recall:    ${perUser.map(_._3).sum / n}%.4f")
    println(f"Accuracy:  ${perUser.map(_._4).sum / n}%.4f")

    // New synthetic user profile
    val maxUserID = userArtistData.map(_.user).max()
    val newUserID = maxUserID + 1

    // 8 ratings for existing artist IDs
    val existingArtistIDs = userArtistData
      .map(_.product)
      .distinct()
      .takeSample(withReplacement = false, 8, seed = 42)

    val newUserRatings = sc.parallelize(
      existingArtistIDs.zipWithIndex.map { case (artistID, i) =>
        val score = 5000.0 - i * 300
        Rating(newUserID, artistID, score.toInt)
      }
    )

    // Now retrain with the new data
    val updatedTrainData = trainData.union(newUserRatings).cache()
    val newModel = ALS.trainImplicit(updatedTrainData, bestRank, iterations, bestLambda, bestAlpha)
    val ratedArtistIDs = newUserRatings.map(_.product).collect().toSet

    // Print recommendations for new user
    val top25ForNewUser = newModel
      .recommendProducts(newUserID, 100)
      .filter(r => !ratedArtistIDs.contains(r.product))
      .take(25)
    println(s"\nTop-25 recommendations for new user $newUserID:")
    top25ForNewUser.zipWithIndex.foreach { case (r, i) =>
      val artistName = artistByID.getOrElse(r.product, s"Unknown artist ${r.product}")
      println(f"${i + 1}%2d. $artistName%s (artistID=${r.product}, score=${r.rating}%.4f)")
    }
  }
}
