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

    // Filter by users that listen to 
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

    // Split 80/20 per user
    val splitData = trainData100.map(r => (r.user, r))
      .groupByKey()
      .map { case (user, ratings) =>
        val rnd = new scala.util.Random(42L)
        val shuffled = rnd.shuffle(ratings.toSeq)
        val splitIndex = (shuffled.size * 0.8).toInt
        shuffled.splitAt(splitIndex)
      }.cache()

    val trainData = splitData.flatMap(_._1).cache()
    val testData = splitData.flatMap(_._2).cache()

    // Set of artists per user
    val actualArtistsPerUser = userArtistData
      .map(r => (r.user, r.product))
      .groupByKey()
      .mapValues(_.toSet)
      .collectAsMap()

    val bActualArtists = sc.broadcast(actualArtistsPerUser)
    val testUserIDs = testData.map(_.user).distinct().map(id => (id, true))

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
    val baselineAUC = testUserIDs.map(_._1).map { userID =>
      val actualArtists = bActualArtists.value.getOrElse(userID, Set.empty[Int])
      val predsAndLabels = predictMostPopular(userID, 50).map { r =>
        (r.rating, if (actualArtists.contains(r.product)) 1.0 else 0.0)
      }
      val metrics = new BinaryClassificationMetrics(sc.parallelize(predsAndLabels))
      metrics.areaUnderROC()
    }
    println(s"Baseline: Average AUC (Most Popular): ${baselineAUC.mean()}")

    // Train the recommender model
    // Hyperparameters
    val ranks   = Array(10, 25, 50)
    val lambdas = Array(1.0, 0.1, 0.01)
    val alphas  = Array(1.0, 10.0, 100.0)
    val numFolds   = 5
    val iterations = 5

    var bestAUC    = Double.MinValue
    var bestRank   = 0
    var bestLambda = 0.0
    var bestAlpha  = 0.0

    val indexedTrain = trainData.zipWithIndex().cache()

    // Hyperparameter triple loop
    for (rank <- ranks; lambda <- lambdas; alpha <- alphas) {
      // 5-fold cross validation
      val cvAUCs = (0 until numFolds).map { fold =>
        val cvTrain = indexedTrain.filter(_._2 % numFolds != fold).map(_._1)
        val cvVal   = indexedTrain.filter(_._2 % numFolds == fold).map(_._1)
        val cvModel = ALS.trainImplicit(cvTrain, rank, iterations, lambda, alpha)
        val testUserIDs = cvVal.map(_.user).distinct().map(id => (id, true))
        val recs = cvModel.recommendProductsForUsers(50)
          .join(testUserIDs).map { case (uid, (r, _)) => (uid, r) }
        recs.map { case (uid, ratings) =>
          val actual = bActualArtists.value.getOrElse(uid, Set.empty[Int])
          val pal = ratings.map(r => (r.rating, if (actual.contains(r.product)) 1.0 else 0.0))
          new BinaryClassificationMetrics(sc.parallelize(pal)).areaUnderROC()
        }.mean()
      }
      val meanAUC = cvAUCs.sum / numFolds
      if (meanAUC > bestAUC) { bestAUC = meanAUC; bestRank = rank; bestLambda = lambda; bestAlpha = alpha }
    }

    // Train the final model with the best hyper parameters
    val model = ALS.trainImplicit(trainData, bestRank, iterations, bestLambda, bestAlpha)
    val recommendations = model.recommendProductsForUsers(50)
      .join(testUserIDs).map { case (uid, (r, _)) => (uid, r) }

    // Calculate all evaluation metrics
    val perUser = recommendations.map { case (uid, ratings) =>
      val actual = bActualArtists.value.getOrElse(uid, Set.empty[Int])
      val pal = ratings.map(r => (r.rating, if (actual.contains(r.product)) 1.0 else 0.0))
      val auc = new BinaryClassificationMetrics(sc.parallelize(pal)).areaUnderROC()
      val tp = ratings.count(r => actual.contains(r.product)).toDouble
      val fp = ratings.length - tp
      val fn = actual.size - tp
      val precision = if (tp+fp > 0) tp/(tp+fp) else 0.0
      val recall    = if (tp+fn > 0) tp/(tp+fn) else 0.0
      val accuracy  = if (tp+fp+fn > 0) tp/(tp+fp+fn) else 0.0
      (auc, precision, recall, accuracy)
    }.cache()

    val n = perUser.count().toDouble
    println("BEST MODEL")
    println(s"Best params: rank=$bestRank lambda=$bestLambda alpha=$bestAlpha CV-AUC=$bestAUC")
    println(f"AUC:       ${perUser.map(_._1).sum/n}%.4f")
    println(f"Precision: ${perUser.map(_._2).sum/n}%.4f")
    println(f"Recall:    ${perUser.map(_._3).sum/n}%.4f")
    println(f"Accuracy:  ${perUser.map(_._4).sum/n}%.4f")

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
        Rating(newUserID, artistID, score)
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
