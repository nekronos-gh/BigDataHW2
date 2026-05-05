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

    // Train the recommender model
    val rank = 10
    val iterations = 5
    val lambda = 0.01
    val alpha = 1.0
    val model = ALS.trainImplicit(trainData, rank, iterations, lambda, alpha)

    val recommendations = model.recommendProductsForUsers(50)

    // Set of artists per user
    val actualArtistsPerUser = userArtistData
      .map(r => (r.user, r.product))
      .groupByKey()
      .mapValues(_.toSet)
      .collectAsMap()

    val bActualArtists = sc.broadcast(actualArtistsPerUser)

    // RDD with (value, has_listened)
    val predictionsAndLabels = recommendations.flatMap { case (userID, ratings) =>
      val actualArtists = bActualArtists.value.getOrElse(userID, Set.empty[Int])
      ratings.map { r =>
        (r.rating, if (actualArtists.contains(r.product)) 1.0 else 0.0)
      }
    }

    // Use BinaryClassificationMetrics to compute global AUC
    val metrics = new BinaryClassificationMetrics(predictionsAndLabels)
    println(s"Global Area under ROC: ${metrics.areaUnderROC()}")

  }
}
