name := "Recommender Systems via Matrix Factorization"

version := "1.0"

scalaVersion := sys.env.getOrElse("SCALA_VERSION", "2.13.16")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "3.5.0"
)
