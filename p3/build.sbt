name := "Recommender Systems via Matrix Factorization"
version := "1.0"
scalaVersion := sys.env.getOrElse("SCALA_VERSION", "2.13.16")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % "4.0.1" % "provided",
  "org.apache.spark" %% "spark-sql"   % "4.0.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "4.0.1" % "provided"
)

ThisBuild / evictionErrorLevel := Level.Warn
