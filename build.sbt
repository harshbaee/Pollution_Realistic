name := "Pollution_Realistic"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % "3.5.7",
  "org.apache.spark" %% "spark-sql"   % "3.5.7",
  "org.apache.spark" %% "spark-mllib" % "3.5.7",
  "org.apache.spark" %% "spark-graphx" % "3.5.7"
)
