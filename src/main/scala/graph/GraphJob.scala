package graph

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphJob {

  val INPUT = "data/urban_pollution_latest.csv"

  def run(): Unit = {

    val spark = SparkSession.builder()
      .appName("GraphJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println("=== Loading dataset for GraphX ===")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(INPUT)
      .withColumnRenamed("station", "station_id")
      .withColumnRenamed("line", "line_id")
      .withColumnRenamed("noise_dB", "noise")
      .withColumnRenamed("passenger_traffic", "passengers")
      .na.drop()

    // Drop timestamps because they aren’t needed for the graph
    val dfClean = df
      .withColumn("PM25", $"PM25".cast("double"))
      .withColumn("CO2", $"CO2".cast("double"))
      .withColumn("passengers", $"passengers".cast("int"))

    println(s"Loaded ${dfClean.count()} rows for graph processing")

    // =====================================================
    //   1) Station vertices
    // =====================================================
    val stationStats = dfClean.groupBy("station_id")
      .agg(
        avg("PM25").as("pm25_avg"),
        avg("CO2").as("co2_avg"),
        avg("passengers").as("passengers_avg")
      )

    val vertices: RDD[(VertexId, (String, Double, Double, Double))] =
      stationStats.rdd.map { row =>
        val id = row.getAs[String]("station_id").replace("ST_", "").toLong
        val name = row.getAs[String]("station_id")
        val pm25 = row.getAs[Double]("pm25_avg")
        val co2 = row.getAs[Double]("co2_avg")
        val pas = row.getAs[Double]("passengers_avg")
        (id, (name, pm25, co2, pas))
      }

    // =====================================================
    //   2) Edges (stations connected by the same line)
    // =====================================================
    // Convert Dataset → RDD[(station_id, line_id)]
    val edgesRaw = dfClean.select("station_id", "line_id").distinct()
    val pairs: RDD[(String, String)] = edgesRaw
      .select("station_id", "line_id")
      .rdd
      .map(row => (row.getString(0), row.getString(1)))

    // Group stations by line_id, then connect stations on the same line
    val edgesRDD: RDD[Edge[String]] = pairs
      .groupBy { case (_, line) => line }
      .flatMap { case (_, stations) =>
        val ids = stations
          .map { case (station, _) => station.replace("ST_", "").toLong }
          .toList

        ids.combinations(2).map {
          case List(a, b) => Edge(a, b, "same_line")
        }
      }



    // =====================================================
    //   3) Build the graph
    // =====================================================
    val graph = Graph(vertices, edgesRDD)

    println(s"Graph loaded: ${graph.vertices.count()} stations, ${graph.edges.count()} edges")

    // =====================================================
    //   4) PageRank — Most influential stations
    // =====================================================
    val pr = graph.pageRank(0.0001).vertices

    val ranked = pr.join(vertices).map {
      case (id, (score, (name, pm25, co2, pas))) =>
        (name, score, pm25)
    }.sortBy(-_._2)

    println("\n=== Top 10 Most Influential Stations (PageRank) ===")
    ranked.take(10).foreach(println)

    // =====================================================
    //   5) Pollution propagation score (neighbour averages)
    // =====================================================
    val nbrPM25 = graph.aggregateMessages[Double](
      triplet => triplet.sendToDst(triplet.srcAttr._2),
      (a, b) => a + b
    )

    val nbrCounts = graph.aggregateMessages[Double](
      triplet => triplet.sendToDst(1.0),
      (a, b) => a + b
    )

    val propagationScore = nbrPM25.join(nbrCounts).map {
      case (id, (sumPM25, count)) => (id, sumPM25 / count)
    }

    val propagated = propagationScore.join(vertices).map {
      case (id, (nbPM25, (name, pm25, co2, pas))) =>
        (name, pm25, nbPM25)
    }.sortBy(-_._3)

    println("\n=== Stations Influenced by Neighbour Pollution ===")
    println("(station, pm25_avg, neighbour_pm25_avg)")
    propagated.take(10).foreach(println)

    // =====================================================
    //   6) Connected components — Line clusters
    // =====================================================
    val cc = graph.connectedComponents().vertices

    val groups = cc.join(vertices).map {
      case (_, (comp, (name, pm25, co2, pas))) => (comp, name)
    }

    println("\n=== Connected Components (clusters of stations) ===")
    groups.groupByKey().take(5).foreach { case (comp, stations) =>
      println(s"Component $comp → ${stations.toList.mkString(", ")}")
    }

    spark.stop()
  }
}
