package analysis

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object AnalysisJob {

  val INPUT = "data/urban_pollution_latest.csv"

  def run(): Unit = {

    val spark = SparkSession.builder()
      .appName("AnalysisJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // ============================
    // 0. Load + Rename Columns
    // ============================

    val df0 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(INPUT)

    val df = df0
      .withColumn("timestamp", to_timestamp($"timestamp", "dd/MM/yyyy HH:mm"))
      .withColumnRenamed("station", "station_id")
      .withColumnRenamed("line", "line_id")
      .withColumnRenamed("noise_dB", "noise")
      .withColumnRenamed("passenger_traffic", "passengers")
      .withColumn("date", to_date($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .withColumn("day_of_week", dayofweek($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("is_holiday", lit(0))
      .cache()

    println(s"Loaded cleaned dataset: ${df.count()} rows")

    // ============================
    // 1. Station statistics
    // ============================

    val stats = df.groupBy("station_id")
      .agg(
        avg("PM25").alias("pm25_avg"),
        max("PM25").alias("pm25_max"),
        avg("CO2").alias("co2_avg"),
        avg("noise").alias("noise_avg")
      )
      .orderBy(desc("pm25_avg"))

    stats.show(10, false)

    // ============================
    // 2. Hourly peaks
    // ============================
    val hourly = df.groupBy("hour")
      .agg(avg("PM25").alias("pm25_hourly"))
      .orderBy(desc("pm25_hourly"))

    hourly.show(24, false)

    // ============================
    // 3. Correlation matrix
    // ============================
    val numCols = Seq("PM25", "CO2", "noise", "humidity", "temperature", "passengers")

    println("\n=== Correlations ===")
    numCols.foreach { a =>
      numCols.foreach { b =>
        val corr = df.stat.corr(a, b)
        print(f"[$a vs $b = $corr%1.3f] ")
      }
      println()
    }

    // ============================
    // 4. Global Pollution Index
    // ============================
    val statsRow = df.select(
      mean($"PM25"), stddev($"PM25"),
      mean($"CO2"), stddev($"CO2"),
      mean($"noise"), stddev($"noise")
    ).first()

    val withIndex = df.withColumn("global_index",
      (($"PM25" - statsRow.getDouble(0)) / statsRow.getDouble(1)) * 0.5 +
        (($"CO2" - statsRow.getDouble(2)) / statsRow.getDouble(3)) * 0.3 +
        (($"noise" - statsRow.getDouble(4)) / statsRow.getDouble(5)) * 0.2
    )

    println("\nSample Global Index:")
    withIndex.select("timestamp", "station_id", "PM25", "global_index").show(10, false)

    // ============================
    // 5. PM2.5 anomalies
    // ============================
    val pm25Mean = statsRow.getDouble(0)
    val pm25Std  = statsRow.getDouble(1)

    val anomalies = df.filter($"PM25" > pm25Mean + 3 * pm25Std)

    println(s"\nAnomaly Count: ${anomalies.count()}")
    anomalies.show(10, false)

    // ============================
    // 6. Event pollution levels
    // ============================
    val events = df.groupBy("event")
      .agg(avg("PM25").alias("pm25_avg"))
      .orderBy(desc("pm25_avg"))

    println("\n=== PM2.5 by Event ===")
    events.show(false)

    // ============================
    // 7. Rush-hour comparison
    // ============================
    val rush = df.withColumn("rush_hour",
      when($"hour".between(7, 9) || $"hour".between(17, 19), "rush")
        .otherwise("normal")
    )

    val rushStats = rush.groupBy("rush_hour").agg(avg("PM25").alias("pm25_avg"))
    println("\n=== Rush vs Normal ===")
    rushStats.show(false)

    spark.stop()
  }
}
