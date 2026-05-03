package etl

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ETLJob {

  val RAW_IN = "data/urban_pollution_500k.csv"
  val CLEANED_OUT = "data/cleaned/urban_pollution_cleaned.csv"

  def run(): Unit = {

    val spark = SparkSession.builder()
      .appName("ETLJob")
      .master("local[*]")
      .config("spark.hadoop.io.native.lib.available", "false")
      .config("spark.hadoop.fs.shell.delete.recursive", "true")
      .getOrCreate()


    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // ----- Read raw -----
    val raw = spark.read
      .option("header","true")
      .csv(RAW_IN)

    println(s"Loaded RAW rows: ${raw.count()}")

    // ----- Converters -----
    val toDoubleSafe = udf((s: String) => {
      try {
        if (s == null) null
        else s.trim.replace(",", ".").toDouble: java.lang.Double
      } catch {
        case _: Throwable => null
      }
    })


    // ----- Basic cleanup -----
    val df = raw
      .withColumn("timestamp", to_timestamp($"timestamp"))
      .withColumn("station_id", trim($"station"))
      .withColumn("line_id", trim($"line"))
      .withColumn("co2", toDoubleSafe($"co2"))
      .withColumn("pm25", toDoubleSafe($"pm25"))
      .withColumn("noise", toDoubleSafe($"noise_dB"))
      .withColumn("humidity", toDoubleSafe($"humidity"))
      .withColumn("temperature", toDoubleSafe($"temperature"))
      .withColumn("passengers", $"passenger_traffic".cast(IntegerType))
      .withColumn("event", when($"event".isNull, "none").otherwise(trim($"event")))
      .withColumn("weather", when($"weather".isNull, "unknown").otherwise(trim($"weather")))

    // ----- Remove junk -----
    val cleaned = df
      .na.drop("any", Seq("timestamp", "station_id", "pm25"))
      .filter($"pm25" > 0 && $"pm25" < 1000)

    // ----- Feature extraction -----
    val featured = cleaned
      .withColumn("date", to_date($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .withColumn("day_of_week", date_format($"timestamp","u").cast("int"))
      .withColumn("month", month($"timestamp"))
      .withColumn("is_weekend", when($"day_of_week" >= 6, 1).otherwise(0))
      .withColumn("is_holiday", when($"event" === "holiday", 1).otherwise(0))
      .withColumn("rush_hour",
        when($"hour".between(7,10), "morning")
          .when($"hour".between(17,20), "evening")
          .otherwise("none")
      )
      .withColumn("temp_bin",
        when($"temperature" < 5, "cold")
          .when($"temperature" < 20, "mild")
          .when($"temperature" < 35, "warm")
          .otherwise("hot")
      )
      .withColumn("pm25_log", log($"pm25"))

    println("Final cleaned + engineered schema:")
    featured.printSchema()

    // ----- Save as single CSV -----
    featured
      .coalesce(1)
      .write
      .option("header","true")
      .mode("overwrite")
      .csv(CLEANED_OUT)

    println(s"Saved CLEANED CSV → $CLEANED_OUT")

    spark.stop()
  }
}
