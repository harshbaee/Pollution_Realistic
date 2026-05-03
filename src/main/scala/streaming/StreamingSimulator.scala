package streaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingSimulator {
  def run(): Unit = {
    val spark = SparkSession.builder().appName("StreamingSimulator").master("local[*]").getOrCreate()
    import spark.implicits._
    try {
      val schema = spark.read.option("header", "true").csv("data/urban_pollution_realistic.csv").schema
      val s = spark.readStream.schema(schema).option("maxFilesPerTrigger",1).csv("data/stream_input")
      val agg = s.groupBy(window(col("timestamp"), "10 minutes"), col("station")).agg(avg(col("PM25")).alias("avg_pm25"))
      val q = agg.writeStream.format("console").outputMode("update").start()
      q.awaitTermination()
    } catch { case _:Throwable => println("Streaming directory missing — skipping streaming.") }
    spark.stop()
  }
}
