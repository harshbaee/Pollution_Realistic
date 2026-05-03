package ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressor, GBTRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline}

object MLJob {

  val INPUT = "data/urban_pollution_latest.csv"

  def run(): Unit = {

    val spark = SparkSession.builder()
      .appName("MLJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // =====================================================
    // 1) LOAD DATA
    // =====================================================
    val df0 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(INPUT)
      .withColumn("timestamp", to_timestamp($"timestamp", "dd/MM/yyyy HH:mm"))
      .withColumnRenamed("station", "station_id")
      .withColumnRenamed("line", "line_id")
      .withColumnRenamed("noise_dB", "noise")
      .withColumnRenamed("passenger_traffic", "passengers")

    // =====================================================
    // 2) ADD TIME FEATURES
    // =====================================================
    val df1 = df0
      .withColumn("date", to_date($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .withColumn("day_of_week", dayofweek($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("rush_hour",
        when($"hour".between(7, 9) || $"hour".between(17, 19), 1).otherwise(0)
      )
      .withColumn("is_event", when($"event" =!= "none", 1).otherwise(0))
      .na.drop("any")

    // =====================================================
    // 3) SAMPLE DOWN TO 100K ROWS
    // =====================================================
    val df = df1.sample(false, 100000.0 / df1.count()).cache()
    println(s"ML Dataset Size: ${df.count()} rows")

    // =====================================================
    // 4) LAG FEATURES
    // =====================================================
    val w = Window.partitionBy("station_id").orderBy($"timestamp")

    val dfLagged = df
      .withColumn("pm25_lag1", lag("PM25", 1).over(w))
      .withColumn("pm25_lag2", lag("PM25", 2).over(w))
      .na.fill(Map("pm25_lag1" -> 0.0, "pm25_lag2" -> 0.0))

    // =====================================================
    // 5) STRING INDEXING
    // =====================================================
    val idxStation = new StringIndexer().setInputCol("station_id").setOutputCol("station_idx").setHandleInvalid("keep")
    val idxLine    = new StringIndexer().setInputCol("line_id").setOutputCol("line_idx").setHandleInvalid("keep")
    val idxWeather = new StringIndexer().setInputCol("weather").setOutputCol("weather_idx").setHandleInvalid("keep")
    val idxEvent   = new StringIndexer().setInputCol("event").setOutputCol("event_idx").setHandleInvalid("keep")

    // =====================================================
    // 6) FEATURES
    // =====================================================
    val featureCols = Array(
      "CO2", "noise", "humidity", "temperature",
      "passengers", "hour", "day_of_week", "month",
      "pm25_lag1", "pm25_lag2",
      "rush_hour", "is_event",
      "station_idx", "line_idx", "weather_idx", "event_idx"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    // =====================================================
    // 7) MODELS
    // =====================================================
    val rf = new RandomForestRegressor()
      .setLabelCol("PM25")
      .setFeaturesCol("features")
      .setNumTrees(80)
      .setMaxDepth(10)
      .setMaxBins(128)

    val gbt = new GBTRegressor()
      .setLabelCol("PM25")
      .setFeaturesCol("features")
      .setMaxIter(50)
      .setMaxDepth(6)
      .setMaxBins(128)

    // =====================================================
    // 8) EVALUATORS
    // =====================================================
    val evaluatorRMSE = new RegressionEvaluator().setLabelCol("PM25").setMetricName("rmse")
    val evaluatorR2   = new RegressionEvaluator().setLabelCol("PM25").setMetricName("r2")
    val evaluatorMAE  = new RegressionEvaluator().setLabelCol("PM25").setMetricName("mae")

    val Array(train, test) = dfLagged.randomSplit(Array(0.8, 0.2), seed = 42)

    // =====================================================
    // 9) TRAINING FUNCTION + FULL METRICS
    // =====================================================
    def trainModel(name: String, model: org.apache.spark.ml.Predictor[_, _, _]): Unit = {
      println(s"\n========================= $name =========================")

      val pipeline = new Pipeline()
        .setStages(Array(idxStation, idxLine, idxWeather, idxEvent, assembler, model))

      val fitted = pipeline.fit(train)
      val preds = fitted.transform(test)

      val rmse = evaluatorRMSE.evaluate(preds)
      val r2 = evaluatorR2.evaluate(preds)
      val mae = evaluatorMAE.evaluate(preds)

      val mape = preds
        .withColumn("ape", abs(($"PM25" - $"prediction") / $"PM25"))
        .agg(avg($"ape"))
        .first().getDouble(0) * 100.0

      println(s"RMSE  = ${"%.4f".format(rmse)}")
      println(s"MAE   = ${"%.4f".format(mae)}")
      println(s"R²    = ${"%.4f".format(r2)}")
      println(s"MAPE  = ${"%.2f".format(mape)} %")

      // =====================================================
      // CONFUSION MATRIX (BINNED PM25)
      // =====================================================
      println("\nConfusion Matrix (Binned PM25 ranges):")

      val binned = preds
        .withColumn("actual_bin",
          when($"PM25" < 25, "Low")
            .when($"PM25" < 60, "Moderate")
            .otherwise("High")
        )
        .withColumn("pred_bin",
          when($"prediction" < 25, "Low")
            .when($"prediction" < 60, "Moderate")
            .otherwise("High")
        )
        .groupBy("actual_bin", "pred_bin")
        .count()
        .orderBy("actual_bin", "pred_bin")

      binned.show(20, false)
    }

    // =====================================================
    // 10) TRAIN BOTH MODELS
    // =====================================================
    trainModel("Random Forest", rf)
    trainModel("GBT", gbt)

    spark.stop()
  }
}
