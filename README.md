# 🌆 Urban Pollution Analysis — Apache Spark (Scala)

A distributed data pipeline built with **Scala + Apache Spark** to process, analyze, and model urban air quality data from 500,000+ sensor readings across metro stations.

---

## Screenshots

> 📸 Add screenshots of your Spark output / charts here.  
> You can export slides from `Urban_Pollution_Analysis_with_Apache_Spark.pptx` as images and drop them in a `screenshots/` folder, then reference them like:
> ```markdown
> ![ETL Output](picture1.png)
> ![ETL Output](picture2.png)
> ```

---

## Features

- **ETL Pipeline** — ingest raw CSV, clean nulls, cast types, normalize columns, export cleaned dataset
- **Statistical Analysis** — per-station & per-line aggregations (avg CO₂, PM2.5, noise), weather correlations, top polluters
- **Machine Learning** — Random Forest & GBT regressors to predict PM2.5; RMSE/R² evaluation with feature importance
- **Graph Processing** — GraphX station co-pollution network; PageRank to identify high-influence pollution nodes
- **Streaming Simulation** — micro-batch ingestion of new sensor files, sliding-window PM2.5 averages per station

---

## Architecture

```
Pollution_Realistic/
├── build.sbt                              ← SBT project config (Spark 3.5.7 / Scala 2.12)
├── data/
│   └── urban_pollution_latest.csv         ← 500k sensor readings
├── src/main/scala/
│   ├── main.scala                         ← Entry point — dispatches to jobs
│   ├── etl/
│   │   └── ETLJob.scala                   ← Clean & transform raw data
│   ├── analysis/
│   │   └── AnalysisJob.scala              ← Descriptive statistics & aggregations
│   ├── ml/
│   │   └── MLJob.scala                    ← Random Forest & GBT regression
│   ├── graph/
│   │   └── GraphJob.scala                 ← GraphX co-pollution network + PageRank
│   └── streaming/
│       └── StreamingSimulator.scala       ← Structured Streaming micro-batch
└── project/
    └── build.properties
```

---

## Dataset

| Column | Type | Description |
|---|---|---|
| timestamp | DateTime | Reading timestamp (dd/MM/yyyy HH:mm) |
| station | String | Station ID (ST_001 → ST_050) |
| line | String | Metro line (L1–L5) |
| CO2 | Double | CO₂ concentration (ppm) |
| PM25 | Double | Fine particulate matter (µg/m³) |
| noise_dB | Double | Ambient noise level (dB) |
| humidity | Double | Relative humidity (%) |
| temperature | Double | Temperature (°C) |
| weather | String | Weather condition (rain/fog/cloudy/…) |
| passenger_traffic | Int | Passenger count at reading time |
| event | String | Local event (football_match/none/…) |

500,000 rows · 11 columns

---

## Setup & Run

### Prerequisites
- Java 11+
- Scala 2.12
- SBT 1.x

### 1. Clone & install dependencies
```bash
git clone https://github.com/YOUR_USERNAME/Pollution_Realistic.git
cd Pollution_Realistic
sbt compile
```

### 2. Run a job
```bash
sbt "run etl"        # ETL: clean raw data → data/cleaned/
sbt "run analysis"   # Statistical analysis & aggregations
sbt "run ml"         # Train Random Forest & GBT, print metrics
sbt "run graph"      # GraphX network + PageRank
```

### 3. Streaming simulation *(optional)*
Place new CSV files in `data/stream_input/` then:
```bash
sbt "run streaming"
```

---

## ML Results

Two models are trained and compared on PM2.5 prediction:

| Model | Metric |
|---|---|
| Random Forest Regressor | RMSE + R² printed to console |
| Gradient Boosted Trees | RMSE + R² printed to console |

Feature importance is printed for both models after training.

---

## Tech Stack

| Tool | Version |
|---|---|
| Scala | 2.12.18 |
| Apache Spark Core | 3.5.7 |
| Spark SQL | 3.5.7 |
| Spark MLlib | 3.5.7 |
| Spark GraphX | 3.5.7 |
| SBT | 1.x |

---

*MASTER ADEO2 — Big Data Project — 2025*
