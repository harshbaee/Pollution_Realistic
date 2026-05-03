object main {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: main <job>")
      println("jobs: etl | analysis | ml | graph")
      sys.exit(1)
    }
    args(0) match {
      case "etl"      => etl.ETLJob.run()
      case "analysis" => analysis.AnalysisJob.run()
      case "ml"       => ml.MLJob.run()
      case "graph"    => graph.GraphJob.run()
      case other      => println(s"Unknown job: $other")
    }
  }
}
