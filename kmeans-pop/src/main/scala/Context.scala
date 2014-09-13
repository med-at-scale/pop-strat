package medatscale

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object GlobalSparkContext {
  // create a spark config, option ae set from
  // environment variables or from options of spark-submit
  val sparkConf = new SparkConf().setAppName("Medatscale tool")

  // the spark context
  implicit val sparkContext = new SparkContext(sparkConf)

}