package medatscale

import scala.io.Source
import org.apache.spark.SparkContext

// a panel is a Map SampleID => population
object Panel {
	def extract(filename: String)(filter: (String, String) => Boolean= (s, t) => true): Map[String, String] = {
			
		Source.fromFile(filename).getLines().map( line => {
			val toks = line.split("\t").toList
  			toks(0) -> toks(1)
			}).toMap.filter( tup => filter(tup._1, tup._2) )

	}

	def extractFromHDFS(url: String)(filter: (String, String) => Boolean = (s, t) => true)(implicit sc: SparkContext): Map[String, String] = {
			
		sc.textFile(url).map(line => {
			val toks = line.split("\t").toList
  			toks(0) -> toks(1)
			}).collect.toMap
	}

}