package medatscale

import scala.io.Source
 

// a panel is a Map SampleID => population
object Panel {
	def extract(filename: String)(filter: (String, String) => Boolean): Map[String, String] = {
			
		Source.fromFile(filename).getLines().map( line => {
			val toks = line.split("\t").toList
  			toks(0) -> toks(1)
			}).toMap.filter( tup => filter(tup._1, tup._2) )

	}

}