import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.{Genotype, FlatGenotype, GenotypeAllele}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector=>MLVector, Vectors}

val hdfsurl = "hdfs://ec2-54-77-227-155.eu-west-1.compute.amazonaws.com:9010"

def hu(s:String) = hdfsurl + s"/data/$s"


// read the panel file in HDFS
val panel = {
  val p = sparkContext.textFile(hu("ALL.panel"))
  p.map{ line => 
    val toks = line.split("\t").toList
    toks(0) -> toks(1)
  }.collectAsMap
}

