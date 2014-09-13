package medatscale

import java.io.{ File, FileInputStream }
import scala.util.Try


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector=>MLVector, Vectors}

//import net.sf.samtools._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.bdgenomics.adam.converters.{ VCFLine, VCFLineConverter, VCFLineParser }
import org.bdgenomics.formats.avro.{Genotype, FlatGenotype}
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.bdgenomics.adam.rdd.ADAMContext

//import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
//import parquet.avro.AvroParquetWriter

import Implicits._
import scala.collection.JavaConversions._

object VariantSampler extends App {
  val input::output::fractStr::rest = args.toList
  val hdfsUrl = rest.headOption

  val fraction = fractStr.toDouble

  val sparkContext: SparkContext = ADAMContext.createSparkContext(
                                        "variant-sampler",
                                        "local[6]",
                                        "",
                                        sparkJars = Seq.empty[String],
                                        sparkEnvVars = Seq.empty[(String, String)],
                                        sparkAddStatsListener = false,
                                        sparkKryoBufferSize = 4,
                                        sparkMetricsListener = None /*Option[ADAMMetricsListener]*/,
                                        loadSystemValues = true,
                                        sparkDriverPort = None)
  val gts:RDD[Genotype] = sparkContext.adamLoad(input)

  val sampledGts = gts.filter(elt => scala.util.Random.nextDouble <= fraction)


  sampledGts.adamSave(output)
  println("Converted file created and written!")
  println(s"Number of genotypes: ${sampledGts.count}")

}
