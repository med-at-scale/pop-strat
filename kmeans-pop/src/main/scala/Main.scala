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

object Main extends App {
  import GlobalSparkContext._

  val file::output::rest = args.toList
  val hdfsUrl = rest.headOption

  /*val sampleSubset:Option[Set[String]] = None
  val vcfReader = new VCFLineParser(new FileInputStream(new File(file)), sampleSubset)

  vcfReader foreach {
    case vcfLine: VCFLine =>
      VCFLineConverter.convert(vcfLine).foreach {
        case genotype: FlatGenotype =>
        // here is where we assign each ADAMFlatGenotype to an output file
        println(s"""
          Gen [${genotype.getSampleId.toString}]:
            * ${genotype.variantId}
            * ${genotype.getReferenceName}
            * ${genotype.getPosition}
            * ${genotype.getReferenceAllele}
            * ${genotype.getAlleles.mkString(",")}
            * ${genotype.asDouble}
        """)
      }
  }*/

  //val printMetics = true
  //val metricsListener = if (printMetics) Some(new ADAMMetricsListener(new ADAMMetrics())) else None

/*  val sparkContext: SparkContext = ADAMContext.createSparkContext(
                                        "kmeans-pop",
                                        "local[6]",
                                        "",
                                        sparkJars = Seq.empty[String],
                                        sparkEnvVars = Seq.empty[(String, String)],
                                        sparkAddStatsListener = false,
                                        sparkKryoBufferSize = 4,
                                        sparkMetricsListener = None /*Option[ADAMMetricsListener]*/,
                                        loadSystemValues = true,
                                        sparkDriverPort = None)*/

  val outputExists = Try {
                    val fs = FileSystem.get(new java.net.URI(hdfsUrl.get), sparkContext.hadoopConfiguration)
                    val hdfsPath: Path = new Path(output)
                    val ok = fs.exists(hdfsPath)
                    println(s"hdfs $file exists!")
                    ok
                  }.toOption.orElse(
                  Try {
                    val f = new File(output)
                    val ok = f.exists
                    println(s"file $file exists!")
                    ok
                  }.toOption)
                  .getOrElse(throw new Exception(s"Cannot deal with output: $output"))


  val gts:RDD[Genotype] =
    (if (!outputExists) {
      val adamVariants: RDD[VariantContext] = sparkContext.adamVCFLoad(file, dict = None)
      val coalesce = 1

      def doif[A](p: A=>Boolean)(f:A=>A):A=>A= (a:A) => if(p(a)) f(a) else a
      val maybeCoalesce = doif[RDD[VariantContext]](_ => coalesce > 1)(_.coalesce(coalesce, true))
      val gts:RDD[Genotype] = maybeCoalesce(adamVariants).flatMap(p => p.genotypes)

      gts.adamSave(output)
      println("Converted file created and written!")

      gts
    } else {
      import org.bdgenomics.adam.predicates._
      val gts:RDD[Genotype] = sparkContext.adamLoad(output, Some(classOf[GenotypePopulationPredicate]))
      // instead filter the RDD[Genotype] here?
      //val gts:RDD[Genotype] = sparkContext.adamLoad(output)
      gts
    }).cache
  println(s"Number of genotypes found ${gts.count}")

  val sampleCount = gts.map(_.getSampleId.toString.hashCode).distinct.count
  println(s"#Samples: $sampleCount")

  @transient val variantIds:Set[String] = gts.map(_.variantId).distinct.collect().toSet
  println("Variants:")
  variantIds foreach println
  //val variantIdsSize = variantIds.size

  val variantsById = gts.keyBy(_.variantId.hashCode).groupByKey.cache
  val variantsCount = variantsById.keys.count
  println(s"#Variants: $variantsCount")
  val missingVariants = variantsById.filter { case (k, it) =>
                                      it.size != sampleCount
                                    }.keys.collect().toSet
  println(s"#Missing $missingVariants")

  type VariantHashCode = Int
  val sampleToData:RDD[(String, (Double, VariantHashCode))] =
    gts .filter { g => ! (missingVariants contains g.variantId.hashCode) }
        .map { g =>
          (g.getSampleId.toString, (g.asDouble, g.variantId.hashCode))
        }

  val dataPerSampleId:RDD[(String, MLVector)] = sampleToData.groupByKey
                                    .mapValues { it =>
                                      Vectors.dense(it.toArray.sortBy(_._2).map(_._1))
                                    }
                                    .cache
  val dataFrame:RDD[MLVector] = dataPerSampleId.values

  val dataFrameSizes = dataFrame.map(_.size).collect()
  println("Vector sizes:")
  dataFrameSizes foreach (x => println(" > " + x))

  val kmeansStart = System.nanoTime
  println("About to run the KMeans: " + kmeansStart)
  val model:KMeansModel = KMeans.train(dataFrame, 2, 10)
  val kmeansEnd = System.nanoTime
  println("Ran the KMeans in " + (kmeansEnd - kmeansStart))

  println("KMeans centroids")
  val centroids = model.clusterCenters.map { center => center.toArray.toList }
  centroids map (c => println(s" > ${c.mkString(" ; ")}"))

  dataPerSampleId.collect().foreach { case (sampleId, vector) =>
    val cluster = model.predict(vector)
    println(s"Sample [$sampleId] is in cluster #$cluster")
  }

}
