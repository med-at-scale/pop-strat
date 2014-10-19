package medatscale

import Implicits._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector=>MLVector, Vectors}

import org.bdgenomics.formats.avro.{Genotype, FlatGenotype}

object KMeansTrain {
	type VariantHashCode = Int
	// public static void /// no, joking
	/*
	 dataFrame: The dataframe
	 k: number of clusters
	 modelDir is the model save directory (HDFS)
	*/
	def train(dataFrame :RDD[MLVector], k: Int, modelDir: String)(implicit hu: (String) => String, sc: SparkContext) = {

		val model:KMeansModel = KMeans.train(dataFrame, 2, 10)
		val modelRDD: RDD[KMeansModel] = sc.parallelize(List[KMeansModel](model))
  		modelRDD.saveAsObjectFile(modelDir + "/kMeansModel")
  		model
	}


	/*
	Removes the missingVariants and creates a dataFrome for MLLib,
	*/
	def makeDataFrame(gts:RDD[Genotype], missingVariantsRDD: RDD[VariantHashCode]): RDD[MLVector] = {

		val missingVariants = missingVariantsRDD.collect().toSet
  		val sampleToData: RDD[(String, (Double, VariantHashCode))] =
    		gts.filter { g => ! (missingVariants contains g.variantId.hashCode) }
        	    .map { g =>
          			(g.getSampleId.toString, (g.asDouble, g.variantId.hashCode))
        			}
  		val dataPerSampleId:RDD[(String, MLVector)] = sampleToData.groupByKey
                                    .mapValues { it =>
                                      Vectors.dense(it.toArray.sortBy(_._2).map(_._1))
                                    }
                                    .cache
		dataPerSampleId.values
	}

	/*
	Returns (ALLVariants, MissingVairants)
	*/
	def filterIncompleteVariants(gts :RDD[Genotype], modelDir: String): (RDD[VariantHashCode], RDD[VariantHashCode]) = {
		@transient val variantIds: Set[String] = gts.map(_.variantId).distinct.collect().toSet
		//##############

		val sampleCount = gts.map(_.getSampleId.toString.hashCode).distinct.count
		val variantsById = gts.keyBy(_.variantId.hashCode).groupByKey.cache
		val variantsCount = variantsById.keys.count

		val missingVariantsRDD = variantsById.filter { case (k, it) =>
                                      it.size != sampleCount
                                    }.keys
  		// saving the list of variants with missing genotypes (because cannot be used for prediction later)
  		// it is a list of Int (variantId.hashCode)  
  		//val modelDir = "/vol3/data"                            
		//missingVariantsRDD.saveAsObjectFile(modelDir + "/missing-variants")
  		// saving the list of all variants 
  		// (diff with missing-variants is the list of vatiants to be used for prediction)
  		// it is a list of Int (variantId.hashCode)                              
		//variantsById.keys.saveAsObjectFile(modelDir + "/all-variants")
		(variantsById.keys, missingVariantsRDD)

	}
}

