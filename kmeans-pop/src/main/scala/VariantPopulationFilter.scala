package org.bdgenomics.adam.predicates

import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.adam.predicates.ColumnReaderInput.ColumnReaderInput
import parquet.column.ColumnReader

object MorePredicteUtils {
	def isLargerOrEqual(x: Long, y: Long): Boolean = x >= y
	def isSmallerOrEqual(x: Long, y: Long): Boolean = x <= y

    def isEqual(x: String, y: String): Boolean = x == y
    def isInSet(x: String, y: Set[String]) = y.contains(x)

}

object MoreColumnReaderInput extends Serializable {
 implicit object ColumnReaderInputLong extends ColumnReaderInput[Long] {
    def convert(input: ColumnReader): Long = input.getLong
  }	
}


case class GenotypePopulationPredicate extends ADAMPredicate[Genotype] {
 import MoreColumnReaderInput._
 	/* TERRIBLE HARDCODED CONDITIONS...*/ 
  val popSamples = Set("HG00096")
  val (start, end) = (133017695, 133017814)
  val chr = "6"

  val isInPopulation = (x: String) => MorePredicteUtils.isInSet(x, popSamples)
  val isOnContig = (x: String) => MorePredicteUtils.isEqual(x, chr)
  val isAfterStart = (x: Long) => MorePredicteUtils.isLargerOrEqual(x, start)
  val isBeforeEnd = (x: Long) => MorePredicteUtils.isSmallerOrEqual(x, end)

  override val recordCondition = RecordCondition[Genotype](FieldCondition("sampleId", isInPopulation),
  	                                                       FieldCondition("variant.contig.contigName", isOnContig),
  	                                                       FieldCondition("variant.start", isAfterStart),
  	                                                       FieldCondition("variant.end", isBeforeEnd)

  	)
}