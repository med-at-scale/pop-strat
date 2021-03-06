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
  val popSamples = Set("HG00096","HG00097","HG00099","HG00100","HG00101","HG00102","HG00103","HG00104","HG00106","HG00108","HG00109","HG00110","HG00111","HG00112","HG00113","HG00114","HG00116","HG00117","HG00118","HG00119","HG00120","HG00121","HG00122","HG00123","HG00124","HG00125","HG00126","HG00127","HG00128","HG00129","HG00130","HG00131","HG00133","HG00134","HG00135","HG00136","HG00137","HG00138","HG00139","HG00140","HG00141","HG00142","HG00143","HG00146","HG00148","HG00149","HG00150","HG00151","HG00152","HG00154","HG00155","HG00156","HG00158","HG00159","HG00160","HG00231","HG00232","HG00233","HG00234","HG00235","HG00236","HG00237","HG00238","HG00239","HG00240","HG00242","HG00243","HG00244","HG00245","HG00246","HG00247","HG00249","HG00250","HG00251","HG00252","HG00253","HG00254","HG00255","HG00256","HG00257","HG00258","HG00259","HG00260","HG00261","HG00262","HG00263","HG00264","HG00265","HG01334","NA19625","NA19700","NA19701","NA19703","NA19704","NA19707","NA19711","NA19712","NA19713","NA19818","NA19819","NA19834","NA19835","NA19900","NA19901","NA19904","NA19908","NA19909","NA19914","NA19916","NA19917","NA19920","NA19921","NA19922","NA19923","NA19982","NA19984","NA19985","NA20126","NA20127","NA20276","NA20278","NA20281","NA20282","NA20287","NA20289","NA20291","NA20294","NA20296","NA20298","NA20299","NA20314","NA20317","NA20322","NA20332","NA20334","NA20336","NA20339","NA20340","NA20341","NA20342","NA20344","NA20346","NA20348","NA20351","NA20356","NA20357","NA20359","NA20363","NA20412","NA20414")
  val (start, end) = (133017695, 133117814)
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