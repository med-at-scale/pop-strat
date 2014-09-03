package medatscale

import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele}
import scala.collection.JavaConversions._

object Implicits {
  implicit class RichGenotype(g:Genotype) {
    val variantId:String = {
      val name = g.getVariant.getContig.getContigName
      val start = g.getVariant.getStart
      val end = g.getVariant.getEnd
      s"$name:$start:$end"
    }

    val asDouble:Double = g.getAlleles.count(_ != GenotypeAllele.Ref)
  }
}
