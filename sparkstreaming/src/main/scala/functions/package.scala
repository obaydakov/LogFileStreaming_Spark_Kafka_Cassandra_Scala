import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import domain.{Activity, ActivityByProduct}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.State
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
  * Created by deepu_us on 4/18/2017.
  */
package object functions {

  def rddToRDDActivity(input : RDD[(String,String)]) ={
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

    input.mapPartitionsWithIndex( {(index, it) =>
      val or = offsetRanges(index)
      it.flatMap { kv =>
        val line = kv._2
        val record = line.split("\\t")
        val MS_IN_HOUR = 1000 * 60 * 60
        if (record.length == 7)
          Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6),
            Map("topic"-> or.topic, "kafkaPartition" -> or.partition.toString,
              "fromOffset" -> or.fromOffset.toString, "untilOffset" -> or.untilOffset.toString)))
        else
          None
      }
    })
  }
  def mapActivityStatefunc=(k:(String,Long),v:Option[ActivityByProduct],state:State[(Long,Long,Long)]) =>{
    var(purchase_count,add_to_cart_count,page_view_count) = state.getOption().getOrElse((0L,0L,0L))
    val newVal = v match {
      case Some(a:ActivityByProduct) =>(a.purchase_count,a.add_to_cart_count,a.page_view_count)
      case _ =>(0L,0L,0L)
    }
    purchase_count += newVal._1
    add_to_cart_count += newVal._2
    page_view_count +=newVal._3

    state.update((purchase_count,add_to_cart_count,page_view_count))
    val underExposed  = {
      if (purchase_count ==0)
        0
      else
        page_view_count/purchase_count
    }
    underExposed
  }

  def mapVisitorsStatefunc =(k:(String,Long),v:Option[HLL],state:State[HLL]) =>{
    val currentvisitorHLL = state.getOption().getOrElse(new HyperLogLogMonoid(12).zero)
    val newvisitorHLL = v match{
      case Some(vistorHLL) => currentvisitorHLL+vistorHLL
      case None =>currentvisitorHLL
    }
    state.update(newvisitorHLL)
    val output = newvisitorHLL.approximateSize.estimate
    output
  }
}
