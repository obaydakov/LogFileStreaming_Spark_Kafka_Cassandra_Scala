package streaming

import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import utils.SparkUtils._
import functions._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
/**
  * Created by deepu_us on 4/17/2017.
  */
object StreamingJob {

  def main(args:Array[String]):Unit = {
    //setup spark context
    val sc = getSparkContext("Streaming With Spark")
    val sqlcontext = getSQLContext(sc)
    import sqlcontext.implicits._
    val batchDuration = Seconds(4)

    def streamingApp(sc: SparkContext, batchDuration: Duration)= {
      val ssc = new StreamingContext(sc, batchDuration)
      val wlc = Settings.WebLogGen
      val topic = wlc.kafkaTopic

      val inputPath = isIDE match {
        case true => "file:///C:\\Users\\deepu_us\\VirtualBox VMs\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\input"
        case false => "file:///vagrant/input"
      }

      val textDstream = ssc.textFileStream(inputPath)
      val activityStream = textDstream.transform(input => {
        input.flatMap{ line =>
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
          else
            None
        }
      })

      val statefulActivityDstream = activityStream.transform(rdd =>{
        val df = rdd.toDF()
        df.registerTempTable("activity")
        val activityByProductSQL = sqlcontext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """)
          activityByProductSQL.map(r =>((r.getString(0),r.getLong(1)),ActivityByProduct(r.getString(0),r.getLong(1),r.getLong(2),r.getLong(3)
          ,r.getLong(4))))
      }
     ).cache()

      val activitySpaceSpec = StateSpec.function(mapActivityStatefunc)
                              .timeout(Minutes(120))
      val statefulActivityByProductMap = statefulActivityDstream.mapWithState(activitySpaceSpec)

      val activitySnapshot = statefulActivityByProductMap.stateSnapshots()
      activitySnapshot.reduceByKeyAndWindow(
        (a,b)=>a,
        (x,y)=>x,
        Seconds(30/4*4)
      ).foreachRDD(rdd => rdd.map(sr=>ActivityByProduct(sr._1._1,sr._1._2,sr._2._1,sr._2._2,sr._2._3))
                    .toDF().registerTempTable("ActivityByproduct"))

      //unique visitors by product
      val visitorsStateSpec = StateSpec.function(mapVisitorsStatefunc)
                                        .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val stateVisitorsByProduct = activityStream.map(a=>{
        ((a.product,a.timestamp_hour),hll(a.visitor.getBytes))
      }).mapWithState(visitorsStateSpec)

      val visitorStateSnapshot = stateVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot.reduceByKeyAndWindow(
        (a,b) =>b,
        (x,y) =>x,
        Seconds(30/4*4)
      ) //only save or expose the snapshot every x seconds
        .foreachRDD(rdd => rdd.map(sr =>VisitorsByProduct(sr._1._1,sr._1._2, sr._2.approximateSize.estimate))
        .toDF().registerTempTable("visitors by product"))

      //update state by key
       val statefulActivityByProduct = statefulActivityDstream.updateStateByKey((newItemsPerKey:Seq[ActivityByProduct], currentState:Option[(Long,Long,Long,Long)]) =>{
        var(prevTimeStamp,purchase_count,add_to_cart_count,page_view_count) = currentState.getOrElse((System.currentTimeMillis(),0L,0L,0L))
        var result :Option[(Long,Long,Long,Long)] = null
        if(newItemsPerKey.isEmpty){
          if(System.currentTimeMillis()-prevTimeStamp >30000+4000)
            result = None
          else
            result = Some((prevTimeStamp,purchase_count,add_to_cart_count,page_view_count))
        }else {

          newItemsPerKey.foreach(a => {
            purchase_count += a.purchase_count
            add_to_cart_count += a.add_to_cart_count
            page_view_count += a.page_view_count
          })
          result = Some((System.currentTimeMillis(), purchase_count, add_to_cart_count, page_view_count))
        }
          result
      })
      statefulActivityByProduct.print(10)
      ssc
    }
    val ssc = getStreamingContext(streamingApp,sc,batchDuration)
    ssc.start()
    ssc.awaitTermination()
  }
}
