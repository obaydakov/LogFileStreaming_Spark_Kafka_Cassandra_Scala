package Batch

import java.lang.management.ManagementFactory

import config.Settings
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import domain._
import utils.SparkUtils._
/**
  * Created by deepu_us on 4/17/2017.
  */
object BatchJob {
  def main(args:Array[String]):Unit = {
    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    val wlc = Settings.WebLogGen

    //for reading data from input file and loading data using text files
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val sourcefile = "file:///vagrant/data.tsv"
    val input = sc.textFile(sourcefile)

    //using spark transformatons and actions
    val inputRDD = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }

    //pair RDDS where key will be product and timestamp
    val keyedByproduct = inputRDD.keyBy(a =>(a.product,a.timestamp_hour)).cache()

    //calculating number of visitors for the key
    val visitorByProduct = keyedByproduct.mapValues(a => a.visitor).distinct().countByKey()

    //calculating actions done on the product
    val activityByProduct = keyedByproduct.mapValues(a =>
      a.action match{
        case "purchase" =>(1,0,0)
        case "add_to_cart" =>(0,1,0)
        case "page_view" =>(0,0,1)
      }
    )
      .reduceByKey((a,b) =>(a._1+b._1,a._2+b._2,a._3+b._3))

    //printing data
    visitorByProduct.foreach(println)
    activityByProduct.foreach(println)


    //using data frames and SQL transformations.
    //converting input data to DataFrame
    val inputDF = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }.toDF()

    // a user defined function to calculae under exposed products
    sqlContext.udf.register("underExposed",(pageViewCount:Long,purchaseCount:Long)=>if(purchaseCount==0) 0 else pageViewCount/purchaseCount)

    //creating df from inputDF
    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour")/1000),1).as("timestamp_hour"),
      inputDF("referrer"),inputDF("action"),inputDF("prevPage"),inputDF("page"),inputDF("visitor"),inputDF("product")
    ).cache()

    df.registerTempTable("activity")

    //calculating number of visitors for the key
    val visitorByProductSQL = sqlContext.sql(
      """ SELECT product,timestamp_hour,count(DISTINCT visitor) as unique_visitors
        | from activity GROUP BY  product,timestamp_hour
      """.stripMargin)

    // calculating number of activities on the product
    val activityByProductSQL = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """).cache()
    activityByProductSQL.registerTempTable("activityproduct")

    // calculating under exposed products using sql
    val underExposedProducts = sqlContext.sql(
      """ SELECT product,timestamp_hour,underExposed(page_view_count,purchase_count) as negative_exposure
        | from activityproduct
        | order by negative_exposure DESC
        | limit 5
      """.stripMargin)

    /*visitorByProductSQL.printSchema()
    activityByProductSQL.printSchema()
    underExposedProducts.printSchema()*/

    // partioning the data by timestamp_hour and writing the output to data in hdfs
    activityByProductSQL.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")


  }
}
