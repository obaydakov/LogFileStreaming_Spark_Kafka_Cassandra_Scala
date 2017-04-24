package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by deepu_us on 4/17/2017.
  */
object SparkUtils {

  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }
  def getSparkContext(appName:String) ={
    var checkPointDirectory = ""
    //get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)

    // Check if running from IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir", "C:\\WinUtil") // required for winutils
      conf.setMaster("local[*]")
      checkPointDirectory ="file:///C:\\Temp"
    }else{
      checkPointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkPointDirectory)
    sc
  }

  def getSQLContext(sc:SparkContext) ={
    val sQLContext = SQLContext.getOrCreate(sc)
    sQLContext
  }


  def getStreamingContext(streamingApp: (SparkContext,Duration) => StreamingContext,sc:SparkContext,batchDuration:Duration)={
    val creatingFunc =() =>streamingApp(sc,batchDuration)
    val ssc = sc.getCheckpointDir match{
    case Some(checkPointDir) => StreamingContext.getActiveOrCreate(checkPointDir,creatingFunc,sc.hadoopConfiguration,createOnError = true)
    case None =>StreamingContext.getActiveOrCreate(creatingFunc)
  }
    sc.getCheckpointDir.foreach(cp =>ssc.checkpoint(cp))
    ssc
  }
}
