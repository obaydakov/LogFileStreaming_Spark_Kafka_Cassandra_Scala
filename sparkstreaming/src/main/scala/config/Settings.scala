package config

import com.typesafe.config.ConfigFactory

/**
  * Created by deepu_us on 4/16/2017.
  */
object Settings {

  private val config = ConfigFactory.load()

  object WebLogGen{
    private val webLogGen = config.getConfig("clickstream")
    lazy val records = webLogGen.getInt("records")
    lazy val timeMultiplier = webLogGen.getInt("time_multiplier")
    lazy val pages = webLogGen.getInt("pages")
    lazy val visitors = webLogGen.getInt("visitors")
    lazy val filePath = webLogGen.getString("file_path")
    lazy val destPath = webLogGen.getString("dest_path")
    lazy val numberOfFiles = webLogGen.getInt("number_of_files")
    lazy val kafkaTopic = webLogGen.getString("kafka_topic")
    lazy val hdfsPath = webLogGen.getString("hdfs_path")
  }
}
