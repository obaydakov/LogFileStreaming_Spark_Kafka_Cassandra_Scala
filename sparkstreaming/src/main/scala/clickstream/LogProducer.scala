package clickstream

import java.io.FileWriter
import java.util.{Properties, Random}

import config.Settings
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
/**
  * Created by deepu_us on 4/16/2017.
  * code for generating log file and streaming it using Kafka.
  */
object LogProducer extends App{

  val wlc = Settings.WebLogGen
  val Products = scala.io.Source.fromInputStream(getClass().getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass().getResourceAsStream("/referrers.csv")).getLines().toArray
  val Visitors = (0 to wlc.visitors).map("Visitor-"+ _)
  val Pages =(0 to wlc.pages).map("Page-"+ _)
  val rnd = new Random()
  //Kafka Properties

  val topic = wlc.kafkaTopic
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "locahost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "WebLogProducer")
  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  //val filepath = wlc.filePath
  //val destPath = wlc.destPath

  for(fileCount <- 1 to wlc.numberOfFiles) {
    //val fw = new FileWriter(filepath, true)
    // introduce a bit of randomness to time increments
    val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedtimestamp = timestamp
    for (iteration <- 1 to wlc.records) {
      adjustedtimestamp = adjustedtimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
      timestamp = System.currentTimeMillis()
      val action = iteration % (rnd.nextInt(200) + 1) match {
        case 0 => "purchase"
        case 1 => "add_to_cart"
        case _ => "page_view"
      }
      val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
      val prevpage = referrer match {
        case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
        case _ => ""
      }
      val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
      val page = Pages(rnd.nextInt(Pages.length - 1))
      val product = Products(rnd.nextInt(Products.length - 1))

      val line = s"$adjustedtimestamp\t$referrer\t$action\t$prevpage\t$visitor\t$page\t$product\n"

      val producerRecord = new ProducerRecord(topic, line)
      kafkaProducer.send(producerRecord)
      //fw.write(line)
      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages !")
        val sleeping = rnd.nextInt(1500)
        println(s"sleeping for $sleeping ms")
        Thread sleep sleeping
      }
    }
    /*fw.close()
    val outputFile = FileUtils.getFile(s"$destPath data_$timestamp")
    println(s"moving produced data to $outputFile")
    FileUtils.moveFile(FileUtils.getFile(filepath),outputFile)*/

    val sleeping = 5000
    println(s"sleeping for $sleeping ms")

  }
  kafkaProducer.close()
}
