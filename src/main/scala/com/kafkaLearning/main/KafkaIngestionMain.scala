package com.kafkaLearning.main

import com.kafkaLearning.kafka.KafkaOffSetManagement.{readOffsets, saveOffsets}
import .loadToBlastStreamTables
import .loadToAccessorryStreamTables
import .loadToProductsStreamTables
import .loadToThresholdStreamTables
import .loadToholemeasurementsStreamTables
//import com.orica.rti.stream.HoleMeasurmentsStream_test.loadToholemeasurementsStreamTables
import .loadToHolesStreamTables
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime
import java.net.URI
import java.util.{Base64, Properties}



object KafkaIngestionMain {

  /* Logging configuration */

  @transient lazy val logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  /* Spark configuration  */
 val sparkConf = new SparkConf()
   .set("spark.driver.allowMultipleContexts", "true")
   .set("spark.streaming.backpressure.enabled", "true").
    set("spark.streaming.backpressure.pid.minRate", "100").
    set("spark.streaming.receiver.maxRate", "7").
    set("spark.streaming.kafka.maxRatePerPartition", "84").
    set("spark.speculation", "false").
    set("spark.shuffle.service.enabled","true").
    set("spark.eventLog.enabled","false").
    set("spark.sql.codegen.comments", "true")
    .set("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
    .set("spark.hadoop.fs.s3a.fast.upload","true")
    .set(" spark.yarn.submit.waitAppCompletion","false")
    .set("spark.sql.broadcastTimeout","36000")


  val spark = SparkSession.builder.config(sparkConf).getOrCreate()

   def main(args: Array[String])
  {
    var streamingIterationTime = args(2).toLong

    var prop = new Properties()
    prop = loadHDFSProperties(args(1).toString)

    def loadHDFSProperties(propPath: String): Properties = {

      var s3Property: FSDataInputStream = null
      val s3fs: FileSystem=FileSystem.get(new URI(propPath),spark.sparkContext.hadoopConfiguration)

      try {

        logger.info("Path taken from argument : " + propPath )
        s3Property = s3fs.open(new Path(propPath))
        prop.load(s3Property)
        s3Property.close
        prop
      } catch {
        case ex: Throwable =>
          logger.error(ExceptionUtils.getFullStackTrace(ex))
          throw ex
          return null
      } finally {
        if (s3Property != null) {
          s3Property.close()
        }
      }
    }

    logger.info(s"Initiating for the stream - " +args(0))

    try {
      val topicName = prop.getProperty("kafkaTopicPrefix") + "-" + args(0)
      val bootsrap_detail = prop.getProperty("kafkaBootStrapServers")
      val ostreamName = args(0).toString
/*
      val ssc = new StreamingContext(sc, Seconds(streamingIterationTime))
*/
      val ssc = new StreamingContext(spark.sparkContext, Seconds(streamingIterationTime))

      /**
        * Kafka consumer configs
        */
      val s3Land = ostreamName
      val consumerGroupID = "grp" + topicName

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> bootsrap_detail,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> consumerGroupID,
        "auto.offset.reset" -> "earliest", //uses this parameter if offset is not available
        "enable.auto.commit" -> (false: java.lang.Boolean)) //should be false, to use the custom offset commit from SQL table


      val fromOffsets = readOffsets(topicName,prop)

      val inputDStream = {
        if (fromOffsets != null && fromOffsets.size > 0) {
          logger.info("Reloading the offset value from Database to kafka consumer ")
          logger.info(s"Creating kafka inputstream Read from $fromOffsets")
          KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Assign[String, String](
            fromOffsets.keys, kafkaParams, fromOffsets)) //specify topic not topicset

        } else {
          logger.info("Initiating from starting position of offset")
          KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topicName.toString.split(",").toSet, kafkaParams)) //specify topic not topicset

        }
      }
      var offsetRanges = Array.empty[OffsetRange]


      inputDStream.transform(
        { Offset_rdd =>
          offsetRanges = Offset_rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          offsetRanges.foreach(offset => logger.debug(" Offset Details from SQL server" + offset.topic, offset.partition, offset.fromOffset, offset.untilOffset))
          Offset_rdd
        }).map(record => (record.value())).foreachRDD {
        consumerDataRDD =>

          logger.info("========================Process started ====================================")
          logger.info("At Start of Batch : "+DateTime.now())
          ostreamName match
          {
            /*case "products"    => loadToProductsStreamTables(consumerDataRDD,prop)
            case "sites"      => loadToThresholdStreamTables(consumerDataRDD,prop)
            case "blasts"     => loadToBlastStreamTables(consumerDataRDD,prop)
            case "holemeasurements"     => loadToholemeasurementsStreamTables(consumerDataRDD,prop)
            case "holes"      =>  loadToHolesStreamTables(consumerDataRDD,prop)
            case "accessoryplacements"      =>  loadToAccessorryStreamTables(consumerDataRDD,prop)*/
            case _            => println("Please check the stream name given is valid !!")
          }
          logger.info("End of the Batch : "+DateTime.now())
          logger.info("=====================Process completed=================================")

      }//Map closed

      inputDStream.foreachRDD(OffSet_rdd =>
        saveOffsets(OffSet_rdd, topicName, consumerGroupID,prop))

      ssc.start()
      ssc.awaitTermination()

    }
    catch {
      case ex: Throwable =>logger.error(ex)

    }

  }

}
