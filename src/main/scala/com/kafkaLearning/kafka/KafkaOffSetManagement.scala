package com.kafkaLearning.kafka

import java.util.{Calendar, Properties}

import com.kafkaLearning.main.KafkaIngestionMain.spark
import org.apache.spark.rdd.RDD
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
import com.microsoft.azure.sqldb.spark.query._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
object KafkaOffSetManagement {

  @transient lazy val logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  def saveOffsets(rdd: RDD[_], TOPIC_NAME:String,GROUP_ID:String,prop:Properties)
  {
    logger.info("Initiated saveOffSet ")
    val  offsetRangesNew = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (o <- offsetRangesNew) {
       val topic =   o.topic
      val kpartition = o.partition
      val fromOffset = o.fromOffset
      val toOffset = o.untilOffset

      val updateOffset  = s"""
                             |UPDATE KafkaOffSetMgmt
                             |SET fromoffset = $fromOffset , untilloffset= $toOffset
                             |WHERE topic = '$topic' AND partition = '$kpartition' ;
                        """.stripMargin

       val Updateconfig = Config(Map(
        "url"            -> prop.getProperty("jdbcHostname"),
        "databaseName"   -> prop.getProperty("Offset_jdbcDatabase"),
        "user"           -> prop.getProperty("username"),
        "password"       -> prop.getProperty("password"),
        "queryCustom"  -> updateOffset
      ))

      spark.sqlContext.sqlDBQuery(Updateconfig)

    }
    logger.info("Updated OffSet value to KafkaOffSetMgmt table @ " + Calendar.getInstance().getTime)
   }




  def  readOffsets(TOPIC_NAME:String,prop:Properties): Map[TopicPartition, Long]  = {
/*
    Configs to get partition details from Zookeeper kafka directly
 */
/*  ------ OffSetMgmnt Table----------------*/

val WriteOffSetMgmtTableConfig = Config(Map(
  "url"            -> prop.getProperty("jdbcHostname"),
  "databaseName"   -> prop.getProperty("Offset_jdbcDatabase"),
  "user"           -> prop.getProperty("username"),
  "password"       -> prop.getProperty("password"),
  "dbTable"        -> prop.getProperty("OffSet_tbl_name"),
  "connectTimeout" -> "5",
  "queryTimeout"   -> "5"
))

    var configProperties = new Properties()
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,prop.getProperty("kafkaBootStrapServers"))
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,prop.getProperty("KEY_SERLZR"))
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,prop.getProperty("VALUE_SERLZR"))

    var producer = new KafkaProducer(configProperties)
     val zKNumberOfPartitionsForTopic = producer.partitionsFor(TOPIC_NAME).size()

    /*val zkKafkaRootDir = prop.getProperty("KAFKA_ROOT_DIR")
    val zkSessionTimeOut = prop.getProperty("zkSessionTimeOut").toInt
    val zkConnectionTimeOut = prop.getProperty("zkConnectionTimeOut")
    val zkQuorum = prop.getProperty("zkQuorum")

    val zkWatcher = new Watcher {
      override def process(event: WatchedEvent): Unit = logger.info("watcher created for Zookeper!")
    }

    val zk = new ZooKeeper(zkQuorum, zkSessionTimeOut,zkWatcher)
    val zkNodeName = s"/brokers/topics/$TOPIC_NAME/partitions"
    val zKNumberOfPartitionsForTopic = zk.getChildren(zkNodeName, false).size
    zk.close()*/
   logger.info("Initiated ReadOffset method from Kafka Consumer ")
   logger.info("Number of partition count for the topic :" +TOPIC_NAME+": in Zookeper is -" + zKNumberOfPartitionsForTopic)
   logger.info("Reading offset from SQL Server - " + prop.getProperty("OffSet_tbl_name"))

  /*
     SQL connection to get partition details from SQL server
   */


    val KafkaOffsetMgmtTblDF = spark.read.sqlDB(WriteOffSetMgmtTableConfig).select("topic","partition","fromoffset","untilloffset")
    val SQLServerNumberOfPartitionsForTopic_DF = KafkaOffsetMgmtTblDF.filter( KafkaOffsetMgmtTblDF("topic")=== s"$TOPIC_NAME").agg(max("partition").as("maxpartition"))

    try {
              var fromOffsets = collection.mutable.Map[TopicPartition, Long]()
              val maxpartitionDF = SQLServerNumberOfPartitionsForTopic_DF.select("maxpartition").filter("maxpartition is not null")
              val DF_sizeCheck = maxpartitionDF.take(1).isEmpty
                     if(DF_sizeCheck )
                    {
                       logger.info("No Entry found for the given TOPIC in SQL server.Adding new entry for the topic ")
                      import spark.implicits._
                        for (partition <- 0 until zKNumberOfPartitionsForTopic)
                        {
                          import spark.implicits._
                          val FreshOffsetEntryDF = Seq((s"${TOPIC_NAME}",s"${partition}",0,0)).toDF("topic","partition","fromoffset","untilloffset")
                          FreshOffsetEntryDF.write.mode(SaveMode.Append).sqlDB(WriteOffSetMgmtTableConfig)


                        }
                       logger.info("Loaded new topic entry into table ")
                     }
                      else {

                        val SQLServerNumberOfPartitionsForTopic = SQLServerNumberOfPartitionsForTopic_DF.first().getInt(0)

                        logger.info(s"SQL Partitions for the topic : $SQLServerNumberOfPartitionsForTopic and Zookeeper Partitions for the topic is $zKNumberOfPartitionsForTopic ")

                        /*
                           handle scenario where new partitions have been added to existing kafka topic
                             */

                            if ((zKNumberOfPartitionsForTopic-1) > SQLServerNumberOfPartitionsForTopic)
                            {
                              logger.info("looks like new partition has been added for the topic : " + TOPIC_NAME)
                              for (partition <- 0 to SQLServerNumberOfPartitionsForTopic)
                              {

                                val UntilOffset_DF = KafkaOffsetMgmtTblDF.filter( KafkaOffsetMgmtTblDF("topic")=== s"$TOPIC_NAME").filter(KafkaOffsetMgmtTblDF("partition")=== s"$partition").agg(max("untilloffset").as("maxuntilloffset"))
                                fromOffsets += (new TopicPartition(TOPIC_NAME, partition.toInt) -> UntilOffset_DF.first().getInt(0))
                              }

                              for (partition <- (SQLServerNumberOfPartitionsForTopic + 1) until zKNumberOfPartitionsForTopic)
                              {
                                fromOffsets += (new TopicPartition(TOPIC_NAME, partition.toInt) -> 0)
                                import spark.implicits._
                                val FreshOffsetEntryDF = Seq((s"${TOPIC_NAME}",s"${partition}",0,0)).toDF("topic","partition","fromoffset","untilloffset")
                                FreshOffsetEntryDF.write.mode(SaveMode.Append).sqlDB(WriteOffSetMgmtTableConfig)

                              }
                              logger.info("New entry for the new partitions were added into table")
                            }
                        /*--------------Initialize fromOffsets from previous run-------------------------- */
                                else {

                                  for (partition <- 0 to SQLServerNumberOfPartitionsForTopic)
                                  {
                                    val UntilOffset_query = s"(select max(untilloffset) as maxoffset from ${prop.getProperty("OffSet_tbl_name")} where topic='${TOPIC_NAME}' AND partition='${partition}' ) pq"
                                    val UntilOffset_DF = KafkaOffsetMgmtTblDF.filter( KafkaOffsetMgmtTblDF("topic")=== s"$TOPIC_NAME").filter(KafkaOffsetMgmtTblDF("partition")=== s"$partition").agg(max("untilloffset").as("maxuntilloffset"))
                                    fromOffsets += (new TopicPartition(TOPIC_NAME, partition.toInt) -> UntilOffset_DF.first().getInt(0))

                                  }
                                }
                        logger.info(fromOffsets)
                      }
                       fromOffsets.toMap
        } // Closing try block
  catch {

      case ex: Throwable =>       ex.printStackTrace()
        var fromOffsets = collection.mutable.Map[TopicPartition, Long]()
      fromOffsets.toMap
  }

  } //closing read offset Method



 }
