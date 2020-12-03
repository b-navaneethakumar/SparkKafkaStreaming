package com.kafkaLearning.utils

import java.util.{ Properties}
import org.apache.commons.lang.exception.ExceptionUtils
import org.joda.time.DateTime
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame


object WriteToS3 {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)


  def writeDFtoS3(resultsDF: DataFrame, s3Land: String,prop:Properties): Unit = {

     val BUCKET_NAME = prop.getProperty("awsS3BucketPrefix")+s3Land

    try{
      logger.info("=================================Loading into S3 =========================")
      logger.info("At Start of Batch : "+DateTime.now())

      val DF_sizeCheck = resultsDF.isEmpty

      if (!DF_sizeCheck)
      {
        val distinctValuesDF = resultsDF.select(resultsDF("EType")).distinct
        val distinctEtypeValuesList = distinctValuesDF.select("EType").rdd.map(r => r(0)).collect()

              for(  value <- distinctEtypeValuesList)
              {
                 resultsDF.select(resultsDF.col("*")).filter(resultsDF("EType")=== value).coalesce(1).write.format("org.apache.spark.sql.json").mode(SaveMode.Append).save("s3n:"+ BUCKET_NAME +"/"+ value)
                logger.info("Uploaded batch of Json into S3 :"+ BUCKET_NAME+"/"+ value)
              }
        }
      else
      {
        logger.info("DataFrame Looks Empty")

      }
      logger.info("End of the Batch : "+DateTime.now())
      logger.info("=================================Loading completed into S3====================")
    }
    catch {case ex: Throwable =>
      logger.error(ExceptionUtils.getFullStackTrace(ex)) }


  }
  def writeDFtoS3History(resultsDF: DataFrame, s3Land: String,prop:Properties): Unit = {

    val BUCKET_NAME = prop.getProperty("awsS3BucketPrefix")+s3Land

    try{
      logger.info("=================================Loading into S3 =========================")
      logger.info("At Start of Batch : "+DateTime.now())

      val DF_sizeCheck = resultsDF.isEmpty

      if (!DF_sizeCheck)
      {

          resultsDF.coalesce(1).write.format("org.apache.spark.sql.json").mode(SaveMode.Append).save("s3n:"+ BUCKET_NAME +"/"+"History")
          logger.info("Uploaded batch of Json into S3 :"+ BUCKET_NAME+"/")

      }
      else
      {
        logger.info("DataFrame Looks Empty")

      }
      logger.info("End of the Batch : "+DateTime.now())
      logger.info("=================================Loading completed into S3====================")
    }
    catch {case ex: Throwable =>
      logger.error(ExceptionUtils.getFullStackTrace(ex)) }


  }



  }
