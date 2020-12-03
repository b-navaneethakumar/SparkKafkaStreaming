name := "KafkaIngestionFramework"
version := "1.0"
scalaVersion := "2.11.12"

val kafkaVersion="2.0.0"
val SparkVersion="2.4.0"
val scalaVers="2.11.12"

//unmanagedJars in Compile += file("///C:/Development Activity/External Jars/azure-sqldb-spark-1.0.2.jar")
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies ++= Seq(
  //Jars required for http connection
  "org.apache.httpcomponents" % "httpclient" % "4.5.6",
  "org.scala-lang" % "scala-library" % scalaVers,

  //Jars required for loggin purpose
  "org.apache.logging.log4j" % "log4j-api" % "2.11.2",
  

  //Scala related jars
  "org.scalatest" %% "scalatest" % "3.0.0" % Test,
  "org.scalactic" %% "scalactic" % "3.0.0",
  

  //Spark Related Jars
  "org.apache.spark" %% "spark-catalyst" % SparkVersion % Test,
  "org.apache.spark" %% "spark-core" % SparkVersion  % "provided",
  "org.apache.spark" %% "spark-hive" % SparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % SparkVersion  % "provided",
  "org.apache.spark" %% "spark-streaming" % SparkVersion ,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % SparkVersion ,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0" % "provided",


  //XML processing related jars
  "com.databricks" %% "spark-xml" % "0.5.0",

  //Kafka related jars
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion ,

  //Other compile time dependecnies
  "com.yammer.metrics" % "metrics-core" % "2.2.0",
  "com.google.protobuf" % "protobuf-java" % "2.4.1",
  "commons-codec" % "commons-codec" % "1.4",
  "joda-time" % "joda-time" % "2.9.9",

  //SQL Server related jars ********MAKE SURE  --driver-class-path mssql-jdbc-6.2.2.jre8.jar IS GIVEN IN SPARK-SUBMIT COMMAND
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.2.jre8" ,
  "com.microsoft.azure" % "azure-sqldb-spark" % "1.0.2",

  //AWS related jars
  "org.apache.hadoop" % "hadoop-aws" % "2.6.5",
  "org.apache.hadoop" % "hadoop-client" % "2.8.5",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.469",
  

  //Azure EventHUB related Jars
  "com.microsoft.azure" %% "azure-eventhubs-spark" % "2.3.1",
  "com.microsoft.azure" % "azure-eventhubs" % "1.0.1",
  "org.apache.qpid" % "proton-j" % "0.25.0",
  "com.microsoft.azure" %% "azure-eventhubs-databricks" % "3.4.0",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "com.typesafe" % "config" % "1.3.1",
  "mrpowers" % "spark-daria" % "2.2.0_0.12.0"

)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


