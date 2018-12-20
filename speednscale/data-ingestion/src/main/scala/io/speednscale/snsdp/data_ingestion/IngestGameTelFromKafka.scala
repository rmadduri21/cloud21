// scalastyle:off println
package io.speednscale.snsdp.data_ingestion


//import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}

import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HConnection
import io.speednscale.snsdp.infrastructure.HBaseUtil
import io.speednscale.snsdp.snsdp_record_models.transformed.GameTelemetry
import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry
import io.speednscale.snsdp.transformations.GameTelemetryTransformer
import io.speednscale.snsdp.DAO.GameTelemetryDao



/**
 * Consumes messages from one or more topics in Kafka and then performs ETL and loads into HBase.
 * Usage: IngestFromKafkaStrem <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      io.speednscale.snsdp.data_ingestion.IngestFromKafkaStream zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 */
object IngestGameTelFromKafka {
    //val hBaseUtil = new HBaseUtil("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181");
	//val hBaseConnection = hBaseUtil.getConnection()
	//val gameTelDao = new GameTelemetryDao(hBaseConnection)
	//val gameTelTransformer = new GameTelemetryTransformer(GameTelemetryDao.getDataFieldCount(), GameTelemetryDao.getMpServerMetadataFieldCount())
	//var recordCount = 0;
	//var addedRecordCount = 0;
	//val logger = Logger.getLogger("IngestFromKafkaStream")

	def main(args: Array[String]) {
		if (args.length < 2) {
	    	System.err.println(s"""
	        	| Usage: IngestFromKafkaStream <data_file_path> <table_name>
				| <data_file_path> is the full path to the data file on the local host
	        	|<table_name> is the table name in the database telemetry1
	        	|
	        	""".stripMargin)
	      	System.exit(1)
	    }
    
    	StreamingData.setStreamingLogLevels()
    	
    	val sparkConf = new SparkConf().setAppName("TelemetryDataLoadFromKafka")
    	
    	// Create context with 120 second batch interval
    	val ssc = new StreamingContext(sparkConf, Seconds(120))
    	ssc.checkpoint("checkpoint")
    	
    	/*
    	// Create kafka stream
    	val Array(zkQuorum, group, topics, numThreads) = args
    	val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
      */
      
      // Create direct kafka stream with brokers and topics
      val Array(brokers, topics) = args
      val topicsSet = topics.split(",").toSet
      println("Topics: " + topics)
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
      println("Kafka brokers: " + brokers)
      //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      val messages = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
      //messages.print()
    	
    	
    	
      val lines = messages.map(_._2)
    
    	//val recordCount = ssc.accumulator(0, "Record Count")
    	//val addedRecordCount = ssc.accumulator(0, "Added Record Count")
    
    	// Create the FileInputDStream on the directory and use the
    	// stream to count words in new files created
   		//val lines = ssc.textFileStream(args(0))
    
    	// lines.foreach(line => transformAndLoad(line, args(1)))
    	//lines.print()	// Print 10 lines for each RDD
    	lines.foreachRDD { rdd =>
    			rdd.foreachPartition { partitionOfRecords =>
        		  val hBaseUtil = new HBaseUtil("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181");
      				val hBaseConnection = hBaseUtil.getConnection()
      				val gameTelDao = new GameTelemetryDao(hBaseConnection)
      				//var recordCount: Int = 0;
      				//var addedRecordCount:Int = 0;
      				val logger = Logger.getLogger("IngestFromKafkaStream")
      				val metrics = new StreamingMetrics(0,0)
        			
          		partitionOfRecords.foreach(record => transformAndLoad(gameTelDao, logger, metrics, record, args(1)))
      	      println("Game Telemetry records processed: " + metrics.getRecordCount() + 
      	            ", added to HBase: " + metrics.getAddedRecordCount())          			
      	      //logger.debug("Game Telemetry records processed: " + metrics.getRecordCount())
      				//logger.debug("Game Telemetry records Added: " + metrics.getAddedRecordCount())
      				hBaseUtil.closeConnection(hBaseConnection)
    			}
    			    
         		//System.out.println("Game Telemetry records processed: " + recordCount)
      		//System.out.println("Game Telemetry records Added: " + addedRecordCount)
  		   	//Thread.sleep(120000)
		  }
		    
      //System.out.println("Game Telemetry records processed: " + recordCount)
    	//System.out.println("Game Telemetry records Added: " + addedRecordCount)
    	
    	ssc.start()
    	ssc.awaitTermination()
    
       	//System.out.println("Game Telemetry records processed: " + recordCount)
    	//System.out.println("Game Telemetry records Added: " + addedRecordCount)
		//hBaseUtil.closeConnection(hBaseConnection)
	}
  
	  def transformAndLoad(gameTelDao:GameTelemetryDao,
					logger: Logger, metrics: StreamingMetrics, message: Array[Byte], tableName: String) = {
		    var gameTelObject: game_telemetry = null
        //logger.debug("Game Telemetry record: " + message)
		    //val gameTelAvroBytes = JsonAvroConversions.getGameTelemetryAvroBytesFromJson(message);
  		
  		  metrics.incRecordCount()

  		  if (message != null) {
  			    gameTelObject = JsonAvroConversions.getGameTelObjectFromAvroBytes(message)
  	        //logger.debug("Telemetry object key (guid): " + gameTelObject.getData().getGuid())
  		
  		   }
  		  else {
  			    logger.debug("Bad game telemetry record, count in the batch: " + metrics.getRecordCount())
  		  }
  
  		  if (gameTelObject != null) {
  			     val stdGameTel = GameTelemetryTransformer.doTransformation(GameTelemetryDao.DATA_FIELD_COUNT,
  			             GameTelemetryDao.MP_SERVER_METADATA_FIELD_COUNT, gameTelObject)
  		
  			    val gameTelKey = gameTelDao.getKey(stdGameTel)
/*
  			    if (metrics.getRecordCount() % 10 == 0) {
      			    println("Records processed in the current batch for this RDD: " + metrics.getRecordCount() + " Game Telemetry object key: " + gameTelKey)
      		  }
*/
  			     try {
  				      val retrievedGameTel = gameTelDao.getGameTelemetry(gameTelKey)
  				      if (retrievedGameTel == null) {
  	    			      gameTelDao.addGameTelemetry(stdGameTel)
  	    			      metrics.incAddedRecordCount()
  			      	    //logger.debug("Game Telemetry record added to HBase: " + gameTelKey)
  				      }
  				      else {
  		    		      //assertGameTelemetry(stdGameTel, retrievedGameTel)
  				      }
  			    } catch {
  	        	    case e: Exception => logger.debug("IO exception while loading/getting from telemetry.game_telemetry table.")
  			    }

  		  }
    }
}