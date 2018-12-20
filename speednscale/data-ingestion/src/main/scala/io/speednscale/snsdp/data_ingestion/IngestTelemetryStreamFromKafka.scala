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
import io.speednscale.snsdp.snsdp_record_models.transformed.TelemetryEvent
import io.speednscale.snsdp.snsdp_record_models.raw.event_v_1_0
import io.speednscale.snsdp.transformations.TelemetryEventTransformer
import io.speednscale.snsdp.DAO.TelemetryEventDao



/**
 * Consumes messages from one or more topics in Kafka and then performs ETL and loads into HBase.
 * Usage: IngestTelemetryFromKafkaStrem <zkQuorum> <group> <topics> <numThreads>
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
object IngestTelemetryStreamFromKafka {
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
	        	| Usage: IngestTelemetryStreamFromKafka <kafka_broker_list> <topics_list>
				    | <kafka_broker_list> is a list of kafka brokers example: fprd-kafka-01.c.platform-production.internal:6667,prd-kafka-02.c.platform-production.internal:6667,prd-kafka-03.c.platform-production.internal:6667
	        	| <topics_list> one or more comma separated list of topics, example: test_telemetry_avro
	        	|
	        	""".stripMargin)
	      	System.exit(1)
	    }
    
    	StreamingData.setStreamingLogLevels()
    	
    	val sparkConf = new SparkConf().setAppName("IngestTelemetryStreamFromKafka")
    	
    	// Create context with 120 second batch interval
    	val ssc = new StreamingContext(sparkConf, Seconds(120))
    	ssc.checkpoint("checkpoint2")
    	
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
      messages.print()
    	
    	
    	
      val lines = messages.map(_._2)
    
    	//val recordCount = ssc.accumulator(0, "Record Count")
    	//val addedRecordCount = ssc.accumulator(0, "Added Record Count")
    
    	// Create the FileInputDStream on the directory and use the
    	// stream to count words in new files created
   		//val lines = ssc.textFileStream(args(0))
    
    	// lines.foreach(line => transformAndLoad(line, args(1)))
    	lines.print()	// Print 10 lines for each RDD
    	lines.foreachRDD { rdd =>
    			rdd.foreachPartition { partitionOfRecords =>
        		  	val hBaseUtil = new HBaseUtil("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181");
      				val hBaseConnection = hBaseUtil.getConnection()
      				val telemetryEventDao = new TelemetryEventDao(hBaseConnection)
      				//var recordCount: Int = 0;
      				//var addedRecordCount:Int = 0;
      				val logger = Logger.getLogger("IngestFromKafkaStream")
      				val metrics = new StreamingMetrics(0,0)
        			
          			partitionOfRecords.foreach(record => transformAndLoad(telemetryEventDao, logger, metrics, record))
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
  
	def transformAndLoad(telemetryEventDao: TelemetryEventDao,
					logger: Logger, metrics: StreamingMetrics, message: Array[Byte]) = {
		var telemetryEventObject: event_v_1_0 = null
        //logger.debug("Game Telemetry record: " + message)
		//val gameTelAvroBytes = JsonAvroConversions.getGameTelemetryAvroBytesFromJson(message);
  		
  		metrics.incRecordCount()

  		if (message != null) {
  			    telemetryEventObject = JsonAvroConversions.getTelemetryEventObjectFromAvroBytes(message)
  	            logger.debug("Telemetry object key (guid): " + telemetryEventObject.getEventGuid())
  		
  		}
  		else {
  			    logger.debug("Bad telemetry event record, count in the batch: " + metrics.getRecordCount())
  		}
  
  		if (telemetryEventObject != null) {
  			val stdTelemetryEventObject = TelemetryEventTransformer.doTransformation(telemetryEventObject)
  		
  			val telemetryEventKey = telemetryEventDao.getKey(stdTelemetryEventObject)
/*
  			if (metrics.getRecordCount() % 10 == 0) {
      			println("Records processed in the current batch for this RDD: " + metrics.getRecordCount() + " Telemetry Event object key: " + telemetryEventKey)
      		}
*/
  			try {
  				val retrievedTelemetryEvent = telemetryEventDao.getTelemetryEvent(telemetryEventKey)
  				if (retrievedTelemetryEvent == null) {
  	    			telemetryEventDao.addTelemetryEvent(stdTelemetryEventObject)
  	    			metrics.incAddedRecordCount()
  			      	//logger.debug("Telemetry event record added to HBase: " + telemetryEventKey)
  				}
  				else {
  		    		//assertGameTelemetry(stdGameTel, retrievedGameTel)
  				}
  			} catch {
  	        	    case e: Exception => logger.debug("IO exception while loading/getting from telemetry:telemetry_event table.")
  			}
  		}
    }
}