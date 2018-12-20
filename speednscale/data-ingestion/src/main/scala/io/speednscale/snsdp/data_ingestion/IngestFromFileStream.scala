// scalastyle:off println
package io.speednscale.snsdp.data_ingestion

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.log4j.{Level, Logger}

import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HConnection
import io.speednscale.snsdp.infrastructure.HBaseUtil
import io.speednscale.snsdp.snsdp_record_models.transformed.GameTelemetry
import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry
import io.speednscale.snsdp.transformations.GameTelemetryTransformer
import io.speednscale.snsdp.DAO.GameTelemetryDao

/**
 * Consumes messages from a data file and inserts into a HBase table in telemetry1 database
 * Usage: IngestDataFromFile <data_file_path> <table_name>
 *   <data_file_path> is the full path to the data file on the local host
 *   <table_name> is the table name in the database telemetry1
 *
 * Example:
 *    $ bin/run-example IngestDataFromFile /user/rao/Data/maxpubst-1/game_telemetry.txt game_telemetry
 */
object IngestFromFileStream {

	def main(args: Array[String]) {
		if (args.length < 2) {
	    	System.err.println(s"""
	        	| Usage: IngestDataFromFile <data_file_path> <table_name>
				| <data_file_path> is the full path to the data file on the local host
	        	|<table_name> is the table name in the database telemetry1
	        	|
	        	""".stripMargin)
	      	System.exit(1)
	    }
    
    	StreamingData.setStreamingLogLevels()
    	
    	val sparkConf = new SparkConf().setAppName("TelemetryDataLoadFromFile")
    	// Create context with 120 second batch interval
    	val ssc = new StreamingContext(sparkConf, Seconds(120))
    
    	// Create the FileInputDStream on the directory and use the
    	// stream to perform telemetry ETL
   		val lines = ssc.textFileStream(args(0))   
    	lines.print()	// Print 10 lines for each RDD
    	
    	lines.foreachRDD { rdd =>
  			rdd.foreachPartition { partitionOfRecords =>
  			    val hBaseUtil = new HBaseUtil("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181");
				val hBaseConnection = hBaseUtil.getConnection()
				val gameTelDao = new GameTelemetryDao(hBaseConnection)
				//var recordCount: Int = 0;
				//var addedRecordCount:Int = 0;
				val logger = Logger.getLogger("IngestFromFile")
				val metrics = new StreamingMetrics(0,0)
  			
    			partitionOfRecords.foreach(record => transformAndLoad(gameTelDao, logger, metrics, record, args(1)))
      	        println("Game Telemetry records processed: " + metrics.getRecordCount() + 
      	            ", added to HBase: " + metrics.getAddedRecordCount())     			
	       		logger.debug("Game Telemetry records processed: " + metrics.getRecordCount())
				logger.debug("Game Telemetry records Added: " + metrics.getAddedRecordCount())
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
					logger: Logger, metrics: StreamingMetrics, message: String, tableName: String) = {
		var gameTelObject: game_telemetry = null
       	//logger.debug("Game Telemetry record: " + message)
		val gameTelAvroBytes = JsonAvroConversions.getGameTelemetryAvroBytesFromJson(message);
		
		metrics.incRecordCount()	
		if (gameTelAvroBytes != null) {
			gameTelObject = JsonAvroConversions.getGameTelObjectFromAvroBytes(gameTelAvroBytes)
	      	//logger.debug("Telemetry object key (guid): " + gameTelObject.getData().getGuid())
		
		}
		else {
			logger.debug("Bad game telemetry record: " + message)
		}

		if (gameTelObject != null) {
			val stdGameTel = GameTelemetryTransformer.doTransformation(GameTelemetryDao.DATA_FIELD_COUNT,
			          GameTelemetryDao.MP_SERVER_METADATA_FIELD_COUNT, gameTelObject)
		
			val gameTelKey = gameTelDao.getKey(stdGameTel)
        
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