// scalastyle:off println
package io.speednscale.snsdp.data_ingestion

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HConnection
import io.speednscale.snsdp.infrastructure.HBaseUtil
import io.speednscale.snsdp.snsdp_record_models.transformed.GameTelemetry
import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry
import io.speednscale.snsdp.transformations.GameTelemetryTransformer
import io.speednscale.snsdp.DAO.GameTelemetryDao
import java.io.IOException

import scala.util.control._
import org.apache.log4j.{Level, Logger}


/**
 * Consumes messages from a data file and inserts into a HBase table in telemetry1 database
 * Usage: IngestDataFromFile <data_file_path> <table_name>
 *   <data_file_path> is the full path to the data file on the local host
 *   <table_name> is the table name in the database telemetry1
 *
 * Example:
 *    $ bin/run-example IngestDataFromFile /home/rao_madduri/Data/maxpubst-1/game_telemetry.txt game_telemetry
 */
object IngestFromFile {
    var recordCount = 0
    var addedRecordCount = 0
    
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
    	
        val telemetryReader = new TelemetryJsonReader(args(0))
  	    val hBaseUtil = new HBaseUtil(
  	            "zookeeper-01.c.platform-production.internal," + "zookeeper-02.c.platform-production.internal," +
  	            "zookeeper-03.c.platform-production.internal", "2181")
		    val hBaseConnection = hBaseUtil.getConnection()
		    val gameTelDao = new GameTelemetryDao(hBaseConnection)
 	
        var rawGameTel: String= null
        val logger: Logger = Logger.getRootLogger();
        
        // create a Breaks object as follows
        val loop = new Breaks;
        
        val recordsToSkip = if (args.length > 2) args(2).toInt else 0
        
        if (recordsToSkip > 0) {
          for (skipCount <- 0 until recordsToSkip) {
            rawGameTel = telemetryReader.nextMessage()
          }
          println("Records skipped = " + recordsToSkip)
        }
        
    	  loop.breakable {
            do {
        		    try {
        			      rawGameTel = telemetryReader.nextMessage()
        		    }
        		    catch {
        		      case ioe: IOException => System.err.println("IO exception while reading game telemetry json record file.")
                	                         ioe.printStackTrace()
        		      case ise: java.lang.IllegalStateException => System.out.print("End of input, file = " + args(0))
                	loop.break
        		    }
        		    if (rawGameTel != null) {
            		    recordCount += 1
        
            		    transformAndLoad(gameTelDao, logger, rawGameTel, args(1))
            		    if ((recordCount % 10000) == 0) {
            	    	    System.out.println("Game Telemetry records processed: " + recordCount)
            	    	    System.out.println("Game Telemetry records Added: " + addedRecordCount)
            		    }
        		    }
        	  //}  while (recordCount < 10000000)
    		    }  while (recordCount < 100)
    		      
    	  }
    	
    	  System.out.println("Game Telemetry records processed: " + recordCount)
    	  System.out.println("Game Telemetry records Added: " + addedRecordCount)
    			
	      //logger.debug("Game Telemetry records processed: " + recordCount)
				//logger.debug("Game Telemetry records Added: " + addedRecordCount)
				hBaseUtil.closeConnection(hBaseConnection)
  	}
  
	  def transformAndLoad(gameTelDao:GameTelemetryDao,
					  logger: Logger, message: String, tableName: String) = {
		    var gameTelObject: game_telemetry = null
		    val gameTelAvroBytes = JsonAvroConversions.getGameTelemetryAvroBytesFromJson(message)	
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
				        //val retrievedGameTel = gameTelDao.getGameTelemetry(gameTelKey)
				        //if (retrievedGameTel == null) {
			          if ( !gameTelDao.existsGameTelemetry(gameTelKey) ) {
	    			        gameTelDao.addGameTelemetry(stdGameTel)
	    			        addedRecordCount += 1
			      	      //logger.debug("Game Telemetry record added to HBase: " + gameTelKey)
				        }
				        else {
		    		        //assertGameTelemetry(stdGameTel, retrievedGameTel)
				        }
			      } catch {
                case e: Exception => {
                      logger.debug("IO exception while loading/getting from telemetry1:game_telemetry table, key = " + gameTelKey)
                      e.printStackTrace()
                }
			      }
		    }
    }
}