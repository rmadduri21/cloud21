package io.speednscale.snsdp.DAO

import java.util.Collection
import java.nio.file.Paths
import java.util.Scanner
import java.io.IOException;

import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HConnection

import com.google.common.base.Optional
import com.google.inject.Inject
import com.google.inject.assistedinject.Assisted

import io.speednscale.snsdp.infrastructure.HBaseUtil
import io.speednscale.snsdp.snsdp_record_models.transformed.GTRunbegin
import io.speednscale.snsdp.snsdp_record_models.raw.RawGTRunbegin
import io.speednscale.snsdp.transformations.GTRunbeginTransformer

import scala.util.control._
import scala.language.reflectiveCalls
import org.apache.log4j.{Level, Logger}


object GTRunbeginLoadFromCSV {
    var csvInput: Scanner = null;
  
  	def fixture(ipAddress: String, port: String) = new {
  		//Connect to HBase 
  		val hBaseUtil = new HBaseUtil(ipAddress, port)
  		val hBaseConnection = hBaseUtil.getConnection()
  	}

	      
	  def main(args: Array[String]) {
		    if (args.length < 2) {
	    	    System.err.println(s"""
	        	| Usage: LoadFromCSV <data_file_path> <table_name>
				    | <data_file_path> is the full path to the data file on the local host
	        	|<table_name> is the table name in the database analytics
	        	|
	        	""".stripMargin)
	      	System.exit(1)
	      }
		    
		    try {
			      csvInput = new Scanner(Paths.get(args(0)));
		    }
		    catch {
		        case ioException: IOException => {
			          println("Error opening CSV data File, file = " + args(0));
			          System.exit(1);
		        }
		    }
		        
    	 // StreamingData.setStreamingLogLevels()
		    val fix = fixture("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181")
		    println("Obtained hBaseConnection")
		    
		    val gtRunbeginDao = new GTRunbeginDao(fix.hBaseConnection)
	    	println("gtRunbeginDao created")
		
    		var recordCount = 0
    		var addedRecordCount = 0
    		var recordFromFile: String = null
    		var rawGTRunbegin: RawGTRunbegin = null
    		var gtRunbegin: GTRunbegin = null
    		var retrievedGTRunbegin: GTRunbegin = null
		
    		do {
            		  
            if (csvInput.hasNext()) {
            	recordFromFile = csvInput.nextLine();
            	//System.out.println("Game telemetry record: " + gameTelRecord);
            }
            else {
            	csvInput.close();
            	recordFromFile = null;
            }
            
            if (recordFromFile != null) {
                //println("GTRunbegin record: " + recordFromFile)
          		  rawGTRunbegin = gtRunbeginDao.buildRawGTRunbeginFromCSVLine(recordFromFile)
          		  //println("GTRunbegin object created, key = " + gtRunbeginDao.getKey(gtRunbegin))
          		  recordCount += 1
          		  
          		  if (rawGTRunbegin != null) {
          		      gtRunbegin = GTRunbeginTransformer.doTransformation(rawGTRunbegin)
          		  
          		      retrievedGTRunbegin = gtRunbeginDao.getGTRunbegin(gtRunbeginDao.getKey(gtRunbegin))
          		  
              		  if (retrievedGTRunbegin == null) {
              		      gtRunbeginDao.addGTRunbegin(gtRunbegin)
              		      addedRecordCount += 1
              		  }
          		  }
            }
            else {
              // exit loop
              recordCount = 1000
            }
            
            if ((recordCount % 1000) == 0) {
                println("GTRunbegin records processed: " + recordCount +
                    ", RunbeginCountStar" + gtRunbegin.getBeginCountStar() +
                    ", RunbeginCountUsers" + gtRunbegin.getBeginCountUsers()
                    )
                println("GTRunbegin records added: " + addedRecordCount)
            }
  
  		  } while (recordCount < 10000)
    
    		println("GTRunbegin records processed: " + recordCount)
        println("GTRunbegin records added: " + addedRecordCount)
    		
    		val gtRunbeginObjects = gtRunbeginDao.getGTRunbeginRecords(null,null,null,100)
          
    		fix.hBaseUtil.closeConnection(fix.hBaseConnection)
    		if (csvInput != null) {
              csvInput.close()
              csvInput = null
        }
    		println("GTRunbegin objects retrieved: " + gtRunbeginObjects.size())
  	}
}