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
import io.speednscale.snsdp.snsdp_record_models.transformed.DauInstalled

import scala.util.control._
import scala.language.reflectiveCalls
import org.apache.log4j.{Level, Logger}


object LoadFromCSV {
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
		    
		    val dauInstalledDao = new DauInstalledDao(fix.hBaseConnection)
	    	println("dauInstalledDao created")
		
    		var recordCount = 0
    		var addedRecordCount = 0
    		var dauInstalledRecord: String = null
    		var dauInstalled: DauInstalled = null
    		var retrievedDauInstalled: DauInstalled = null
		
    		do {
            		  
            if (csvInput.hasNext()) {
            	dauInstalledRecord = csvInput.nextLine();
            	//System.out.println("Game telemetry record: " + gameTelRecord);
            }
            else {
            	csvInput.close();
            	dauInstalledRecord = null;
            }
            
            if (dauInstalledRecord != null) {
                //println("DauInstalled record: " + dauInstalledRecord)
          		  dauInstalled = dauInstalledDao.buildDauInstalledFromCSVLine(dauInstalledRecord)
          		  //println("DauInstalled object created, key = " + dauInstalledDao.getKey(dauInstalled))
          		  recordCount += 1
          		
          		  retrievedDauInstalled = dauInstalledDao.getDauInstalled(dauInstalledDao.getKey(dauInstalled))
          		  
          		  if (retrievedDauInstalled == null) {
          		      dauInstalledDao.addDauInstalled(dauInstalled)
          		      addedRecordCount += 1
          		  }
            }
            else {
              // exit loop
              recordCount = 1000
            }
            
            if ((recordCount % 100) == 0) {
                println("DauInstalled records processed: " + recordCount +
                    ", " + dauInstalled.getSumInstalled() +
                    ", " + dauInstalled.getCountAdvertisers() +
                    ", " + dauInstalled.getCountDevices() + 
                    ", " + dauInstalled.getCountMessages()
                    )
                println("DauInstalled records added: " + addedRecordCount)
            }
  
  		  } while (recordCount < 10000)
    
    		println("DauInstalled records processed: " + recordCount)
        println("DauInstalled records added: " + addedRecordCount)
    		
    		
    		val dauInstalledObjects = dauInstalledDao.getDauInstalledRecords(null,null,null,100)
          
    		fix.hBaseUtil.closeConnection(fix.hBaseConnection)
    		if (csvInput != null) {
              csvInput.close()
              csvInput = null
        }
    		println("DauInstalled objects retrieved: " + dauInstalledObjects.size())
  	}
		
}