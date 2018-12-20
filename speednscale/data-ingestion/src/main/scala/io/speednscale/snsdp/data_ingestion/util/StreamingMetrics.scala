// scalastyle:off println
package com.speednscale.snsdp.data_ingestion.util


/**
 * Class for counters.
 */
class StreamingMetrics (initialRecordCount:Int = 0, initialAddedRecordCount:Int =0, intialFailedRecordCount:Int =0) {
	var recordCount = initialRecordCount;
	var addedRecordCount = initialAddedRecordCount;
	var failedRecordCount = intialFailedRecordCount;
	def incRecordCount() = {
	  recordCount += 1
	}
	
	def incAddedRecordCount() = {
	  addedRecordCount += 1
	}
	def incFailedRecordCount() = {
	  failedRecordCount += 1
	}
	def getRecordCount(): Int = {
	    recordCount
  }
	
	def getAddedRecordCount(): Int = {
	  addedRecordCount
	}
	def getFailedRecordCount():Int = {
	  failedRecordCount
	}
	
}