// scalastyle:off println
package io.speednscale.snsdp.data_ingestion

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
 *    $ bin/run-example IngestDataFromFile /home/rao_madduri/Data/maxpubst-1/game_telemetry.txt game_telemetry
 */
class StreamingMetrics (initialRecordCount:Int = 0, initialAddedRecordCount:Int =0) {
	var recordCount = initialRecordCount;
	var addedRecordCount = initialAddedRecordCount;

	def incRecordCount() = {
	  recordCount += 1
	}
	
	def incAddedRecordCount() = {
	  addedRecordCount += 1
	}
	
	def getRecordCount(): Int = {
	    recordCount
  }
	
	def getAddedRecordCount(): Int = {
	  addedRecordCount
	}
	
}