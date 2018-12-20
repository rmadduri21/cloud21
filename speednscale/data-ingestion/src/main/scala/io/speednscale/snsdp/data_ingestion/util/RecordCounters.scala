package com.speednscale.snsdp.data_ingestion.util

import org.apache.spark.SparkContext
import org.apache.spark.Accumulator

/**
 * utility for counters.
 */
object RecordCounters {
  
	@volatile private var successRecords: Accumulator[Long] = null
	@volatile private var failedRecords: Accumulator[Long] = null
	@volatile private var totalRecods: Accumulator[Long] = null
	
	/**
	 * Provides an Accumulator for success records for a given context.
	 * 
	 * @param sc Spark Context.
	 */
  def getSuccessRecordsInstance(sc: SparkContext): Accumulator[Long] = {
			if (successRecords == null) {
				synchronized {
					if (successRecords == null) {
						successRecords = sc.accumulator(0l,"SUCCESS_RECORDS");
					}
				}
			}
			successRecords
	}
	
	/**
	 * Provides an accumulator for failed records.
	 * 
	 * @param sc Spark Context.
	 */
	def getFailedRecordsInstance(sc: SparkContext): Accumulator[Long] = {
			if (failedRecords == null) {
				synchronized {
					if (failedRecords == null) {
						failedRecords = sc.accumulator(0l,"FAILED_RECORDS");
					}
				}
			}
			failedRecords
	}

	/**
	 * Provides an accumulator for total records.
	 * 
	 * @param sc Spark Context.
	 */
	def getTotalRecordsInstance(sc: SparkContext): Accumulator[Long] = {
			if (totalRecods == null) {
				synchronized {
					if (totalRecods == null) {
						totalRecods = sc.accumulator(0l,"TOTAL_RECORDS");
					}
				}
			}
			totalRecods
	}
}