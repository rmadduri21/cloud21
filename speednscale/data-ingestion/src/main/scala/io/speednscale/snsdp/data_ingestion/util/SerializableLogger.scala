package com.speednscale.snsdp.data_ingestion.util

import org.apache.log4j.Logger

/**
 * 
 * Serializable log4j logger
 * This class will provide a serializable logger for spark jobs.
 */
object SerializableLogger extends Serializable {      
   @transient lazy val log = Logger.getLogger(getClass.getName)
}