package com.speednscale.snsdp.data_ingestion.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import scala.collection.JavaConversions._

object HbaseConfigurationWithAppProperties {
  
  /**
   * Generates a hbase configuration with application properties.
   * 
   * 
   */
  def getConf():Configuration = {
   var hbaseConf =  HBaseConfiguration.create()
   			SerializableGlobalProperties.props.entrySet().foreach(entry =>
			hbaseConf.set(entry.getKey.toString(), entry.getValue.toString())
			)
			return hbaseConf
  }
  
  /**
   * Utility to Load application Properties into the configuration provided.
   * 
   * @param conf Configuration to which application properties need to be added.
   */
  def loadAppProperties(conf: Configuration) = {
      SerializableGlobalProperties.props.entrySet().foreach(entry =>
			conf.set(entry.getKey.toString(), entry.getValue.toString())
			)
  }
}