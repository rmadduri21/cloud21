package com.speednscale.snsdp.data_ingestion.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SerializableWritable
import org.apache.hadoop.io.Text

/**
 * Singleton broadcast of brokers. Required when using hbase context.
 */
object BrokersBroadcast {
   @volatile private var instance: Broadcast[SerializableWritable[Text]] = null

   /**
    * provides a singleton broadcast instance for Serializable Text.
    * 
    * @param sc Spark Context
    * @param broadcastParam Parameter to be broadcasted.
    */
  def getInstance(sc: SparkContext, broadcastParam:String): Broadcast[SerializableWritable[Text]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(new SerializableWritable(new Text(broadcastParam)))
        }
      }
    }
    instance
  }
}