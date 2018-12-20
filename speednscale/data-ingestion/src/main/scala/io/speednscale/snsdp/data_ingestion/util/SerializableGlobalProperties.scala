package com.speednscale.snsdp.data_ingestion.util


import com.typesafe.config.ConfigFactory
import java.util.Properties
import java.io.FileInputStream

/**
 * Utility to Provide environment specific application properties.
 */
object SerializableGlobalProperties extends Serializable{
  
  private val properties = new Properties
  @transient lazy val props:Properties =  {
    try {

    properties.load(this.getClass.getClassLoader.getResourceAsStream("app.properties"))
    properties

    } catch { case e: Exception => 
         SerializableLogger.log.error(e)
      sys.exit(1)
    }
    
    
  }
  

  
}