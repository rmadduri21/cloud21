/*package io.speednscale.snsdp.data_ingestion

import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.language.reflectiveCalls

//Here is the sample code of a Simple Kafka consumer written in Scala. Got it working after few trial and errors.

import kafka.api.FetchRequest
import kafka.api.FetchRequestBuilder
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.Collections
import java.util.HashMap
import java.util.List
import java.util.Map
import SimpleExample._

//remove if not needed
import scala.collection.JavaConversions._

object SimpleExample {

  def main(args: Array[String]) {
    val example = new SimpleExample()
    val maxReads = java.lang.Integer.parseInt(args(0))
    val topic = args(1)
    val partition = java.lang.Integer.parseInt(args(2))
    val seeds = new ArrayList[String]()
    seeds.add(args(3))
    val port = java.lang.Integer.parseInt(args(4))
    try {
      example.run(maxReads, topic, partition, seeds, port)
    } catch {
      case e: Exception => {
        println("Oops:" + e)
        e.printStackTrace()
      }
    }
  }

  def getLastOffset(consumer: SimpleConsumer, 
      topic: String, 
      partition: Int, 
      whichTime: Long, 
      clientName: String): Long = {
    val topicAndPartition = new TopicAndPartition(topic, partition)
    val requestInfo = new HashMap[TopicAndPartition, PartitionOffsetRequestInfo]()
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1))
    val request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)
    val response = consumer.getOffsetsBefore(request)
    if (response.hasError) {
      println("Error fetching data Offset Data the Broker. Reason: " + 
        response.errorCode(topic, partition))
      return 0
    }
    val offsets = response.offsets(topic, partition)
    offsets(0)
  }
}

class SimpleExample {

  private var m_replicaBrokers: List[String] = new ArrayList[String]()

  def run(a_maxReads: Int, 
      a_topic: String, 
      a_partition: Int, 
      a_seedBrokers: List[String], 
      a_port: Int) {
    val metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition)
    if (metadata == null) {
      println("Can't find metadata for Topic and Partition. Exiting")
      return
    }
    if (metadata.leader == null) {
      println("Can't find Leader for Topic and Partition. Exiting")
      return
    }
    var leadBroker = metadata.leader.host
    val clientName = "Client_" + a_topic + "_" + a_partition
    var consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName)
    var readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime, clientName)
    var numErrors = 0
    //while (a_maxReads > 0) {
      if (consumer == null) {
        consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName)
      }
      val req = new FetchRequestBuilder().clientId(clientName).addFetch(a_topic, a_partition, readOffset, 
        100000)
        .build()
      val fetchResponse = consumer.fetch(req)
      if (fetchResponse.hasError) {
        numErrors += 1
        val code = fetchResponse.errorCode(a_topic, a_partition)
        println("Error fetching data from the Broker:" + leadBroker + 
          " Reason: " + 
          code)
        if (numErrors > 5) //break
        if (code == ErrorMapping.OffsetOutOfRangeCode) {
          readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime, clientName)
          //continue
        }
        consumer.close()
        consumer = null
        leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port)
        //continue
      }
      numErrors = 0
      var numRead = 0
      for (messageAndOffset <- fetchResponse.messageSet(a_topic, a_partition)) {
        val currentOffset = messageAndOffset.offset
        if (currentOffset < readOffset) {
          println("Found an old offset: " + currentOffset + " Expecting: " + 
            readOffset)
          //continue
        }
        readOffset = messageAndOffset.nextOffset
        val payload = messageAndOffset.message.payload
        val bytes = Array.ofDim[Byte](payload.limit())
        payload.get(bytes)
        println(String.valueOf(messageAndOffset.offset) + ": " + new String(bytes, "UTF-8"))
        numRead += 1
       // a_maxReads -= 1
      }
      if (numRead == 0) {
        try {
          Thread.sleep(1000)
        } catch {
          case ie: InterruptedException => 
        }
      }
    //}
    if (consumer != null) consumer.close()
  }

  private def findNewLeader(a_oldLeader: String, 
      a_topic: String, 
      a_partition: Int, 
      a_port: Int): String = {
    for (i <- 0 until 3) {
      var goToSleep = false
      val metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition)
      if (metadata == null) {
        goToSleep = true
      } else if (metadata.leader == null) {
        goToSleep = true
      } else if (a_oldLeader.equalsIgnoreCase(metadata.leader.host) && i == 0) {
        goToSleep = true
      } else {
        return metadata.leader.host
      }
      if (goToSleep) {
        try {
          Thread.sleep(1000)
        } catch {
          case ie: InterruptedException => 
        }
      }
    }
    println("Unable to find new leader after Broker failure. Exiting")
    throw new Exception("Unable to find new leader after Broker failure. Exiting")
  }

  private def findLeader(a_seedBrokers: List[String], 
      a_port: Int, 
      a_topic: String, 
      a_partition: Int): PartitionMetadata = {
    var returnMetaData: PartitionMetadata = null

    for (seed <- a_seedBrokers) {
      var consumer: SimpleConsumer = null
      try {
        consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup")
        val topics = Collections.singletonList(a_topic)
        val req = new TopicMetadataRequest(topics)
        val resp = consumer.send(req)
        val metaData = resp.topicsMetadata
        for (item <- metaData; part <- item.partitionsMetadata){
          if (part.partitionId == a_partition) {
          returnMetaData = part
         //break
        }
        }
      } catch {
        case e: Exception => println("Error communicating with Broker [" + seed + "] to find Leader for [" + 
          a_topic + 
          ", " + 
          a_partition + 
          "] Reason: " + 
          e)
      } finally {
        if (consumer != null) consumer.close()
      }
    }
    if (returnMetaData != null) {
      m_replicaBrokers.clear()
      for (replica <- returnMetaData.replicas) {
        m_replicaBrokers.add(replica.host)
      }
    }
    returnMetaData
  }
}*/