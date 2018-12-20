package io.speednscale.snsdp.infrastructure

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.ZooKeeperConnectionException
import org.apache.hadoop.hbase.MasterNotRunningException

/**
 * This class is responsible for providing access to HBase.
 */
class HBaseUtil(hBaseIpAddress: String, hBasePort: String) {
  val HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
  val HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort"
  // val config: Configuration = HBaseConfiguration.create(new Configuration())
  val config: Configuration = HBaseConfiguration.create()

  def fixture() = new {
    //println("zookeeper configuration quorum: " + config.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM))
    //println("zookeeper configuration clientport: " + config.get(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT))
    val zookeeperQuorum = config.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM)

    //println("Setting zookeepr information...")
    config.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hBaseIpAddress)
    config.set("zookeeper.znode.parent", "/hbase-unsecure")
    //println("Setting zookeepr information...")
    config.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hBasePort)
    config.set("hbase.master", "master-01.c.platform-production.internal" + ":16000");

    //println("HBase master: " + config.get("hbase.master"))
    //println("HBase master port: " + config.get("hbase.master.port"))
    //println("zookeeper configuration quorum: " + config.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM))
    //println("zookeeper configuration clientport: " + config.get(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT))

    try {
      HBaseAdmin.checkHBaseAvailable(config)
      //println("Check if HBase running test passed...")
    }
    catch  {
      case mne: MasterNotRunningException => println("Master not running...")
      case zce: ZooKeeperConnectionException => println("Unable to connect to ZooKeeper...")
    }
  }

  def getConnection(): HConnection = {
    val fix = fixture()
    try {
      //println("Before creating HBase connection...")
      //println("zookeeper configuration quorum: " + config.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM))
      //println("zookeeper configuration clientport: " + config.get(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT))
      val connection = HConnectionManager.createConnection(config)
      return connection
    } catch {
      case ioEx: IOException => {
        println("IO exception while opening a HBase connection")
        println("IO exception: " + ioEx.getMessage())
        ioEx.printStackTrace()
        return null
      }
    }
  }

  def getHBaseAdmin(hConnection: HConnection): HBaseAdmin = {
    new HBaseAdmin(hConnection)
  }

  def closeConnection(connectionToClose: HConnection): Boolean = {
    connectionToClose.close()
    return true
  }
}
