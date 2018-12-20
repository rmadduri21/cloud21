package io.speednscale.snsdp.infrastructure

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HBaseAdmin

object ConnectToHBase {

  def main(args: Array[String]) {
    val ipAddress = args(0)
    val port = args(1)
    val hBaseUtil = new HBaseUtil(ipAddress, port)
    val hBaseConnection = hBaseUtil.getConnection()

    val admin = hBaseUtil.getHBaseAdmin(hBaseConnection)

    var exists  = admin.tableExists("telemetry_v1:game_telemetry")
    println("Table: telemetry_v1:game_telemetry exists - " + exists)

    exists  = admin.tableExists("telemetry_v1:adjust_postback")
    println("Table: telemetry_v1:adjust_postback exists - " + exists)

    exists  = admin.tableExists("telemetry_v1:fyber_report")
    println("Table: telemetry_v1:fyber_report exists - " + exists)

    hBaseUtil.closeConnection(hBaseConnection)
  }
}