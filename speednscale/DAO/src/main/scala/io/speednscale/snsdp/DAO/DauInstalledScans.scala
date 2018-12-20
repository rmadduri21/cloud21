package io.speednscale.snsdp.DAO
// scalastyle:off println

import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HConnection
import io.speednscale.snsdp.infrastructure.HBaseUtil
import io.speednscale.snsdp.snsdp_record_models.transformed.GameTelemetry
import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry
import io.speednscale.snsdp.transformations.GameTelemetryTransformer
import java.io.IOException

import scala.util.control._
import org.apache.log4j.{Level, Logger}
import com.google.common.base.Optional
import org.apache.hadoop.hbase.client.HConnection


import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.QualifierFilter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.FamilyFilter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.ValueFilter
import org.apache.hadoop.hbase.util.Bytes

import java.io.IOException
import java.util.Collection
import java.util.ArrayList
import java.util.List

import io.speednscale.snsdp.infrastructure.HBaseUtil
import io.speednscale.snsdp.snsdp_record_models.transformed.DauInstalled

object DauInstalledScans {
  
	  def main(args: Array[String]) {
    		val hBaseUtil = new HBaseUtil(
    				"zookeeper-01.c.platform-production.internal," +
    				"zookeeper-02.c.platform-production.internal," +
    				"zookeeper-03.c.platform-production.internal", "2181")
    		val hBaseConnection = hBaseUtil.getConnection()
    		val dauInstalledDao = new DauInstalledDao(hBaseConnection)
    
    		//var dauInstalledCollection: List[DauInstalled] = null
    		var dauInstalledCollection = dauInstalledDao.getDauInstalledRecords(null,null,null,100)
    
 /*   		try {
    			var dauInstalledCollection = dauInstalledDao.getDauInstalledRecords(null,null,null,100)
    		} catch {
    		 	case ioe: IOException => println("IO exception while scanning table: " + dauInstalledDao.getTableName())		
    			case e: Exception => println("Exception while scanning table: " + dauInstalledDao.getTableName())
    		}
*/    
    		// Validate retrieved DauInstalled aggregates
    		println("Retrieved objects with no filter: " + dauInstalledCollection.size())
    
        var filterList: FilterList = new FilterList() 
    
    /*		
    	Scan scan = new Scan(Bytes.toBytes(0), Bytes.toBytes(10)) 
        FilterList filterList = new FilterList() 
        SingleColumnValueFilter filter = new SingleColumnValueFilter( 
                cfBytes, 
                qBytes, 
                CompareFilter.CompareOp.GREATER_OR_EQUAL, 
                Bytes.toBytes(1)) 
        filterList.addFilter(filter) 
        filter = new SingleColumnValueFilter( 
                cfBytes, 
                qBytes, 
                CompareFilter.CompareOp.LESS, 
                Bytes.toBytes(Integer.MIN_VALUE)) 
        filterList.addFilter(filter) 
        scan.setFilter(filterList) 
    */
    	  var colValFilter: SingleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("dau"), 
    				Bytes.toBytes("sum_installed"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(100))
    		colValFilter.setFilterIfMissing(false)
    		filterList.addFilter(colValFilter)
    		
    		colValFilter = new SingleColumnValueFilter( 
                Bytes.toBytes("dau"), 
                Bytes.toBytes("sum_installed"),
                CompareFilter.CompareOp.LESS, 
                Bytes.toBytes(Integer.MIN_VALUE))
        	colValFilter.setFilterIfMissing(false)
        	filterList.addFilter(colValFilter)
    
    		try {
    			dauInstalledCollection = dauInstalledDao.getDauInstalledRecords(null, null, filterList, 10)
    		} catch {
    		 	case ioe: IOException => println("IO exception while scanning table: " + dauInstalledDao.getTableName())		
    			case e: Exception => println("Exception while scanning table: " + dauInstalledDao.getTableName())
    		}
    
    		// Validate retrieved DauInstalled aggregates
    		//assertFalse(dauInstalledCollection.isEmpty())
    		println("Retrieved objects with no filter: " + dauInstalledCollection.size())
    		
     		hBaseUtil.closeConnection(hBaseConnection)
	  }
}