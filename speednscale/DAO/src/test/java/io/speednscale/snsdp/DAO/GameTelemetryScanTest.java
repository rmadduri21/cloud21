package io.speednscale.snsdp.DAO;

import com.google.common.base.Optional;
import org.apache.hadoop.hbase.client.HConnection;


import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.speednscale.snsdp.infrastructure.HBaseUtil;
import io.speednscale.snsdp.snsdp_record_models.transformed.GameTelemetry;
import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry;
import io.speednscale.snsdp.transformations.GameTelemetryTransformer;
import io.speednscale.snsdp.DAO.GameTelemetryDao;

@SuppressWarnings("javadoc")
public class GameTelemetryScanTest {
	private static HBaseUtil hBaseUtil = null;
	private static HConnection hBaseConnection = null;
	private static GameTelemetryDao gameTelDao = null;
	private static GameTelemetryTransformer gameTelTransformer = null;


	@BeforeClass
	public static void onceExecutedBeforeAll() {
		hBaseUtil = new HBaseUtil("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181");
		hBaseConnection = hBaseUtil.getConnection();
		gameTelDao = new GameTelemetryDao(hBaseConnection);
	}	


	@AfterClass
	public static void onceExecutedAfterAll() {
		hBaseUtil.closeConnection(hBaseConnection);
	}

	@Test
	public void testScanGameTelAll() {
		Collection<GameTelemetry> gameTelCollection = null;

		try {
			gameTelCollection = gameTelDao.getGameTelemetryRecords(null, null, null, 100);
		} catch (Exception e) {
			System.err.println("IO exception while scanning table: " + gameTelDao.getTableName());
			fail();
		}

		// Validate retrieved GameTels
		//assertFalse(gameTelCollection.isEmpty());
		System.out.println("Test: testScanGameTelAll, Retrieved objects: " + gameTelCollection.size());
		for (GameTelemetry scannedGameTel : gameTelCollection) {
			GameTelemetry retrievedGameTel = null;
			try {
				retrievedGameTel = gameTelDao.getGameTelemetry(gameTelDao.getKey(scannedGameTel));
			} catch (Exception e) {
				fail();
			}
			assertFalse(scannedGameTel == null);
			assertFalse(retrievedGameTel == null);
			//assertLoadAndRetievedGameTel(scannedGameTel, retrievedGameTel);
		}
	}


	@Test
	public void testScanGameTelWithOneFilter() {
		Collection<GameTelemetry> gameTelCollection = null;

		List<Filter> filters = new ArrayList<Filter>();

		SingleColumnValueFilter colValFilter = new SingleColumnValueFilter(Bytes.toBytes("gt"), Bytes.toBytes("game")
				, CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("popdash")));
		colValFilter.setFilterIfMissing(false);
		filters.add(colValFilter);

		FilterList fl = new FilterList( FilterList.Operator.MUST_PASS_ALL, filters);

		try {
			gameTelCollection = gameTelDao.getGameTelemetryRecords(null, null, fl, 10);
		} catch (Exception e) {
			System.err.println("IO exception while scanning game_telemetry table.");
			fail();
		}

		// Validate retrieved GameTels
		System.out.println("Test: testScanGameTelWithOneFilter, Retrieved objects: " + gameTelCollection.size());
		for (GameTelemetry scannedGameTel : gameTelCollection) {
			GameTelemetry retrievedGameTel = null;
			try {
				retrievedGameTel = gameTelDao.getGameTelemetry(gameTelDao.getKey(scannedGameTel));
			} catch (Exception e) {
				fail();
			}
	        assertFalse(scannedGameTel == null);
	        assertFalse(retrievedGameTel == null);
			//assertLoadAndRetievedGameTel(scannedGameTel, retrievedGameTel);
		}
	}
	
	
	
	
	@Test
	public void testScanGameTelWithFilter() {
		Collection<GameTelemetry> gameTelCollection = null;

		List<Filter> filters = new ArrayList<Filter>();

		SingleColumnValueFilter colValFilter = new SingleColumnValueFilter(Bytes.toBytes("gt"), Bytes.toBytes("game")
				, CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("longbow")));
		colValFilter.setFilterIfMissing(false);
		filters.add(colValFilter);

		Filter colValFilter2 = new SingleColumnValueFilter(Bytes.toBytes("gt"), Bytes.toBytes("recordname")
				, CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("currency_ledger")));
		filters.add(colValFilter2);

		FilterList fl = new FilterList( FilterList.Operator.MUST_PASS_ALL, filters);

		try {
			gameTelCollection = gameTelDao.getGameTelemetryRecords(null, null, fl, 100);
		} catch (Exception e) {
			System.err.println("IO exception while scanning game_telemetry table.");
			fail();
		}

		// Validate retrieved GameTels
		System.out.println("Test: testScanGameTelWithFilter, Retrieved objects: " + gameTelCollection.size());
		for (GameTelemetry scannedGameTel : gameTelCollection) {
			GameTelemetry retrievedGameTel = null;
			try {
				retrievedGameTel = gameTelDao.getGameTelemetry(gameTelDao.getKey(scannedGameTel));
			} catch (Exception e) {
				fail();
			}
	        assertFalse(scannedGameTel == null);
	        assertFalse(retrievedGameTel == null);
			//assertLoadAndRetievedGameTel(scannedGameTel, retrievedGameTel);
		}
	}

	private boolean assertLoadAndRetievedGameTel(GameTelemetry scannedGameTel, GameTelemetry retrievedGameTel) {
        assertFalse(scannedGameTel == null);
        assertFalse(retrievedGameTel == null);
        assertFalse(gameTelDao == null);
		
		assertTrue(gameTelDao.compareGameTels(scannedGameTel, retrievedGameTel));
		return true;
	}
}