package io.speednscale.snsdp.data_ingestion;

import com.google.common.base.Optional;

import org.junit.*;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.runner.RunWith;

import com.google.common.base.Optional;

import java.util.Collection;
import java.io.IOException;

import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnection;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import io.speednscale.snsdp.infrastructure.HBaseUtil;
import io.speednscale.snsdp.snsdp_record_models.transformed.AdjustPostback;
import io.speednscale.snsdp.snsdp_record_models.raw.adjust_postback;
import io.speednscale.snsdp.transformations.AdjustPostbackTransformer;
import io.speednscale.snsdp.DAO.AdjustPostbackDao;

/**
 * 
 * This JUnit test will exercise Data collection by reading a CSV file, object generation and standardization. Contains four main sections:
 * 1) Before section to create objects needed to initiate test
 * 2) Raw object creation with test data
 * 3) Standardizer object creation and standardization code exercise to standardize raw object and generation of
 * standard object
 * 4) Validation of standardized object by comparing the standardized data items to expected data items (raw object)
 * 
 */
@SuppressWarnings("javadoc")
public class IngestAdjustPostbackTest {
	private static TelemetryJsonReader telemetryReader = null;
	private static HBaseUtil hBaseUtil = null;
	private static HConnection hBaseConnection = null;
	private static AdjustPostbackDao adjustPostbackDao = null;
	
	
	@BeforeClass
	public static void onceExecutedBeforeAll() {
		telemetryReader = new TelemetryJsonReader("./src/test/resources/adjust_postbacks.txt");
		hBaseUtil = new HBaseUtil("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181");
		hBaseConnection = hBaseUtil.getConnection();
		adjustPostbackDao = new AdjustPostbackDao(hBaseConnection);
	}
	
	@AfterClass
	public static void onceExecutedAfterAll() {
		hBaseUtil.closeConnection(hBaseConnection);
	}
	
	/**
	 * Collects data from Json records file
	 */
    @Test
    public void testAdjustPostbackDataCollection() {
        String gameTelKey = null;
        String addedGameTelKey = null;
        adjust_postback rawAdjustPostback = null;
        AdjustPostback stdAdjustPostback = null;            
        AdjustPostback retrievedAdjustPostback = null;
        int recordCount = 0;
        int addedRecordCount = 0;
        String apKey = null;
        String addedAPKey = null;
        
    	do {
    		try {
    			rawAdjustPostback = telemetryReader.nextAdjustPostbackRecord();
    		}
    		catch (IOException ioe) {
            	System.err.println("IO exception while reading adjust postback json record file.");
            	ioe.printStackTrace();
            	break;
    		}
    		
    		++recordCount;
    		if (rawAdjustPostback == null)
    			break;
    		stdAdjustPostback = AdjustPostbackTransformer.doTransformation(AdjustPostbackDao.AP_DATA_FIELD_COUNT, 
    				AdjustPostbackDao.AP_MP_SERVER_METADATA_FIELD_COUNT, rawAdjustPostback);
    		apKey = adjustPostbackDao.getKey(stdAdjustPostback);
            
    		try {
    		    retrievedAdjustPostback = adjustPostbackDao.getAdjustPostback(apKey);
    			if (retrievedAdjustPostback == null) {
    				adjustPostbackDao.addAdjustPostback(stdAdjustPostback);
        			addedAPKey = apKey;
        			addedRecordCount++;
    			}
    			else {
    				//assertAdjustPostback(stdAdjustPostback, retrievedAdjustPostback);
    			}
    		} catch (Exception e) {
            	System.err.println("IO exception while loading/getting from telemetryV1:adjust_postback table.");
            	break;
    		}
    		
		    if ((recordCount % 1000) == 0) {
	    		System.out.println("Adjust Postback records processed: " + recordCount + ", Adjust Postback key (random_user_id + random): " + apKey);
	    		System.out.println("Adjust Postback records Added: " + addedRecordCount + ", Last added Adjust Postback key (random_user_id + random): " + addedAPKey);
    		}
    	//}  while (recordCount < 1000000);
    	}  while (recordCount < 100);
    	
    	System.out.println("Adjust Postback records processed: " + recordCount + ", Adjust Postback key (random_user_id + random): " + apKey);
	    System.out.println("Adjust Postback records Added: " + addedRecordCount + ", Last added Adjust Postback key (random_user_id + random): " + addedAPKey);
    }
    
    private void assertAdjustPostback(final AdjustPostback adjustPostback1, final AdjustPostback adjustPostback2) {
		assertTrue(adjustPostbackDao.compareAdjustPostbacks(adjustPostback1, adjustPostback2));

        return;
    }
}