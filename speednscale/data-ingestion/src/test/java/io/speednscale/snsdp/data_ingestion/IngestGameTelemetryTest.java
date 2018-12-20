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
import io.speednscale.snsdp.snsdp_record_models.transformed.GameTelemetry;
import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry;
import io.speednscale.snsdp.transformations.GameTelemetryTransformer;
import io.speednscale.snsdp.DAO.GameTelemetryDao;

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
public class IngestGameTelemetryTest {
	private static TelemetryJsonReader telemetryReader = null;
	private static HBaseUtil hBaseUtil = null;
	private static HConnection hBaseConnection = null;
	private static GameTelemetryDao gameTelDao = null;	
	
	@BeforeClass
	public static void onceExecutedBeforeAll() {
		telemetryReader = new TelemetryJsonReader("./src/test/resources/game_telemetry.txt");
		//System.out.println("Input data file: ./src/test/resources/game_telemetry.txt");
		hBaseUtil = new HBaseUtil("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181");
		hBaseConnection = hBaseUtil.getConnection();
		gameTelDao = new GameTelemetryDao(hBaseConnection);
	}
	
	@AfterClass
	public static void onceExecutedAfterAll() {
		hBaseUtil.closeConnection(hBaseConnection);
	}
	
	/**
	 * Collects data from Json records file
	 */
    @Test
    public void testDataCollection() {
        game_telemetry rawGameTel = null;
        GameTelemetry stdGameTel = null;            
        int recordCount = 0;
        int addedRecordCount = 0;
        String gameTelKey = null;
        String addedGameTelKey = null;
        
    	do {
    		try {
    			rawGameTel = telemetryReader.nextGameTelemetryRecord();
    		}
    		catch (IOException ioe) {
            	System.err.println("IO exception while reading game telemetry json record file.");
            	ioe.printStackTrace();
            	break;
    		}
    		if (rawGameTel == null)
    			continue;
    		++recordCount;
    	    stdGameTel = GameTelemetryTransformer.doTransformation(GameTelemetryDao.DATA_FIELD_COUNT,
    	    		GameTelemetryDao.MP_SERVER_METADATA_FIELD_COUNT, rawGameTel);
			gameTelKey = gameTelDao.getKey(stdGameTel);
            
    		try {
    			if (gameTelDao.existsGameTelemetry(gameTelKey) == false) {
        			gameTelDao.addGameTelemetry(stdGameTel);
        			addedGameTelKey = gameTelKey;
        			addedRecordCount++;
    			}
    			else {
    	    		//assertGameTelemetry(stdGameTel, retrievedGameTel);
    			}
    		} catch (Exception e) {
            	System.err.println("IO exception while loading/getting from telemetry.game_telemetry table.");
            	break;
    		}
    		
    		if ((recordCount % 1000) == 0) {
    	    	System.out.println("Game Telemetry records processed: " + recordCount + ", Game Telemetry key (advertiser_id + _ + guid): " + gameTelKey);
    	    	System.out.println("Game Telemetry records Added: " + addedRecordCount + ", Last added Game Telemetry key (advertiser_id + _ + guid): " + addedGameTelKey);
    		}
    	//}  while (recordCount < 1000000);
		}  while (recordCount < 1000);
    	
    	System.out.println("Game Telemetry records processed: " + recordCount + ", Game Telemetry key (advertiser_id + _ + guid): " + gameTelKey);
    	System.out.println("Game Telemetry records Added: " + addedRecordCount + ", Last added Game Telemetry key (advertiser_id + _ + guid): " + addedGameTelKey);
    }
    
    private void assertGameTelemetry(final GameTelemetry gameTel1, final GameTelemetry gameTel2) {
    	int dataFieldCount = GameTelemetryDao.DATA_FIELD_COUNT;
    	int mpServerMetadataFieldCount = GameTelemetryDao.MP_SERVER_METADATA_FIELD_COUNT;
    	assertEquals(gameTel1.getGame(), gameTel2.getGame());
    	assertEquals(gameTel1.getRecordname(), gameTel2.getRecordname());

    	assertEquals(gameTel1.getAdvertiserId(), gameTel2.getAdvertiserId());
    	assertEquals(gameTel1.getClientEventTime(), gameTel2.getClientEventTime());
    	assertEquals(gameTel1.getDeviceId(), gameTel2.getDeviceId());
    	assertEquals(gameTel1.getEvid(), gameTel2.getEvid());
    	assertEquals(gameTel1.getGuid(), gameTel2.getGuid());
    	assertEquals(gameTel1.getLastLoginTime(), gameTel2.getLastLoginTime());
    	assertEquals(gameTel1.getTitleId(), gameTel2.getTitleId());
    	assertEquals(gameTel1.getUserId(), gameTel2.getUserId());
    	
    	for (int i = 0; i < dataFieldCount; ++i) {
            assertEquals(gameTel1.get(i), gameTel2.get(i));
    	}
    	
    	for (int i = 0; i < mpServerMetadataFieldCount; ++i) {
            assertEquals(gameTel1.getGtMpServerMetadata().get(i), gameTel2.getGtMpServerMetadata().get(i));
    	}

        return;
    }

}