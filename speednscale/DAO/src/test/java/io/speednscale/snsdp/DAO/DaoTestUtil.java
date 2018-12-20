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
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.nio.file.Paths;
import java.util.Scanner;

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
import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry;
import io.speednscale.snsdp.snsdp_record_models.raw.adjust_postback;
import io.speednscale.snsdp.snsdp_record_models.raw.data;
import io.speednscale.snsdp.snsdp_record_models.raw._mp_server_metadata;

@SuppressWarnings("javadoc")
public class DaoTestUtil {
	
	private static final String stringData = new String("123456789");
	private static final long longValue = 100l;
	private static final String dateString = "2015-12-29T16:41:36.000Z";
	
	public static game_telemetry buildRawGameTelemetry(String recordName, String gameName) {

		data rawData = data.newBuilder().build();
		int dataFieldCount = GameTelemetryDao.DATA_FIELD_COUNT;

		for (int i = 0; i < dataFieldCount; ++i) {
			switch (i) {
			case 5:
			case 6:
			case 12:
			case 13:
			case 14:
			case 15:
			case 17:
			case 19:
			case 20:
			case 27:
			case 29:
			case 35:
			case 36:
			case 37:
			case 40:
			case 49:
			case 51:
				rawData.put(i, longValue);
				break;
			case 11:
			case 26:
			case 33:
			case 34:
			case 50:
			case 52:
				rawData.put(i, dateString);
				break;
			case 9:
			case 30:
			case 31:
			case 32:
			case 43:
				rawData.put(i, true);
				break;
			case 28:
				rawData.put(i, UUID.randomUUID().toString());
				break;
			case 53:
				rawData.put(i, "Rao Madduri");
				break;
			default:
				rawData.put(i, stringData);
				break;
			}
		}

		_mp_server_metadata.Builder mpServerMetadataBuilder = _mp_server_metadata.newBuilder()
				.setMpHeaderContentType$1(stringData)
				.setMpServerTimestamp$1(stringData)
				.setMpHeaderAcceptEncoding$1(stringData)
				.setMpHeaderConnection$1(stringData)
				.setMpRemoteAddress$1(stringData)
				.setMpQueryString$1(stringData)
				.setMpHeaderHost$1(stringData)
				.setMpRequestUri$1(stringData)
				.setMpHeaderContentLength$1(longValue)
				.setMpHeaderUserAgent$1(stringData)
				.setMpHeaderXUnityVersion$1(stringData)
				.setServerTimestamp(dateString);
		
		_mp_server_metadata mpServerMetadata = mpServerMetadataBuilder.build();
		
		Map<String, String> extendedDataFields = new HashMap<String, String>();
		extendedDataFields.put("key1", "value1");

		game_telemetry.Builder gtBuilder = game_telemetry.newBuilder()
				.setData(rawData)
				.setMpServerMetadata$1(mpServerMetadata)
				.setRecordname(recordName)
				.setGame(gameName)
				.putAllExtended_data_fields(extendedDataFields);
		return gtBuilder.build();
	}
	
	public static adjust_postback buildRawAdjustPostback(String recordName, String gameName) {
		int dataFieldCount = AdjustPostbackDao.getDataFieldCount();

		_mp_server_metadata.Builder mpServerMetadataBuilder = _mp_server_metadata.newBuilder()
				.setMpHeaderContentType$1(stringData)
				.setMpServerTimestamp$1(stringData)
				.setMpHeaderAcceptEncoding$1(stringData)
				.setMpHeaderConnection$1(stringData)
				.setMpRemoteAddress$1(stringData)
				.setMpQueryString$1(stringData)
				.setMpHeaderHost$1(stringData)
				.setMpRequestUri$1(stringData)
				.setMpHeaderContentLength$1(longValue)
				.setMpHeaderUserAgent$1(stringData)
				.setMpHeaderXUnityVersion$1(stringData)
				.setServerTimestamp(stringData);
		
		_mp_server_metadata mpServerMetadata = mpServerMetadataBuilder.build();
		
		Map<String, String> extendedDataFields = new HashMap<String, String>();
		extendedDataFields.put("key1", "value1");
		
		adjust_postback.Builder apBuilder = adjust_postback.newBuilder()
				.setMpServerMetadata$1(mpServerMetadata)
				.setRecordname(recordName)
				.setGame(gameName)
				.putAllExtended_data_fields(extendedDataFields);
		
		adjust_postback rawAdjustPostback = apBuilder.build();
		
		for (int i = 0; i < dataFieldCount; ++i) {
			rawAdjustPostback.put(i, stringData);
		}

		rawAdjustPostback.put(45, UUID.randomUUID().toString());
		
		return rawAdjustPostback;
	}
	
	public static Scanner startCSVReader(String csvDataFile) {
		Scanner input = null;
		try {
			input = new Scanner(Paths.get(csvDataFile));
		}
		catch (IOException ioException) {
			System.err.println("Error opening CSV data File, file = " + csvDataFile);
			System.exit(1);
		}
		return input;
	}


	/**
	 * Return the next record read from CSV file
	 * @return data record, null if end of CSV file
	 */
	public static String nextRecord(Scanner input) throws IOException {
		String nextRecord;
		if (input.hasNext()) {
			nextRecord = input.nextLine();
			//System.out.println("Game telemetry record: " + gameTelRecord);
		}
		else {
			input.close();
			return null;
		}
		
		return nextRecord;
	}
}