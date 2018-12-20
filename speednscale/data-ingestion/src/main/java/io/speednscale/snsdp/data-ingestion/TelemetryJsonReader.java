package io.speednscale.snsdp.data_ingestion;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Scanner;

import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.databind.DeserializationFeature;

import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry;
import io.speednscale.snsdp.snsdp_record_models.raw.data;
import io.speednscale.snsdp.snsdp_record_models.raw._mp_server_metadata;
import io.speednscale.snsdp.snsdp_record_models.raw.fyber_report;
import io.speednscale.snsdp.snsdp_record_models.raw.adjust_postback;

/**
 * This class is responsible for providing data access features for Patient table.
 */
public class TelemetryJsonReader {
	
	private Scanner input;
	private String jsonFilePath;

	/**
	 * Constructor - instantiates an object of the class.
	 * 
	 * @param csvFilePath
	 *            CSV File full path
	 */
	public TelemetryJsonReader(String telemetryFile) {
		this.jsonFilePath = telemetryFile;

		try {
			input = new Scanner(Paths.get(this.jsonFilePath));
		}
		catch (IOException ioException) {
			System.err.println("Error opening Telemetry File, file = " + this.jsonFilePath);
			System.exit(1);
		}
	}


	/**
	 * Return the next game_telemetry object read from CSV file
	 * @return game_telemetry, null if end of CSV file
	 */
	public String nextMessage() throws IOException {
		String gameTelRecord;
		if (input.hasNext()) {
			gameTelRecord = input.nextLine();
			//System.out.println("Game telemetry record: " + gameTelRecord);
		}
		else {
			input.close();
			return null;
		}
		
		return gameTelRecord;
	}
	
	/**
	 * Return the next game_telemetry object read from CSV file
	 * @return game_telemetry, null if end of CSV file
	 */
	public game_telemetry nextGameTelemetryRecord() throws IOException {
		game_telemetry gameTelObject = null;
		String gameTelRecord;
		if (input.hasNext()) {
			gameTelRecord = input.nextLine();
			//System.out.println("Game telemetry record: " + gameTelRecord);
		}
		else {
			input.close();
			return null;
		}

		byte[] gameTelAvroBytes = JsonAvroConversions.getGameTelemetryAvroBytesFromJson(gameTelRecord);
		
		
		if (gameTelAvroBytes != null)
			gameTelObject = JsonAvroConversions.getGameTelObjectFromAvroBytes(gameTelAvroBytes);

		//System.out.println("Telemetry object key (guid): " + gameTelObject.getData().getGuid());	

		return gameTelObject;
	}

	/**
	 * Return the next fyber_report object read from json file
	 * @return fyber_report object, null if end of json file
	 */
	public fyber_report nextFyberReportRecord() throws IOException {
		String fyberReportRecord;
		fyber_report fyberReportObject = null;
		
		if (input.hasNext()) {
			fyberReportRecord = input.nextLine();
		}
		else {
			input.close();
			return null;
		}
		
		byte[] fyberReportAvroBytes = JsonAvroConversions.getFyberReportAvroBytesFromJson(fyberReportRecord);
		if (fyberReportAvroBytes != null)
			fyberReportObject = JsonAvroConversions.getFyberReportObjectFromAvroBytes(fyberReportAvroBytes);
		return fyberReportObject;
	}

	/**
	 * Return the next adjust_postback object read from json file
	 * @return adjust_postback object, null if end of json file
	 */
	public adjust_postback nextAdjustPostbackRecord() throws IOException {
		adjust_postback adjustPostbackObject = null;
		String adjustPostbackRecord;
		if (input.hasNext()) {
			adjustPostbackRecord = input.nextLine();
		}
		else {
			input.close();
			return null;
		}
		
		byte[] adjustPostbackAvroBytes = JsonAvroConversions.getAdjustPostbackAvroBytesFromJson(adjustPostbackRecord);
		if (adjustPostbackAvroBytes != null)
			adjustPostbackObject = JsonAvroConversions.getAdjustPostbackObjectFromAvroBytes(adjustPostbackAvroBytes);
		
		return adjustPostbackObject;
	}
}
