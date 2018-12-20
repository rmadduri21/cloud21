package io.speednscale.snsdp.transformations;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.joda.time.DateTime;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;

import com.google.common.base.Optional;
import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry;
import io.speednscale.snsdp.snsdp_record_models.raw.data;
import io.speednscale.snsdp.snsdp_record_models.raw._mp_server_metadata;
import io.speednscale.snsdp.snsdp_record_models.transformed.GameTelemetry;
import io.speednscale.snsdp.snsdp_record_models.transformed.gt__mp_server_metadata;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * 
 * This class and its methods transform and validate a raw game telemetry object and generate a validated and standardized
 * GameTelemetry object.
 * 
 */
@ParametersAreNonnullByDefault
public class GameTelemetryTransformer {
	
	private static final int SERVER_TIMESTAMP_COL = 11;
	
	public static GameTelemetry doTransformation(final int dataFieldCount, 
			final int mpServerMetadataFieldCount, game_telemetry rawGameTelemetry) {

		// Copy _mp_server_metadata
		
		gt__mp_server_metadata.Builder mpServerMetadataBuilder = gt__mp_server_metadata.newBuilder();
		gt__mp_server_metadata mpServerMetadata = mpServerMetadataBuilder.build();
		
		// Fill in mp_server_metadata fields
		for (int i = 0; i < mpServerMetadataFieldCount; ++i) {
			if (i == SERVER_TIMESTAMP_COL) {
				//System.out.println("Metadata server timestamp: " + (String)(rawGameTelemetry.getMpServerMetadata$1().get(i)));
				String dateString = (String)(rawGameTelemetry.getMpServerMetadata$1().get(i));
				if ((dateString != "") && (dateString != null)) {
					//DateTime dateAndTime = DateTime.parse(dateString);
					//long timeInMillis = dateAndTime.getMillis();
					//gameTel.put(i, Long.parseLong((String)(rawGameTelemetry.getData().get(i))));
					mpServerMetadata.put(i, DateTime.parse(dateString).getMillis());
				}
			}
			else {
				mpServerMetadata.put(i, rawGameTelemetry.getMpServerMetadata$1().get(i));
			}
		}
		
		GameTelemetry.Builder gtBuilder = GameTelemetry.newBuilder()
				.setRecordname(rawGameTelemetry.getRecordname())
				.setGame(rawGameTelemetry.getGame())
				.setGtMpServerMetadata(mpServerMetadata)
				.putAllExtended_data_fields(rawGameTelemetry.getExtendedDataFields());

		GameTelemetry gameTel = gtBuilder.build();
		
		// For now we are not doing any transformations, preserving the data received as is
		for (int i = 0; i < dataFieldCount; ++i) {
			switch (i) {
				case 11:
				case 26:
				case 33:
				case 34:
				case 52:
					String dateString = (String)(rawGameTelemetry.getData().get(i));
					DateTime dateAndTime = null;
					long timeInMillis = 0l;
					if ((dateString != "") && (dateString != null)) {
						try {
							dateAndTime = DateTime.parse(dateString);
							timeInMillis = dateAndTime.getMillis();
						}
						catch (IllegalArgumentException iae) {
							System.out.println("Data field: " + i + ", " + dateString);
							System.out.println("IllegalArgumentException: " + iae.getMessage());
						}
						catch (Exception e) {
							System.out.println(" Unknown Exception: " + e.getMessage());
						}
					}
					//gameTel.put(i, Long.parseLong((String)(rawGameTelemetry.getData().get(i))));
					gameTel.put(i, timeInMillis);
					break;
				default:
					gameTel.put(i, rawGameTelemetry.getData().get(i));
					break;
			}
			//System.out.println("Transformed GameTelemetry column: " + i + " , Value: " + gameTel.get(i));
		}
		
		return gameTel;
	}
}