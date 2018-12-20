package io.speednscale.snsdp.transformations;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

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
import io.speednscale.snsdp.snsdp_record_models.raw.adjust_postback;
import io.speednscale.snsdp.snsdp_record_models.raw._mp_server_metadata;
import io.speednscale.snsdp.snsdp_record_models.transformed.AdjustPostback;
import io.speednscale.snsdp.snsdp_record_models.transformed.ap__mp_server_metadata;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * 
 * This class and its methods transform and validate a raw game telemetry object and generate a validated and standardized
 * AdjustPostback object.
 * 
 */
@ParametersAreNonnullByDefault
public class AdjustPostbackTransformer {

	public static AdjustPostback doTransformation(final int dataFieldCount, 
			final int mpServerMetadataFieldCount, adjust_postback rawAdjustPostback) {

		//Transform metadata
		ap__mp_server_metadata mpServerMetadata = ap__mp_server_metadata.newBuilder().build();

		// For now we are not doing any transformations, preserving the data received as is
		for (int i = 0; i < mpServerMetadataFieldCount; ++i) {
			mpServerMetadata.put(i, rawAdjustPostback.getMpServerMetadata$1().get(i));
		}
        AdjustPostback.Builder apBuilder = AdjustPostback.newBuilder()
				.setApMpServerMetadata(mpServerMetadata)
				.setRecordname(rawAdjustPostback.getRecordname())
				.setGame(rawAdjustPostback.getGame())
				.putAllExtended_data_fields(rawAdjustPostback.getExtendedDataFields());
		
		//Transform initial data part of the record
		AdjustPostback apObject = apBuilder.build();

		// For now we are not doing any transformations, preserving the data received as is
		for (int i = 0; i < dataFieldCount; ++i) {
			apObject.put(i, rawAdjustPostback.get(i));
		}
				
		return apObject;
	}
}