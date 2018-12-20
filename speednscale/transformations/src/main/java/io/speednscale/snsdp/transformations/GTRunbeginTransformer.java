package io.speednscale.snsdp.transformations;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.google.common.base.Optional;
import io.speednscale.snsdp.snsdp_record_models.raw.RawGTRunbegin;
import io.speednscale.snsdp.snsdp_record_models.transformed.GTRunbegin;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * 
 * This class and its methods transform and validate a raw game telemetry object and generate a validated and standardized
 * GameTelemetry object.
 * 
 */
@ParametersAreNonnullByDefault
public class GTRunbeginTransformer {
	private static final int FIELD_COUNT = 18;
	
	public static GTRunbegin doTransformation(final RawGTRunbegin rawGTRunbegin) {

		GTRunbegin.Builder gtRunbeginBuilder = GTRunbegin.newBuilder();

		GTRunbegin gtRunbegin = gtRunbeginBuilder.build();
		

		for (int i = 0; i < FIELD_COUNT; ++i) {
			switch (i) {
				case 3:
					gtRunbegin.put(i, rawGTRunbegin.get(i));
					Date date = new Date(Long.parseLong(rawGTRunbegin.getClientEventTime().get()));
					DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
					gtRunbegin.put(18, formatter.format(date));
					break;
				case 6:
					gtRunbegin.put(i, rawGTRunbegin.get(i));
					date = new Date(Long.parseLong(rawGTRunbegin.getFirstPlayTime().get()));
					formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
					gtRunbegin.put(19, formatter.format(date));
					break;
				default:
					gtRunbegin.put(i, rawGTRunbegin.get(i));
					break;
			}
		}
		
		return gtRunbegin;
	}
}