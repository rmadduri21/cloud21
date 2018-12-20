package io.speednscale.snsdp.transformations;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.joda.time.DateTime;

import com.google.common.base.Optional;
import io.speednscale.snsdp.snsdp_record_models.raw.event_v_1_0;
import io.speednscale.snsdp.snsdp_record_models.raw.tools_event_data_v_1_0;
import io.speednscale.snsdp.snsdp_record_models.raw.platform_event_data_v_1_0;
import io.speednscale.snsdp.snsdp_record_models.raw.tools_action_event_detail_v_1_0;
import io.speednscale.snsdp.snsdp_record_models.raw.platform_response_event_detail_v_1_0;
import io.speednscale.snsdp.snsdp_record_models.raw.event_type;
import io.speednscale.snsdp.snsdp_record_models.raw.tools_event_type;
import io.speednscale.snsdp.snsdp_record_models.raw.platform_event_type;
import io.speednscale.snsdp.snsdp_record_models.transformed.TelemetryEvent;
import io.speednscale.snsdp.snsdp_record_models.transformed.ToolsEventData;
import io.speednscale.snsdp.snsdp_record_models.transformed.ToolsActionEventDetail;
import io.speednscale.snsdp.snsdp_record_models.transformed.PlatformEventData;
import io.speednscale.snsdp.snsdp_record_models.transformed.PlatformResponseEventDetail;
import io.speednscale.snsdp.snsdp_record_models.transformed.EventType;
import io.speednscale.snsdp.snsdp_record_models.transformed.ToolsEventType;
import io.speednscale.snsdp.snsdp_record_models.transformed.PlatformEventType;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * 
 * This class and its methods transform and validate a raw game telemetry object and generate a validated and standardized
 * EventV10 object.
 * 
 */
@ParametersAreNonnullByDefault
public class TelemetryEventTransformer {
	
	private static int EVENT_FIELD_COUNT = 16;
	private static int PLATFORM_EVENT_FIELD_COUNT = 11;
	private static int TOOLS_EVENT_FIELD_COUNT = 11;
	private static int TOOLS_ACTION_EVENT_DETAIL_FIELD_COUNT = 3;
	private static int PLATFORM_RESPONSE_EVENT_DETAIL_FIELD_COUNT = 3;
	
	
	public static TelemetryEvent doTransformation(event_v_1_0 rawEvent) {
		
		TelemetryEvent telemetryEvent = null;
		Object eventData = null;
		ToolsEventData toolsEventData = null;
		PlatformEventData platformEventData = null;
		ToolsActionEventDetail toolsActionEventDetail = null;
		PlatformResponseEventDetail platformResponseEventDetail = null;
		EventType eventType = EventType.TOOLS;
		ToolsEventType toolsEventType = ToolsEventType._NO_DETAIL;
		PlatformEventType platformEventType = PlatformEventType._NO_DETAIL;
		
		if (rawEvent.getEventType() == event_type.TOOLS) {
			tools_event_data_v_1_0 rawToolsEventData = (tools_event_data_v_1_0)rawEvent.getEventData();
			if (rawToolsEventData.getToolsEventType() == tools_event_type.ACTION) {
				if (rawToolsEventData.getEventDetail().isPresent()) {
					toolsEventType = ToolsEventType.ACTION;
					tools_action_event_detail_v_1_0 rawToolsActionEventDetail = (tools_action_event_detail_v_1_0)rawToolsEventData.getEventDetail().get();
			        
			        toolsActionEventDetail = ToolsActionEventDetail.newBuilder()
							.setActionCommand(rawToolsActionEventDetail.getActionCommand())
							.setActionCommandArgs(rawToolsActionEventDetail.getActionCommandArgs())
							.setActionMethod(rawToolsActionEventDetail.getActionMethod())
							.build();
				}
			}
			toolsEventData = ToolsEventData.newBuilder()
					.setToolsEventCounter(rawToolsEventData.getToolsEventCounter().orNull())
					.setToolsEventCounterResetMs(rawToolsEventData.getToolsEventCounterResetMs().orNull())
					.setToolsEventType(toolsEventType)
					.setToolVersion(rawToolsEventData.getToolVersion())
					.setspeednscaleUserId(rawToolsEventData.getspeednscaleUserId().orNull())
					.setspeednscaleSessionId(rawToolsEventData.getspeednscaleSessionId().orNull())
					.setContainer(rawToolsEventData.getContainer())
					.setModule(rawToolsEventData.getModule())
					.setFunction(rawToolsEventData.getFunction())
					.setFeatureId(rawToolsEventData.getFeatureId().orNull())
					.setEventDetail(toolsActionEventDetail)
					.build();
			eventData = (Object)toolsEventData;
		}
		else if (rawEvent.getEventType() == event_type.PLATFORM) {
			platform_event_data_v_1_0 rawPlatformEventData = (platform_event_data_v_1_0)rawEvent.getEventData();
			eventType = EventType.PLATFORM;
			if (rawPlatformEventData.getPlatformEventType() == platform_event_type.RESPONSE) {
					if (rawPlatformEventData.getEventDetail().isPresent()) {
					platformEventType = PlatformEventType.RESPONSE;
					platform_response_event_detail_v_1_0 rawPlatformResponseEventDetail = (platform_response_event_detail_v_1_0)rawPlatformEventData.getEventDetail().get();
			        
			        platformResponseEventDetail = PlatformResponseEventDetail.newBuilder()
							.setResponseSize(rawPlatformResponseEventDetail.getResponseSize())
							.setResponseStatus(rawPlatformResponseEventDetail.getResponseStatus())
							.setProcessingTime(rawPlatformResponseEventDetail.getProcessingTime())
							.build();
				}
			}
			platformEventData = PlatformEventData.newBuilder()
					.setPlatformEventCounter(rawPlatformEventData.getPlatformEventCounter().orNull())
					.setPlatformEventCounterResetMs(rawPlatformEventData.getPlatformEventCounterResetMs().orNull())
					.setPlatformEventType(platformEventType)
					.setCorrelationId(rawPlatformEventData.getCorrelationId().orNull())
					.setspeednscaleUserId(rawPlatformEventData.getspeednscaleUserId().orNull())
					.setspeednscaleSessionId(rawPlatformEventData.getspeednscaleSessionId().orNull())
					.setContainer(rawPlatformEventData.getContainer())
					.setModule(rawPlatformEventData.getModule())
					.setFunction(rawPlatformEventData.getFunction())
					.setFeatureId(rawPlatformEventData.getFeatureId())
					.setEventDetail(platformResponseEventDetail)
					.build();
			eventData = (Object)platformEventData;
		}
		
		TelemetryEvent.Builder telemetryEventBuilder = TelemetryEvent.newBuilder()
				.setEventName(rawEvent.getEventName())
				.setEventGuid(rawEvent.getEventGuid())
				.setEventType(eventType)
				.setClientEventTimestamp(rawEvent.getClientEventTimestamp())
				.setClientEventTimezone(rawEvent.getClientEventTimezone())
				.setClientOs(rawEvent.getClientOs())
				.setClientId(rawEvent.getClientId())			
				.setClientIp(rawEvent.getClientIp())				
				.setClientEventCounter(rawEvent.getClientEventCounter())				
				.setClientEventCounterResetMs(rawEvent.getClientEventCounterResetMs())
				.setClientSdkVersion(rawEvent.getClientSdkVersion().orNull())
				.setServerTimestamp(rawEvent.getServerTimestamp())
				.setServerObservedAddress(rawEvent.getServerObservedAddress())
				.setProjectId(rawEvent.getProjectId().orNull())
				.setEventData(eventData);
		
		if (rawEvent.getExtendedDataFields().isPresent()) {
			telemetryEventBuilder.putAllExtended_data_fields(rawEvent.getExtendedDataFields().get());
		}
		return telemetryEventBuilder.build();
	}
}