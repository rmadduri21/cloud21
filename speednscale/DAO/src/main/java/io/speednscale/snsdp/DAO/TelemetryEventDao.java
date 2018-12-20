package io.speednscale.snsdp.DAO;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

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
import io.speednscale.snsdp.snsdp_record_models.raw.event_v_1_0;
import io.speednscale.snsdp.snsdp_record_models.transformed.TelemetryEvent;
import io.speednscale.snsdp.snsdp_record_models.transformed.ToolsEventData;
import io.speednscale.snsdp.snsdp_record_models.transformed.ToolsActionEventDetail;
import io.speednscale.snsdp.snsdp_record_models.transformed.PlatformEventData;
import io.speednscale.snsdp.snsdp_record_models.transformed.PlatformResponseEventDetail;
import io.speednscale.snsdp.snsdp_record_models.transformed.EventType;
import io.speednscale.snsdp.snsdp_record_models.transformed.ToolsEventType;
import io.speednscale.snsdp.snsdp_record_models.transformed.PlatformEventType;
import io.speednscale.snsdp.transformations.TelemetryEventTransformer;

/**
 * This class is responsible for providing data access features for Customer table.
 */
public class TelemetryEventDao extends AbstractDao {
	private HConnection  connection;
	private String  eventTable;
	private String  nameSpace;
	private String	toolsEventDataTable;
	private String  toolsActionEventDetailTable;
	private String  platformEventDataTable;
	private String	platformResponseEventDetailTable;
	
	// Need to define these in TelemetryEventTransformer also
	private static int TELEMETRY_EVENT_FIELD_COUNT = 16;
	private static int PLATFORM_EVENT_DATA_FIELD_COUNT = 11;
	private static int TOOLS_EVENT_DATA_FIELD_COUNT = 11;
	private static int TOOLS_ACTION_EVENT_DETAIL_FIELD_COUNT = 3;
	private static int PLATFORM_RESPONSE_EVENT_DETAIL_FIELD_COUNT = 3;

	public static final byte[] TELEMETRY_EVENT_FAMILY = Bytes.toBytes("te");
	public static final byte[] PLATFORM_EVENT_DATA_FAMILY = Bytes.toBytes("ped");
	public static final byte[] TOOLS_EVENT_DATA_FAMILY = Bytes.toBytes("ted");
	public static final byte[] PLATFORM_RESPONSE_EVENT_DETAIL_FAMILY = Bytes.toBytes("pred");
	public static final byte[] TOOLS_ACTION_EVENT_DETAIL_FAMILY = Bytes.toBytes("taed");
	
	
	private static final String[] TELEMETRY_EVENT_COLUMNS = {
    	"event_name",
    	"event_guid",
    	"event_type",
    	"client_event_timestamp",
    	"client_event_timezone",
    	"client_os",
    	"client_id",
    	"client_ip",
    	"client_event_counter",
    	"client_event_counter_reset_ms",
    	"client_sdk_version",
    	"server_timestamp",
    	"server_observed_address",
    	"project_id", 
		"extended_data_field_keys"
	};
	
	private static final String[] TOOLS_EVENT_DATA_COLUMNS = {
	    "tools_event_counter",
	    "tools_event_counter_reset_ms",
	    "tools_event_type",
	    "tool_version",
	    "speednscale_user_id",
	    "speednscale_session_id",
	    "container",
	    "module",
	    "function",
	    "feature_id",
	    "event_detail"
	};
	
	private static final String[] TOOLS_ACTION_EVENT_DETAIL_COLUMNS = {
        "action_command",
        "action_command_args",
        "action_method"
	};
	
	private static final String[] PLATFORM_EVENT_DATA_COLUMNS = {
		"platform_event_counter",
		"platform_event_counter_reset_ms",
        "speednscale_user_id",
        "speednscale_session_id",
        "platform_event_type",
        "correlation_id",
        "container",
        "module",
        "function",
        "feature_id",
        "event_detail"		
	};
	
	private static final String[] PLATFORM_RESPONSE_EVENT_DETAIL_COLUMNS = {
        "response_size",
        "response_status",
        "processing_time"
	};
	
	/**
	 * Constructor - instantiates an object of the class.
	 * 
	 * @param connection
	 *            configures HBase data source, maintains configuration, provides access to HBase table through
	 *            connection to HBase.
	 */
	public TelemetryEventDao(final HConnection connection) {
		this.connection = connection;
		this.nameSpace = "telemetry";
		this.eventTable = this.nameSpace + ":" + "telemetry_event";
		this.toolsEventDataTable = this.nameSpace + ":" + "tools_event_data";
		this.toolsActionEventDetailTable = this.nameSpace + ":" + "tools_action_event_detail";
		this.platformEventDataTable = this.nameSpace + ":" + "platform_event_data";
		this.platformResponseEventDetailTable = this.nameSpace + ":" + "platform_response_event_detail";
	}
    
	/**
	 * Returns the name of the TelemetryEvent table
	 * 
	 * @return Name of the table
	 */
	public String getTableName() {
		return this.eventTable;
	}

	public void addTelemetryEvent(TelemetryEvent telemetryEvent)
			throws IOException {
		if (telemetryEvent == null) {
			return;
		}
		
		HTableInterface telemetryEventHTable = this.connection.getTable(this.eventTable);
		HTableInterface toolsEventDataHTable = null;
		HTableInterface toolsActionEventDetailHTable = null;
		HTableInterface platformEventDataHTable = null;
		HTableInterface platformResponseEventDetailHTable = null;
		
		Put p = mkPut(Bytes.toBytes(this.getKey(telemetryEvent)));
		
		Put putToolsEventData = null;
		Put putToolsActionEventDetail = null;
		Put putPlatformEventData = null;
		Put putPlatformResponseEventDetail = null;
		
		for (int i = 0; i < TELEMETRY_EVENT_FIELD_COUNT; i++) {
			switch (i) {
			case 2:
				if (telemetryEvent.getEventType() == EventType.TOOLS) {
					// TOOLS event
					p.add(TELEMETRY_EVENT_FAMILY, Bytes.toBytes(TELEMETRY_EVENT_COLUMNS[i]), Bytes.toBytes("TOOLS"));
					ToolsEventData toolsEventData = (ToolsEventData)telemetryEvent.getEventData();
					toolsEventDataHTable = this.connection.getTable(this.toolsEventDataTable);
					putToolsEventData = mkPut(Bytes.toBytes(this.getKey(telemetryEvent)));
					
					//Add Tools event data
					for (int j = 0; j < TOOLS_EVENT_DATA_FIELD_COUNT - 1; j++) {
						switch (j) {
							case 0:
							case 1:
							case 4:
							case 5:
								putToolsEventData.add(TOOLS_EVENT_DATA_FAMILY, Bytes.toBytes(TOOLS_EVENT_DATA_COLUMNS[j]), Bytes.toBytes(((Long)toolsEventData.get(j))));
								break;
							case 2:
								if (toolsEventData.getToolsEventType() == ToolsEventType.ACTION) {
									putToolsEventData.add(TOOLS_EVENT_DATA_FAMILY, Bytes.toBytes(TOOLS_EVENT_DATA_COLUMNS[j]), Bytes.toBytes("ACTION"));
								
									//Add Tools_action_event_detail
									ToolsActionEventDetail toolsActionEventDetail = (ToolsActionEventDetail)(toolsEventData.getEventDetail().get());
									putToolsActionEventDetail = mkPut(Bytes.toBytes(this.getKey(telemetryEvent)));
									toolsActionEventDetailHTable = this.connection.getTable(this.toolsActionEventDetailTable);
									
									for (int k = 0; k < TOOLS_ACTION_EVENT_DETAIL_FIELD_COUNT; k++) {
										putToolsActionEventDetail.add(TOOLS_ACTION_EVENT_DETAIL_FAMILY, Bytes.toBytes(TOOLS_ACTION_EVENT_DETAIL_COLUMNS[k]), 
												Bytes.toBytes((String)toolsActionEventDetail.get(k)));
									}
									
								}
								else {
									putToolsEventData.add(TOOLS_EVENT_DATA_FAMILY, Bytes.toBytes(TOOLS_EVENT_DATA_COLUMNS[j]), Bytes.toBytes("_NO_DETAIL"));
								}
								break;
							
						default:	// String column
							p.add(TOOLS_EVENT_DATA_FAMILY, Bytes.toBytes(TOOLS_EVENT_DATA_COLUMNS[j]), Bytes.toBytes((String)toolsEventData.get(j)));
						}
					}
				}
				else if (telemetryEvent.getEventType() == EventType.PLATFORM) {
					p.add(TELEMETRY_EVENT_FAMILY, Bytes.toBytes(TELEMETRY_EVENT_COLUMNS[i]), Bytes.toBytes("PLATFORM"));
					PlatformEventData platformEventData = (PlatformEventData)telemetryEvent.getEventData();
					platformEventDataHTable = this.connection.getTable(this.platformEventDataTable);
					putPlatformEventData = mkPut(Bytes.toBytes(this.getKey(telemetryEvent)));
					
					// Add Platform event data
					putPlatformEventData = mkPut(Bytes.toBytes(this.getKey(telemetryEvent)));
					for (int j = 0; j < PLATFORM_EVENT_DATA_FIELD_COUNT - 1; ++j) {
						switch (j) {
						case 0:
						case 1:
						case 2:
						case 3:
							putPlatformEventData.add(PLATFORM_EVENT_DATA_FAMILY, Bytes.toBytes(PLATFORM_EVENT_DATA_COLUMNS[j]), Bytes.toBytes(((Long)platformEventData.get(j))));
						case 4:
							if (platformEventData.getPlatformEventType() == PlatformEventType.RESPONSE) {
								putPlatformEventData.add(PLATFORM_EVENT_DATA_FAMILY, Bytes.toBytes(PLATFORM_EVENT_DATA_COLUMNS[j]), Bytes.toBytes("RESPONSE"));
								
								//Add Platform_response_event_detail
								PlatformResponseEventDetail platformResponseEventDetail = (PlatformResponseEventDetail)(platformEventData.getEventDetail().get());
								putPlatformResponseEventDetail = mkPut(Bytes.toBytes(this.getKey(telemetryEvent)));
								platformResponseEventDetailHTable = this.connection.getTable(this.platformResponseEventDetailTable);
								
								
								for (int k = 0; k < PLATFORM_RESPONSE_EVENT_DETAIL_FIELD_COUNT; k++) {
									putPlatformResponseEventDetail.add(PLATFORM_RESPONSE_EVENT_DETAIL_FAMILY, Bytes.toBytes(PLATFORM_RESPONSE_EVENT_DETAIL_COLUMNS[k]), 
											Bytes.toBytes((Long)platformResponseEventDetail.get(k)));
								}
								
							}
							else {
								putPlatformEventData.add(PLATFORM_EVENT_DATA_FAMILY, Bytes.toBytes(PLATFORM_EVENT_DATA_COLUMNS[j]), Bytes.toBytes("_NO_DETAIL"));
							}
							break;
							
						default:	// String column
							p.add(PLATFORM_EVENT_DATA_FAMILY, Bytes.toBytes(PLATFORM_EVENT_DATA_COLUMNS[j]), Bytes.toBytes((String)platformEventData.get(j)));
						}
					}
					
				}
				break;
			case 3:
			case 8:
			case 9:
			case 11:
			case 13:
				p.add(TELEMETRY_EVENT_FAMILY, Bytes.toBytes(TELEMETRY_EVENT_COLUMNS[i]), Bytes.toBytes(((Long)telemetryEvent.get(i))));
				break;
			case 14:
				break;
			case 15:
				if (telemetryEvent.getExtendedDataFields().isPresent()) {
					//Extended data fields are added as top level columns
					Map<String, String> extendedFields = telemetryEvent.getExtendedDataFields().get();
					Set<String> keySet = extendedFields.keySet();
					
					//Add extended data fields as top level columns and build '%' separated extended data field keys string
					StringBuilder extendedDataFieldKeys = new StringBuilder();
					for (String colName : keySet) {
						if (extendedDataFieldKeys.length() != 0) {
							//append %, the key value string separator
							extendedDataFieldKeys.append('%');
						}
						extendedDataFieldKeys.append(colName);
						p.add(TELEMETRY_EVENT_FAMILY, Bytes.toBytes(colName), Bytes.toBytes(extendedFields.get(colName)));
					}
					
					if (extendedDataFieldKeys.length() != 0) {
						p.add(TELEMETRY_EVENT_FAMILY, Bytes.toBytes("extended_data_field_keys"), Bytes.toBytes(extendedDataFieldKeys.toString()));
					}
				}
				break;
				
			default:	// String column
				p.add(TELEMETRY_EVENT_FAMILY, Bytes.toBytes(TELEMETRY_EVENT_COLUMNS[i]), Bytes.toBytes((String)telemetryEvent.get(i)));
			}
		}
		
		// Add to HBase tables
		telemetryEventHTable.put(p);		
		telemetryEventHTable.close();
		if (toolsEventDataHTable != null) {
			toolsEventDataHTable.put(putToolsEventData);
			toolsEventDataHTable.close();
		}		
		if (toolsActionEventDetailHTable != null) {
			toolsActionEventDetailHTable.put(putToolsActionEventDetail);
			toolsActionEventDetailHTable.close();
		}
		if (platformEventDataHTable != null) {
			platformEventDataHTable.put(putPlatformEventData);
			platformEventDataHTable.close();
		}		
		if (platformResponseEventDetailHTable != null) {
			platformResponseEventDetailHTable.put(putPlatformResponseEventDetail);
			platformResponseEventDetailHTable.close();
		}	
	}

	public TelemetryEvent getTelemetryEvent(String telemetryEventKey)
			throws IOException {
		HTableInterface telemetryEventTbl = this.connection.getTable(Bytes.toBytes(this.eventTable));
		Get g = mkGet(Bytes.toBytes(telemetryEventKey));
		TelemetryEvent telemetryEventRecord = null;

		g.addFamily(TELEMETRY_EVENT_FAMILY);

		Result result = telemetryEventTbl.get(g);

		if (result.isEmpty()) {
			return null;
		}
		try {
			telemetryEventRecord = buildTelemetryEvent(result);
		}
        catch (IOException ioException) {
        	System.err.println("IO exception while building game telemetry record, table: " + this.eventTable);
        	return null;
        }
        catch (ClassNotFoundException classNotFoundException) {
        	System.err.println("Unable to convert byte array to an object");
        	return null;
        }
		telemetryEventTbl.close();
		return telemetryEventRecord;
	}
	
	public boolean existsTelemetryEvent(String telemetryEventKey)
			throws IOException {
		HTableInterface telemetryEventTbl = this.connection.getTable(Bytes.toBytes(this.eventTable));
		Get g = mkGet(Bytes.toBytes(telemetryEventKey));

		Result result = telemetryEventTbl.get(g);
		telemetryEventTbl.close();

		if (result.isEmpty()) {
			return false;
		}
		return true;
	}
	
	/**
	 * Retrieves all objects in the specified range. If startRow and stopRow are null then all objects in the table 
	 * will be returned.
	 * @param startRowKey
	 * @param stopRowKey
	 * @return collection of rows, null if table empty
	 */
	public Collection<TelemetryEvent> getTelemetryEventRecords(byte[] startRowKey, byte[] stopRowKey, Filter filterList, int limit) {
		Result result = null;
        List<TelemetryEvent> telemetryEventList = new ArrayList<TelemetryEvent>();
        int recordCount = 0;
        
        Scan scan = mkScan(startRowKey, stopRowKey, filterList);
        try {
        	HTableInterface telemetryEventTbl = this.connection.getTable(Bytes.toBytes(this.eventTable));
        	ResultScanner rs = telemetryEventTbl.getScanner(scan);
    		while ((result = rs.next()) != null && recordCount < limit) {
/*    			if ((recordCount % 10) == 0) {
    	        	System.out.println("Scanned record count: " + recordCount);
    			}
*/
    			telemetryEventList.add(buildTelemetryEvent(result));
    			++recordCount;
    		}
    		telemetryEventTbl.close();
        }
        catch (IOException ioException) {
        	System.err.println("IO exception while opening/getting a scan, or scanning table: " + this.eventTable);
        	return null;
        }
        catch (ClassNotFoundException classNotFoundException) {
        	System.err.println("Unable to convert byte array to an object");
        	return null;
        }
		
		return telemetryEventList;
	}
	
	public void deleteTelemetryEvent(String telemetryEventGuid) throws IOException {
		HTableInterface telemetryEventTbl = this.connection.getTable(Bytes.toBytes(this.eventTable));
		Delete d = mkDel(Bytes.toBytes(telemetryEventGuid));
		telemetryEventTbl.delete(d);
		telemetryEventTbl.close();
		return;
	}
	
	public void deltaProcess(event_v_1_0 rawTelemetryEvent)  {
		TelemetryEvent telemetryEvent = TelemetryEventTransformer.doTransformation(rawTelemetryEvent);
		
		TelemetryEvent telemetryEventFromHBase = null;
		
		try {
			telemetryEventFromHBase = getTelemetryEvent(this.getKey(telemetryEvent));
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (telemetryEventFromHBase == null) {
			try {
				// Insert row
				addTelemetryEvent(telemetryEvent);
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		else {
			if (compareGameTels(telemetryEventFromHBase, telemetryEvent)) {
				// Record already exists 
				// Ignore
			}
			else {
				// Check each column, update only changed columns
				// We just don't update now - TBD
				incrementalUpdateTelemetryEvent(telemetryEventFromHBase, telemetryEvent);
			}
		}
	}
	
	private void incrementalUpdateTelemetryEvent(TelemetryEvent existingGameTel, TelemetryEvent newGameTel) {
		return;
	}
	
	public String getKey(TelemetryEvent telemetryEvent) {
		return (telemetryEvent.getEventGuid());
	}
	
	private TelemetryEvent buildTelemetryEvent(Result result) throws IOException, ClassNotFoundException  {

		//System.out.println("Building extended data fields map...");
		Map<String, String> extendedDataFields = new HashMap<String, String>();
		byte[] extendedDataFieldBytes = result.getValue(TELEMETRY_EVENT_FAMILY, Bytes.toBytes(TELEMETRY_EVENT_COLUMNS[TELEMETRY_EVENT_COLUMNS.length - 1]));
		if (extendedDataFieldBytes != null) {
			String extendedDataFieldKeys = new String(extendedDataFieldBytes);
			String[] extendedDataFieldKeysArray = extendedDataFieldKeys.split("%");
			for (String extendedDataFieldKey : extendedDataFieldKeysArray) {
				extendedDataFields.put(extendedDataFieldKey, new String(result.getValue(TELEMETRY_EVENT_FAMILY, 
						Bytes.toBytes(extendedDataFieldKey))));
			}
		}
		
		//System.out.println("Filling in data fields...");
		TelemetryEvent.Builder telemetryEventBuilder = TelemetryEvent.newBuilder()
				.putAllExtended_data_fields(extendedDataFields)
		;
		TelemetryEvent telemetryEvent = telemetryEventBuilder.build();
		
		for (int i = 0; i < TELEMETRY_EVENT_FIELD_COUNT - 1; ++i) {
			byte[] columnVal = result.getValue(TELEMETRY_EVENT_FAMILY, Bytes.toBytes(TELEMETRY_EVENT_COLUMNS[i]));
			if (columnVal == null)
				continue;
			switch (i) {
			case 2:
				// Handle nested object types
				break;
			case 3:
			case 8:
			case 9:
			case 11:
			case 13:

				telemetryEvent.put(i, Bytes.toLong(columnVal));
				//System.out.println("TelemetryEvent column: " + GT_COLUMNS[i] + " , Value: " + telemetryEvent.get(i));
				break;
			case 14:
				break;
			default:
				telemetryEvent.put(i, new String(columnVal));
				//System.out.println("TelemetryEvent column: " + GT_COLUMNS[i] + " , Value: " + telemetryEvent.get(i));
				break;
			}
		}
		
		return telemetryEvent;
	}

	public boolean compareGameTels(TelemetryEvent te1, TelemetryEvent te2) {
		
		return true;
	}
}