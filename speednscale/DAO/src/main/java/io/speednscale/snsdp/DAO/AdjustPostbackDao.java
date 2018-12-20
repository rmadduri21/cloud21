package io.speednscale.snsdp.DAO;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
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
import io.speednscale.snsdp.snsdp_record_models.raw.adjust_postback;
import io.speednscale.snsdp.snsdp_record_models.raw._mp_server_metadata;
import io.speednscale.snsdp.snsdp_record_models.transformed.AdjustPostback;
import io.speednscale.snsdp.snsdp_record_models.transformed.ap__mp_server_metadata;
import io.speednscale.snsdp.transformations.AdjustPostbackTransformer;

/**
 * This class is responsible for providing data access features for adjust_postback table.
 */
public class AdjustPostbackDao extends AbstractDao {
	private HConnection  connection;
	private String  tableName;
	private String  nameSpace;

	public static final byte[] ADJUST_POSTBACK_FAMILY = Bytes.toBytes("ap");
	
	
	//Define columns
	private static final byte[] AP_ACTION_COL =	Bytes.toBytes("action");
	private static final byte[] AP_APP_ID_COL =	Bytes.toBytes("app_id");
	private static final byte[] AP_APP_NAME_COL =	Bytes.toBytes("app_name");
	private static final byte[] AP_STORE_COL =	Bytes.toBytes("store");
	private static final byte[] AP_TRACKER_COL = Bytes.toBytes("tracker");
	private static final byte[] AP_TRACKER_NAME_COL = Bytes.toBytes("tracker_name");
	private static final byte[] AP_NETWORK_NAME_COL = Bytes.toBytes("network_name");
	private static final byte[] AP_CAMPAIGN_NAME_COL =	Bytes.toBytes("campaign_name");
	private static final byte[] AP_ADGROUP_NAME_COL =	Bytes.toBytes("adgroup_name");
	private static final byte[] AP_CREATIVE_NAME_COL =	Bytes.toBytes("creative_name");
	private static final byte[] AP_IS_IAD_COL =	Bytes.toBytes("is_iad");
	private static final byte[] AP_GCLID_COL =	Bytes.toBytes("gclid");
	private static final byte[] AP_APP_VERSION_COL =	Bytes.toBytes("app_version");
	private static final byte[] AP_ADID_COL =	Bytes.toBytes("adid");
	private static final byte[] AP_IDFA_COL =	Bytes.toBytes("idfa");
	private static final byte[] AP_ANDROID_ID_COL =	Bytes.toBytes("android_id");
	private static final byte[] AP_MAC_SHA1_COL =	Bytes.toBytes("mac_sha1");
	private static final byte[] AP_MAC_MD5_COL =	Bytes.toBytes("mac_md5");
	private static final byte[] AP_IDFA_OR_ANDROID_ID_COL =	Bytes.toBytes("idfa_or_android_id");
	private static final byte[] AP_IDFA_OR_GPS_ADID_COL =	Bytes.toBytes("idfa_or_gps_adid");
	private static final byte[] AP_IDFA_MD5_COL =	Bytes.toBytes("idfa_md5");
	private static final byte[] AP_IDFA_MD5_HEX_COL =	Bytes.toBytes("idfa_md5_hex");
	private static final byte[] AP_IDFA_UPPER_COL =	Bytes.toBytes("idfa_upper");
	private static final byte[] AP_IDFV_COL =	Bytes.toBytes("IDFV");
	private static final byte[] AP_GPS_ADID_COL =	Bytes.toBytes("gps_adid");
	private static final byte[] AP_REFTAG_COL =	Bytes.toBytes("reftag");
	private static final byte[] AP_REFERRER_COL =	Bytes.toBytes("referrer");
	private static final byte[] AP_USER_AGENT_COL =	Bytes.toBytes("user_agent");
	private static final byte[] AP_IP_ADDRESS_COL =	Bytes.toBytes("ip_address");
	private static final byte[] AP_CREATED_AT_COL =	Bytes.toBytes("created_at");
	private static final byte[] AP_CLICK_TIME_COL =	Bytes.toBytes("click_time");
	private static final byte[] AP_INSTALLED_AT_COL =	Bytes.toBytes("installed_at");
	private static final byte[] AP_CREATED_AT_HOUR_COL =	Bytes.toBytes("created_at_hour");
	private static final byte[] AP_CLICK_TIME_HOUR_COL =	Bytes.toBytes("click_time_hour");
	private static final byte[] AP_INSTALLED_AT_HOUR_COL =	Bytes.toBytes("installed_at_hour");
	private static final byte[] AP_COUNTRY_COL =	Bytes.toBytes("country");
	private static final byte[] AP_LANGUAGE_COL =	Bytes.toBytes("language");
	private static final byte[] AP_DEVICE_NAME_COL =	Bytes.toBytes("device_name");
	private static final byte[] AP_OS_NAME_COL = Bytes.toBytes("os_name");
	private static final byte[] AP_SDK_VERSION_COL = Bytes.toBytes("sdk_version");
	private static final byte[] AP_OS_VERSION_COL =	Bytes.toBytes("os_version");
	private static final byte[] AP_SESSION_COUNT_COL =	Bytes.toBytes("session_count");
	private static final byte[] AP_CURRENCY_COL =	Bytes.toBytes("currency");
	private static final byte[] AP_RANDOM_COL =	Bytes.toBytes("random");
	private static final byte[] AP_NONCE_COL =	Bytes.toBytes("nonce");	
	private static final byte[] AP_RANDOM_USER_ID_COL =	Bytes.toBytes("random_user_id");
	private static final byte[] AP_ENVIRONMENT_COL =	Bytes.toBytes("environment");
	private static final byte[] AP_TRACKING_ENABLED_COL =	Bytes.toBytes("tracking_enabled");
	private static final byte[] AP_TIMEZONE_COL =	Bytes.toBytes("timezone");
	private static final byte[] AP_LAST_TIME_SPENT_COL =	Bytes.toBytes("last_time_spent");
	private static final byte[] AP_LABEL_COL =	Bytes.toBytes("label");
	private static final byte[] AP_TIME_SPENT_COL =	Bytes.toBytes("time_spent");
	private static final byte[] AP_FB_CAMPAIGN_GROUP_NAME_COL =	Bytes.toBytes("fb_campaign_group_name");
	private static final byte[] AP_FB_CAMPAIGN_GROUP_ID_COL =	Bytes.toBytes("fb_campaign_group_id");
	private static final byte[] AP_FB_CAMPAIGN_NAME_COL =	Bytes.toBytes("fb_campaign_name");
	private static final byte[] AP_FB_CAMPAIGN_ID_COL =	Bytes.toBytes("fb_campaign_id");
	private static final byte[] AP_FB_ADGROUP_NAME_COL =	Bytes.toBytes("fb_adgroup_name");
	private static final byte[] AP_FB_ADGROUP_ID_COL =	Bytes.toBytes("fb_adgroup_id");
	private static final byte[] AP_REVENUE_COL =	Bytes.toBytes("revenue");
	private static final byte[] AP_REVENUE_FLOAT_COL =	Bytes.toBytes("revenue_float");
		
	private static final byte[] AP__MP_SERVER_METADATA_COL = Bytes.toBytes("ap__mp_server_metadata");
	private static final byte[] AP_RECORDNAME_COL =	Bytes.toBytes("recordname");
	private static final byte[] AP_GAME_COL = Bytes.toBytes("game");
	private static final byte[] AP_EXTENDED_DATA_FIELDS_COL = Bytes.toBytes("extended_data_fields");
	
	public static final int AP_DATA_FIELD_COUNT = 60;
	public static final int AP_MP_SERVER_METADATA_FIELD_COUNT = 12;
	
	/**
	 * Constructor - instantiates an object of the class.
	 * 
	 * @param connection
	 *            configures HBase data source, maintains configuration, provides access to HBase table through
	 *            connection to HBase.
	 */
	public AdjustPostbackDao(final HConnection connection) {
		this.connection = connection;
		this.nameSpace = "telemetryV1";
		this.tableName = this.nameSpace + ":" + "adjust_postbacks";
	}
    
	/**
	 * Returns the name of the AdjustPostback table
	 * 
	 * @return Name of the table
	 */
	public String getTableName() {
		return this.tableName;
	}

	public void addAdjustPostback(AdjustPostback adjustPostback)
			throws IOException {
		if (adjustPostback == null) {
			return;
		}
		
		HTableInterface gameTelTbl = this.connection.getTable(this.tableName);
		
		Put p = mkPut(Bytes.toBytes(this.getKey(adjustPostback)));
		
		p.add(ADJUST_POSTBACK_FAMILY, AP_ACTION_COL, Bytes.toBytes(adjustPostback.getAction()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_APP_ID_COL, Bytes.toBytes(adjustPostback.getAppId()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_APP_NAME_COL, Bytes.toBytes(adjustPostback.getAppName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_STORE_COL, Bytes.toBytes(adjustPostback.getStore()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_TRACKER_COL, Bytes.toBytes(adjustPostback.getTracker()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_TRACKER_NAME_COL, Bytes.toBytes(adjustPostback.getTrackerName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_NETWORK_NAME_COL, Bytes.toBytes(adjustPostback.getNetworkName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_CAMPAIGN_NAME_COL, Bytes.toBytes(adjustPostback.getCampaignName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_ADGROUP_NAME_COL, Bytes.toBytes(adjustPostback.getAdgroupName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_CREATIVE_NAME_COL, Bytes.toBytes(adjustPostback.getCreativeName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_IS_IAD_COL, Bytes.toBytes(adjustPostback.getIsIad()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_GCLID_COL, Bytes.toBytes(adjustPostback.getGclid()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_APP_VERSION_COL, Bytes.toBytes(adjustPostback.getAppVersion()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_ADID_COL, Bytes.toBytes(adjustPostback.getAdid()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_IDFA_COL, Bytes.toBytes(adjustPostback.getIdfa()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_ANDROID_ID_COL, Bytes.toBytes(adjustPostback.getAndroidId()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_MAC_SHA1_COL, Bytes.toBytes(adjustPostback.getMacSha1()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_MAC_MD5_COL, Bytes.toBytes(adjustPostback.getMacMd5()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_IDFA_OR_ANDROID_ID_COL, Bytes.toBytes(adjustPostback.getIdfaOrAndroidId()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_IDFA_OR_GPS_ADID_COL, Bytes.toBytes(adjustPostback.getIdfaOrGpsAdid()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_IDFA_MD5_COL, Bytes.toBytes(adjustPostback.getIdfaMd5()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_IDFA_MD5_HEX_COL, Bytes.toBytes(adjustPostback.getIdfaMd5Hex()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_IDFA_UPPER_COL, Bytes.toBytes(adjustPostback.getIdfaUpper()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_IDFV_COL, Bytes.toBytes(adjustPostback.getIdfv()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_GPS_ADID_COL, Bytes.toBytes(adjustPostback.getGpsAdid()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_REFTAG_COL, Bytes.toBytes(adjustPostback.getReftag()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_REFERRER_COL, Bytes.toBytes(adjustPostback.getReferrer()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_USER_AGENT_COL, Bytes.toBytes(adjustPostback.getUserAgent()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_IP_ADDRESS_COL, Bytes.toBytes(adjustPostback.getIpAddress()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_CREATED_AT_COL, Bytes.toBytes(adjustPostback.getCreatedAt()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_CLICK_TIME_COL, Bytes.toBytes(adjustPostback.getClickTime()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_INSTALLED_AT_COL, Bytes.toBytes(adjustPostback.getInstalledAt()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_CREATED_AT_HOUR_COL, Bytes.toBytes(adjustPostback.getCreatedAtHour()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_CLICK_TIME_HOUR_COL, Bytes.toBytes(adjustPostback.getClickTimeHour()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_INSTALLED_AT_HOUR_COL, Bytes.toBytes(adjustPostback.getInstalledAtHour()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_COUNTRY_COL, Bytes.toBytes(adjustPostback.getCountry()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_LANGUAGE_COL, Bytes.toBytes(adjustPostback.getLanguage()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_DEVICE_NAME_COL, Bytes.toBytes(adjustPostback.getDeviceName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_OS_NAME_COL, Bytes.toBytes(adjustPostback.getOsName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_SDK_VERSION_COL, Bytes.toBytes(adjustPostback.getSdkVersion()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_OS_VERSION_COL, Bytes.toBytes(adjustPostback.getOsVersion()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_SESSION_COUNT_COL, Bytes.toBytes(adjustPostback.getSessionCount()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_CURRENCY_COL, Bytes.toBytes(adjustPostback.getCurrency()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_RANDOM_COL, Bytes.toBytes(adjustPostback.getRandom()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_NONCE_COL, Bytes.toBytes(adjustPostback.getNonce()));	
		p.add(ADJUST_POSTBACK_FAMILY, AP_RANDOM_USER_ID_COL, Bytes.toBytes(adjustPostback.getRandomUserId()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_ENVIRONMENT_COL, Bytes.toBytes(adjustPostback.getEnvironment()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_TRACKING_ENABLED_COL, Bytes.toBytes(adjustPostback.getTrackingEnabled()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_TIMEZONE_COL, Bytes.toBytes(adjustPostback.getTimezone()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_LAST_TIME_SPENT_COL, Bytes.toBytes(adjustPostback.getLastTimeSpent()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_LABEL_COL, Bytes.toBytes(adjustPostback.getLabel()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_TIME_SPENT_COL, Bytes.toBytes(adjustPostback.getTimeSpent()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_FB_CAMPAIGN_GROUP_NAME_COL, Bytes.toBytes(adjustPostback.getFbCampaignGroupName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_FB_CAMPAIGN_GROUP_ID_COL, Bytes.toBytes(adjustPostback.getFbCampaignGroupId()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_FB_CAMPAIGN_NAME_COL, Bytes.toBytes(adjustPostback.getFbCampaignName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_FB_CAMPAIGN_ID_COL, Bytes.toBytes(adjustPostback.getFbCampaignId()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_FB_ADGROUP_NAME_COL, Bytes.toBytes(adjustPostback.getFbAdgroupName()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_FB_ADGROUP_ID_COL, Bytes.toBytes(adjustPostback.getFbAdgroupId()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_REVENUE_COL, Bytes.toBytes(adjustPostback.getRevenue()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_REVENUE_FLOAT_COL, Bytes.toBytes(adjustPostback.getRevenueFloat()));
		
		p.add(ADJUST_POSTBACK_FAMILY, AP__MP_SERVER_METADATA_COL, toByteArray(adjustPostback.getApMpServerMetadata()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_RECORDNAME_COL, Bytes.toBytes(adjustPostback.getRecordname()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_GAME_COL, Bytes.toBytes(adjustPostback.getGame()));
		p.add(ADJUST_POSTBACK_FAMILY, AP_EXTENDED_DATA_FIELDS_COL, toByteArray(adjustPostback.getExtendedDataFields()));
		
		Map<String, String> extendedFields = adjustPostback.getExtendedDataFields();
		Set<String> keySet = extendedFields.keySet();
		
		//Add extended data fields as top level columns
		for (String colName : keySet) {
			p.add(ADJUST_POSTBACK_FAMILY, Bytes.toBytes(colName), Bytes.toBytes(extendedFields.get(colName)));
		}
		
		gameTelTbl.put(p);
		gameTelTbl.close();
	}

	public AdjustPostback getAdjustPostback(String apKey)
			throws IOException {
		HTableInterface apTbl = this.connection.getTable(Bytes.toBytes(this.tableName));
		Get g = mkGet(Bytes.toBytes(apKey));
		AdjustPostback apRecord = null;

		g.addFamily(ADJUST_POSTBACK_FAMILY);

		Result result = apTbl.get(g);

		if (result.isEmpty()) {
			return null;
		}
		try {
			apRecord = buildAdjustPostback(result);
		}
        catch (IOException ioException) {
        	System.err.println("IO exception while building game telemetry record, table: " + this.tableName);
        	return null;
        }
        catch (ClassNotFoundException classNotFoundException) {
        	System.err.println("Unable to convert byte array to an object");
        	return null;
        }
		apTbl.close();
		return apRecord;
	}
	
	/**
	 * Retrieves all objects in the specified range. If startRow and stopRow are null then all objects in the table will be returned.
	 * @param startRowKey
	 * @param stopRowKey
	 * @return collection of rows, null if table empty
	 */
	public Collection<AdjustPostback> getAdjustPostbackRecords(byte[] startRowKey, byte[] stopRowKey, Filter filterList) {
		Result result = null;
        List<AdjustPostback> apList = new ArrayList<AdjustPostback>();
        
        Scan scan = mkScan(startRowKey, stopRowKey, filterList);
        try {
        	HTableInterface apTbl = this.connection.getTable(Bytes.toBytes(this.tableName));
        	ResultScanner rs = apTbl.getScanner(scan);
    		while ((result = rs.next()) != null) {
    			apList.add(buildAdjustPostback(result));
    		}
    		apTbl.close();
        }
        catch (IOException ioException) {
        	System.err.println("IO exception while opening and getting a scan, and scanning table: " + this.tableName);
        	return null;
        }
        catch (ClassNotFoundException classNotFoundException) {
        	System.err.println("Unable to convert byte array to an object");
        	return null;
        }
		
		return apList;
	}
	
	public void deleteAdjustPostback(String adjustPostbackKey) throws IOException {
		HTableInterface apTbl = this.connection.getTable(Bytes.toBytes(this.tableName));
		Delete d = mkDel(Bytes.toBytes(adjustPostbackKey));
		apTbl.delete(d);
		apTbl.close();
		return;
	}
	
	public void deltaProcess(adjust_postback rawAdjustPostback)  {
		AdjustPostback adjustPostback = AdjustPostbackTransformer.doTransformation(AP_DATA_FIELD_COUNT, 
				AP_MP_SERVER_METADATA_FIELD_COUNT, rawAdjustPostback);
		
		AdjustPostback apFromHBase = null;
		
		try {
			apFromHBase = getAdjustPostback(this.getKey(adjustPostback));
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (apFromHBase == null) {
			try {
				// Insert row
				addAdjustPostback(adjustPostback);
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		else {
			if (compareAdjustPostbacks(apFromHBase, adjustPostback)) {
				// Record already exists 
				// Ignore
			}
			else {
				// Check each column, update only changed columns
				incrementalUpdateAdjustPostback(apFromHBase, adjustPostback);
			}
		}
	}
	
	public static int getDataFieldCount() {
		return AP_DATA_FIELD_COUNT;
	}
	
	public static int getMpServerMetadataFieldCount() {
		return AP_MP_SERVER_METADATA_FIELD_COUNT;
	}
	
	public String getKey(AdjustPostback adjustPostback) {
		return adjustPostback.getIdfa() + KEY_COLUMN_SEPARATOR + adjustPostback.getDeviceName() + KEY_COLUMN_SEPARATOR 
		+ adjustPostback.getRandomUserId() + KEY_COLUMN_SEPARATOR + adjustPostback.getRandom();
	}
	
	private AdjustPostback buildAdjustPostback(Result result) throws IOException, ClassNotFoundException  {
		byte[] rowKey = result.getRow();
		String keyString = new String(rowKey);
		AdjustPostback.Builder apBuilder = AdjustPostback.newBuilder()
								
				.setAction(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_ACTION_COL)))
		        .setAppId(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_APP_ID_COL)))
		        .setAppName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_APP_NAME_COL)))
		        .setStore(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_STORE_COL)))
		        .setTracker(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_TRACKER_COL)))
		        .setTrackerName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_TRACKER_NAME_COL)))
		        .setNetworkName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_NETWORK_NAME_COL)))
		        .setCampaignName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_CAMPAIGN_NAME_COL)))
		        .setAdgroupName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_ADGROUP_NAME_COL)))
		        .setCreativeName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_CREATIVE_NAME_COL)))
		
		        .setIsIad(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_IS_IAD_COL)))
		        .setGclid(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_GCLID_COL)))
		        .setAppVersion(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_APP_VERSION_COL)))
		        .setAdid(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_ADID_COL)))
		        .setIdfa(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_IDFA_COL)))
		        .setAndroidId(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_ANDROID_ID_COL)))
		        .setMacSha1(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_MAC_SHA1_COL)))
		        .setMacMd5(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_MAC_MD5_COL)))
		        .setIdfaOrAndroidId(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_IDFA_OR_ANDROID_ID_COL)))
		        .setIdfaOrGpsAdid(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_IDFA_OR_GPS_ADID_COL)))
		
		        .setIdfaMd5(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_IDFA_MD5_COL)))
		        .setIdfaMd5Hex(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_IDFA_MD5_HEX_COL)))
		        .setIdfaUpper(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_IDFA_UPPER_COL)))
		        .setIdfv(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_IDFV_COL)))
		        .setGpsAdid(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_GPS_ADID_COL)))
		        .setReftag(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_REFTAG_COL)))
		        .setReferrer(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_REFERRER_COL)))
		        .setUserAgent(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_USER_AGENT_COL)))
		        .setIpAddress(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_IP_ADDRESS_COL)))
		        .setCreatedAt(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_CREATED_AT_COL)))
		
		        .setClickTime(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_CLICK_TIME_COL)))
		        .setInstalledAt(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_INSTALLED_AT_COL)))
		        .setCreatedAtHour(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_CREATED_AT_HOUR_COL)))
		        .setClickTimeHour(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_CLICK_TIME_HOUR_COL)))
		        .setInstalledAtHour(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_INSTALLED_AT_HOUR_COL)))
		        .setCountry(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_COUNTRY_COL)))
		        .setLanguage(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_LANGUAGE_COL)))
		        .setDeviceName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_DEVICE_NAME_COL)))
		        .setOsName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_OS_NAME_COL)))
		        .setSdkVersion(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_SDK_VERSION_COL)))
		
		        .setOsVersion(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_OS_VERSION_COL)))
		        .setSessionCount(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_SESSION_COUNT_COL)))
		        .setCurrency(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_CURRENCY_COL)))
		        .setRandom(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_RANDOM_COL)))
		        .setNonce(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_NONCE_COL)))
		        .setRandomUserId(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_RANDOM_USER_ID_COL)))	
		        .setEnvironment(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_ENVIRONMENT_COL)))
		        .setTrackingEnabled(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_TRACKING_ENABLED_COL)))
		        .setTimezone(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_TIMEZONE_COL)))
		        .setLastTimeSpent(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_LAST_TIME_SPENT_COL)))
		
		        .setLabel(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_LABEL_COL)))
		        .setTimeSpent(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_TIME_SPENT_COL)))
		        .setFbCampaignGroupName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_FB_CAMPAIGN_GROUP_NAME_COL)))
		        .setFbCampaignGroupId(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_FB_CAMPAIGN_GROUP_ID_COL)))
		        .setFbCampaignName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_FB_CAMPAIGN_NAME_COL)))
		        .setFbCampaignId(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_FB_CAMPAIGN_ID_COL)))
		        .setFbAdgroupName(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_FB_ADGROUP_NAME_COL)))
		        .setFbAdgroupId(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_FB_ADGROUP_ID_COL)))
		        .setRevenue(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_REVENUE_COL)))
		        .setRevenueFloat(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_REVENUE_FLOAT_COL)))
		
				.setApMpServerMetadata((ap__mp_server_metadata)toObject(result.getValue(ADJUST_POSTBACK_FAMILY, AP__MP_SERVER_METADATA_COL)))
				.setRecordname(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_RECORDNAME_COL)))
				.setGame(new String(result.getValue(ADJUST_POSTBACK_FAMILY, AP_GAME_COL)))
				.putAllExtended_data_fields((Map<String, String>)toObject(result.getValue(ADJUST_POSTBACK_FAMILY, AP_EXTENDED_DATA_FIELDS_COL)))
				;
				
				AdjustPostback apObject = apBuilder.build();
				
        //Print the retrieved object for debugging
		//for (int i = 0; i < 63; ++i) {
            //if (i != 60)
            	//System.out.println("AdjustPostback attribute: " + i + " is " + apObject.get(i));
        //}
        						
		return apObject;
	}
	
		
	private void incrementalUpdateAdjustPostback(AdjustPostback oldAdjustPostback, AdjustPostback newAdjustPostback) {
		return;
	}
	
	public boolean compareAdjustPostbacks(AdjustPostback ap1, AdjustPostback ap2) {
	
		if (!(ap1.getGame().equals(ap2.getGame()))) {
			return false;
		}
		
		//System.out.println("AdjustPostback Games compared, game1: "+ap1.getGame()+" game2: " + ap2.getGame());
		
		if (!(ap1.getRecordname().equals(ap2.getRecordname()))) {
			return false;
		}
		
		//System.out.println("AdjustPostback Recordnames compared, recorname1: "+ap1.getRecordname()+" recordname2: " + ap2.getRecordname());

/*		
		for (int i = 0; i < AP_DATA_FIELD_COUNT; ++i) {
			System.out.println("AdjustPostback comparing data fields at: " + i + " data1: " + ap1.get(i) + " data2: " + ap2.get(i));
			if (!(ap1.get(i).equals(ap2.get(i))))			
				return false;
			System.out.println("AdjustPostback compared data fields at: " + i + " data1: " + ap1.get(i) + " data2: " + ap2.get(i));
		}
*/
		//System.out.println("AdjustPostback data fields successfully compared.");	
		
		//Compare mp_server_metadata
		ap__mp_server_metadata metadata1 = ap1.getApMpServerMetadata();
		ap__mp_server_metadata metadata2 = ap2.getApMpServerMetadata();				
		for (int i = 0; i < AP_MP_SERVER_METADATA_FIELD_COUNT; ++i) {
			if (!(metadata1.get(i).equals(metadata2.get(i))))
				return false;
		}

		//System.out.println("AdjustPostback mp server metadata fields successfully compared.");
				
		// Add comparision for Map
		if (ap1.getExtendedDataFields().size() != ap2.getExtendedDataFields().size()) {
			return false;
		}		
		Map<String, String> extendedFields1 = ap1.getExtendedDataFields();
		Map<String, String> extendedFields2 = ap2.getExtendedDataFields();
		Set<String> keySet2 = extendedFields2.keySet();		
		for (String key1 : extendedFields1.keySet()) {
			if ((keySet2.contains(key1)) && extendedFields1.get(key1).equals(extendedFields2.get(key1))) {
				continue;
			}
			else {
				return false;
			}
		}
		
		//System.out.println("AdjustPostback extended data fields successfully compared.");
		
		return true;
	}
	
}