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
import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry;
import io.speednscale.snsdp.snsdp_record_models.raw.data;
import io.speednscale.snsdp.snsdp_record_models.raw._mp_server_metadata;
import io.speednscale.snsdp.snsdp_record_models.transformed.GameTelemetry;
import io.speednscale.snsdp.snsdp_record_models.transformed.gt__mp_server_metadata;
import io.speednscale.snsdp.transformations.GameTelemetryTransformer;

/**
 * This class is responsible for providing data access features for Customer table.
 */
public class GameTelemetryDao extends AbstractDao {
	private HConnection  connection;
	private String  tableName;
	private String  nameSpace;
	
	// Need to define these in GameTelemetryTransformer also
	public static final int DATA_FIELD_COUNT = 54;
	private static final int MP_SERVER_METADATA_BEGIN = DATA_FIELD_COUNT;
	public static final int MP_SERVER_METADATA_FIELD_COUNT = 12;
	private static final int MP_SERVER_METADATA_END = DATA_FIELD_COUNT + MP_SERVER_METADATA_FIELD_COUNT;
	private static final int MP_SERVER_METADATA_HDR_LENGTH_COL = MP_SERVER_METADATA_BEGIN + 8;

	public static final byte[] GAME_TELEMETRY_FAMILY = Bytes.toBytes("gt");
	
	
	private static final String[] GT_COLUMNS = {
        "ab_groups",
        "abtestgroupids",
        "abtestgroups",
        "abtestids",
        "abtestvalues",
        "adjust_first_attributed_tracker_activity_kind",
        "adjust_latest_attributed_tracker_activity_kind",
        "advertiser_id",
        "app_bundle_id",
        "are_coins_doubled",
        "binary_version",
        "client_event_time",
        "client_event_time_zone",
        "client_timezone_offset",
        "consecutive_days_played",
        "count_of_runs",
        "country_code",
		"cumulative_score",		
		"current_costume",		
		"current_energy_level",
		"current_lindsey_stirling_tokens",		
		"device_id",		
		"device_model",		
		"device_name",		
		"enabled_remote_notification_types",		
		"evid",		
		"first_play_time",		
		"first_play_time_zone",		
		"guid",		
		"hard_currency_balance",
		"is_install",		
		"is_production_build",		
		"is_release_build",		
		"last_iap_time",		
		"last_login_time",		
		"last_login_time_zone",		
		"level",		
		"lifetime_iap_spend_in_usd_cents",		
		"sdk_version",		
		"speednscale_telemetry_version",
		"os_version",		
		"os_version_full",		
		"platform",		
		"player_is_logged_in_to_facebook",		
		"playfab_session_ticket",		
		"playfab_title_id",		
		"playfab_user_id",		
		"popdash_release_version",		
		"popdash_telemetry_version",		
		"soft_currency_balance",		
		"time_to_next_energy_refill",		
		"title_id",
		"ts",
		"user_id",
		
		//"gt__mp_server_metadata",
        "_mp_header_content_type",
        "_mp_server_timestamp",
        "_mp_header_accept_encoding",
        "_mp_header_connection",
        "_mp_remote_address",
        "_mp_query_string",
        "_mp_header_host",
        "_mp_request_uri",
        "_mp_header_content_length",
        "_mp_header_user_agent",
        "_mp_header_x_unity_version",
        "server_timestamp",
        
		
		"recordname",
		"game",
		"extended_data_field_keys"
	};
	
	private static final byte[] GT_AB_GROUPS_COL =	Bytes.toBytes("ab_groups");
	private static final byte[] GT_ABTESTGROUPIDS_COL =	Bytes.toBytes("abtestgroupids");
	private static final byte[] GT_ABTESTGROUPS_COL =	Bytes.toBytes("abtestgroups");
	private static final byte[] GT_ABTESTIDS_COL =	Bytes.toBytes("abtestids");
	private static final byte[] GT_ABTESTVALUES_COL =	Bytes.toBytes("abtestvalues");
	private static final byte[] GT_ADJUST_FIRST_ATTRIBUTED_TRACKER_ACTIVITY_KIND_COL =	
			Bytes.toBytes("adjust_first_attributed_tracker_activity_kind");
	private static final byte[] GT_ADJUST_LATEST_ATTRIBUTED_TRACKER_ACTIVITY_KIND_COL =	
			Bytes.toBytes("adjust_latest_attributed_tracker_activity_kind");
	private static final byte[] GT_APP_BUNDLE_ID_COL =	Bytes.toBytes("app_bundle_id");
	private static final byte[] GT_ADVERTISER_ID_COL =	Bytes.toBytes("advertiser_id");
	private static final byte[] GT_ARE_COINS_DOUBLED_COL =	Bytes.toBytes("are_coins_doubled");	
	private static final byte[] GT_BINARY_VERSION_COL =	Bytes.toBytes("binary_version");		
	private static final byte[] GT_CLIENT_EVENT_TIME_COL =	Bytes.toBytes("client_event_time");		
	private static final byte[] GT_CLIENT_EVENT_TIME_ZONE_COL =	Bytes.toBytes("client_event_time_zone");		
	private static final byte[] GT_CLIENT_TIMEZONE_OFFSET_COL =	Bytes.toBytes("client_timezone_offset");		
	private static final byte[] GT_CONSECUTIVE_DAYS_PLAYED_COL =	Bytes.toBytes("consecutive_days_played");		
	private static final byte[] GT_COUNT_OF_RUNS_COL =	Bytes.toBytes("count_of_runs");		
	private static final byte[] GT_COUNTRY_CODE_COL =	Bytes.toBytes("country_code");		
	private static final byte[] GT_CUMULATIVE_SCORE_COL =	Bytes.toBytes("cumulative_score");		
	private static final byte[] GT_CURRENT_COSTUME_COL =	Bytes.toBytes("current_costume");		
	private static final byte[] GT_CURRENT_ENERGY_LEVEL_COL =	Bytes.toBytes("current_energy_level");
	private static final byte[] GT_CURRENT_LINDSEY_STIRLING_TOKENS_COL =	Bytes.toBytes("current_lindsey_stirling_tokens");		
	private static final byte[] GT_DEVICE_ID_COL =	Bytes.toBytes("device_id");		
	private static final byte[] GT_DEVICE_MODEL_COL =	Bytes.toBytes("device_model");		
	private static final byte[] GT_DEVICE_NAME_COL =	Bytes.toBytes("device_name");		
	private static final byte[] GT_ENABLED_REMOTE_NOTIFICATION_TYPES_COL =	Bytes.toBytes("enabled_remote_notification_types");		
	private static final byte[] GT_EVID_COL =	Bytes.toBytes("evid");		
	private static final byte[] GT_FIRST_PLAY_TIME_COL =	Bytes.toBytes("first_play_time");		
	private static final byte[] GT_FIRST_PLAY_TIME_ZONE_COL =	Bytes.toBytes("first_play_time_zone");		
	private static final byte[] GT_GUID_COL =	Bytes.toBytes("guid");		
	private static final byte[] GT_HARD_CURRENCY_BALANCE_COL =	Bytes.toBytes("hard_currency_balance");
	private static final byte[] GT_IS_INSTALL_COL =	Bytes.toBytes("is_install");		
	private static final byte[] GT_IS_PRODUCTION_BUILD_COL =	Bytes.toBytes("is_production_build");		
	private static final byte[] GT_IS_RELEASE_BUILD_COL =	Bytes.toBytes("is_release_build");		
	private static final byte[] GT_LAST_IAP_TIME_COL =	Bytes.toBytes("last_iap_time");		
	private static final byte[] GT_LAST_LOGIN_TIME_COL =	Bytes.toBytes("last_login_time");		
	private static final byte[] GT_LAST_LOGIN_TIME_ZONE_COL =	Bytes.toBytes("last_login_time_zone");		
	private static final byte[] GT_LEVEL_COL =	Bytes.toBytes("level");		
	private static final byte[] GT_LIFETIME_IAP_SPEND_IN_USD_CENTS_COL =	Bytes.toBytes("lifetime_iap_spend_in_usd_cents");		
	private static final byte[] GT_speednscale_SDK_VERSION_COL =	Bytes.toBytes("sdk_version");		
	private static final byte[] GT_speednscale_TELEMETRY_VERSION_COL =	Bytes.toBytes("speednscale_telemetry_version");
	private static final byte[] GT_OS_VERSION_COL =	Bytes.toBytes("os_version");		
	private static final byte[] GT_OS_VERSION_FULL_COL =	Bytes.toBytes("os_version_full");		
	private static final byte[] GT_PLATFORM_COL =	Bytes.toBytes("platform");		
	private static final byte[] GT_PLAYER_IS_LOGGED_IN_TO_FACEBOOK_COL =	Bytes.toBytes("player_is_logged_in_to_facebook");		
	private static final byte[] GT_PLAYFAB_SESSION_TICKET_COL =	Bytes.toBytes("playfab_session_ticket");		
	private static final byte[] GT_PLAYFAB_TITLE_ID_COL =	Bytes.toBytes("playfab_title_id");		
	private static final byte[] GT_PLAYFAB_USER_ID_COL =	Bytes.toBytes("playfab_user_id");		
	private static final byte[] GT_POPDASH_RELEASE_VERSION_COL =	Bytes.toBytes("popdash_release_version");		
	private static final byte[] GT_POPDASH_TELEMETRY_VERSION_COL =	Bytes.toBytes("popdash_telemetry_version");		
	private static final byte[] GT_SOFT_CURRENCY_BALANCE_COL =	Bytes.toBytes("soft_currency_balance");		
	private static final byte[] GT_TIME_TO_NEXT_ENERGY_REFILL_COL =	Bytes.toBytes("time_to_next_energy_refill");		
	private static final byte[] GT_TITLE_ID_COL = Bytes.toBytes("title_id");
	private static final byte[] GT_TS_COL = Bytes.toBytes("ts");
	private static final byte[] GT_USER_ID_COL = Bytes.toBytes("user_id");
	
	//private static final byte[] GT__MP_SERVER_METADATA_COL = Bytes.toBytes("gt__mp_server_metadata");
	private static final byte[] GT_RECORDNAME_COL =	Bytes.toBytes("recordname");
	private static final byte[] GT_GAME_COL = Bytes.toBytes("game");
	private static final byte[] GT_EXTENDED_DATA_FIELD_KEYS_COL = Bytes.toBytes("extended_data_field_keys");	
	
	/**
	 * Constructor - instantiates an object of the class.
	 * 
	 * @param connection
	 *            configures HBase data source, maintains configuration, provides access to HBase table through
	 *            connection to HBase.
	 */
	public GameTelemetryDao(final HConnection connection) {
		this.connection = connection;
		this.nameSpace = "telemetryV1";
		this.tableName = this.nameSpace + ":" + "game_telemetry";
	}
    
	/**
	 * Returns the name of the GameTelemetry table
	 * 
	 * @return Name of the table
	 */
	public String getTableName() {
		return this.tableName;
	}

	public void addGameTelemetry(:)
			throws IOException {
		if (gameTelemetry == null) {
			return;
		}
		
		// If key is missing then reject the entry
		
	
		HTableInterface gameTelTbl = this.connection.getTable(this.tableName);
		
		Put p = mkPut(Bytes.toBytes(this.getKey(gameTelemetry)));
		
		p.add(GAME_TELEMETRY_FAMILY, GT_AB_GROUPS_COL,	Bytes.toBytes(gameTelemetry.getAbGroups()));
		p.add(GAME_TELEMETRY_FAMILY, GT_ABTESTGROUPIDS_COL,	Bytes.toBytes(gameTelemetry.getAbtestgroupids()));
		p.add(GAME_TELEMETRY_FAMILY, GT_ABTESTGROUPS_COL,	Bytes.toBytes(gameTelemetry.getAbtestgroups()));
		p.add(GAME_TELEMETRY_FAMILY, GT_ABTESTIDS_COL,	Bytes.toBytes(gameTelemetry.getAbtestids()));
		p.add(GAME_TELEMETRY_FAMILY, GT_ABTESTVALUES_COL,	Bytes.toBytes(gameTelemetry.getAbtestvalues()));
		p.add(GAME_TELEMETRY_FAMILY, GT_ADJUST_FIRST_ATTRIBUTED_TRACKER_ACTIVITY_KIND_COL,	
				Bytes.toBytes(gameTelemetry.getAdjustFirstAttributedTrackerActivityKind()));
		p.add(GAME_TELEMETRY_FAMILY, GT_ADJUST_LATEST_ATTRIBUTED_TRACKER_ACTIVITY_KIND_COL,	
				Bytes.toBytes(gameTelemetry.getAdjustLatestAttributedTrackerActivityKind()));
		p.add(GAME_TELEMETRY_FAMILY, GT_APP_BUNDLE_ID_COL,	Bytes.toBytes(gameTelemetry.getAppBundleId()));
		p.add(GAME_TELEMETRY_FAMILY, GT_ADVERTISER_ID_COL,	Bytes.toBytes(gameTelemetry.getAdvertiserId()));
		p.add(GAME_TELEMETRY_FAMILY, GT_ARE_COINS_DOUBLED_COL,	Bytes.toBytes(gameTelemetry.isAreCoinsDoubled()));	
		p.add(GAME_TELEMETRY_FAMILY, GT_BINARY_VERSION_COL,	Bytes.toBytes(gameTelemetry.getBinaryVersion()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_CLIENT_EVENT_TIME_COL,	Bytes.toBytes(gameTelemetry.getClientEventTime()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_CLIENT_EVENT_TIME_ZONE_COL,	Bytes.toBytes(gameTelemetry.getClientEventTimeZone()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_CLIENT_TIMEZONE_OFFSET_COL,	Bytes.toBytes(gameTelemetry.getClientTimezoneOffset()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_CONSECUTIVE_DAYS_PLAYED_COL,	Bytes.toBytes(gameTelemetry.getConsecutiveDaysPlayed()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_COUNT_OF_RUNS_COL,	Bytes.toBytes(gameTelemetry.getCountOfRuns()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_COUNTRY_CODE_COL,	Bytes.toBytes(gameTelemetry.getCountryCode()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_CUMULATIVE_SCORE_COL,	Bytes.toBytes(gameTelemetry.getCumulativeScore()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_CURRENT_COSTUME_COL,	Bytes.toBytes(gameTelemetry.getCurrentCostume()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_CURRENT_ENERGY_LEVEL_COL,	Bytes.toBytes(gameTelemetry.getCurrentEnergyLevel()));
		p.add(GAME_TELEMETRY_FAMILY, GT_CURRENT_LINDSEY_STIRLING_TOKENS_COL,	
				Bytes.toBytes(gameTelemetry.getCurrentLindseyStirlingTokens()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_DEVICE_ID_COL,	Bytes.toBytes(gameTelemetry.getDeviceId()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_DEVICE_MODEL_COL,	Bytes.toBytes(gameTelemetry.getDeviceModel()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_DEVICE_NAME_COL,	Bytes.toBytes(gameTelemetry.getDeviceName()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_ENABLED_REMOTE_NOTIFICATION_TYPES_COL,	
				Bytes.toBytes(gameTelemetry.getEnabledRemoteNotificationTypes()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_EVID_COL,	Bytes.toBytes(gameTelemetry.getEvid()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_FIRST_PLAY_TIME_COL,	Bytes.toBytes(gameTelemetry.getFirstPlayTime()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_FIRST_PLAY_TIME_ZONE_COL,	Bytes.toBytes(gameTelemetry.getFirstPlayTimeZone()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_GUID_COL,	Bytes.toBytes(gameTelemetry.getGuid()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_HARD_CURRENCY_BALANCE_COL,	Bytes.toBytes(gameTelemetry.getHardCurrencyBalance()));
		p.add(GAME_TELEMETRY_FAMILY, GT_IS_INSTALL_COL,	Bytes.toBytes(gameTelemetry.isIsInstall()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_IS_PRODUCTION_BUILD_COL,	Bytes.toBytes(gameTelemetry.isIsProductionBuild()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_IS_RELEASE_BUILD_COL,	Bytes.toBytes(gameTelemetry.isIsReleaseBuild()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_LAST_IAP_TIME_COL,	Bytes.toBytes(gameTelemetry.getLastIapTime()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_LAST_LOGIN_TIME_COL,	Bytes.toBytes(gameTelemetry.getLastLoginTime()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_LAST_LOGIN_TIME_ZONE_COL,	Bytes.toBytes(gameTelemetry.getLastLoginTimeZone()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_LEVEL_COL,	Bytes.toBytes(gameTelemetry.getLevel()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_LIFETIME_IAP_SPEND_IN_USD_CENTS_COL,
				Bytes.toBytes(gameTelemetry.getLifetimeIapSpendInUsdCents()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_speednscale_SDK_VERSION_COL,	Bytes.toBytes(gameTelemetry.getspeednscaleSdkVersion()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_speednscale_TELEMETRY_VERSION_COL,	Bytes.toBytes(gameTelemetry.getspeednscaleTelemetryVersion()));
		p.add(GAME_TELEMETRY_FAMILY, GT_OS_VERSION_COL,	Bytes.toBytes(gameTelemetry.getOsVersion()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_OS_VERSION_FULL_COL,	Bytes.toBytes(gameTelemetry.getOsVersionFull()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_PLATFORM_COL,	Bytes.toBytes(gameTelemetry.getPlatform()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_PLAYER_IS_LOGGED_IN_TO_FACEBOOK_COL,
				Bytes.toBytes(gameTelemetry.isPlayerIsLoggedInToFacebook()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_PLAYFAB_SESSION_TICKET_COL,	Bytes.toBytes(gameTelemetry.getPlayfabSessionTicket()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_PLAYFAB_TITLE_ID_COL,	Bytes.toBytes(gameTelemetry.getPlayfabTitleId()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_PLAYFAB_USER_ID_COL,	Bytes.toBytes(gameTelemetry.getPlayfabUserId()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_POPDASH_RELEASE_VERSION_COL, Bytes.toBytes(gameTelemetry.getPopdashReleaseVersion()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_POPDASH_TELEMETRY_VERSION_COL,	Bytes.toBytes(gameTelemetry.getPopdashTelemetryVersion()));		
		p.add(GAME_TELEMETRY_FAMILY, GT_SOFT_CURRENCY_BALANCE_COL,	Bytes.toBytes(gameTelemetry.getSoftCurrencyBalance()));		

		p.add(GAME_TELEMETRY_FAMILY, GT_TIME_TO_NEXT_ENERGY_REFILL_COL, Bytes.toBytes(gameTelemetry.getTimeToNextEnergyRefill()));
		p.add(GAME_TELEMETRY_FAMILY, GT_TITLE_ID_COL, Bytes.toBytes(gameTelemetry.getTitleId()));
		p.add(GAME_TELEMETRY_FAMILY, GT_TS_COL, Bytes.toBytes(gameTelemetry.getTs()));
		p.add(GAME_TELEMETRY_FAMILY, GT_USER_ID_COL, Bytes.toBytes(gameTelemetry.getUserId()));

		//p.add(GAME_TELEMETRY_FAMILY, GT__MP_SERVER_METADATA_COL, toByteArray(gameTelemetry.getGtMpServerMetadata()));
		int j = 0;
		for (int i = MP_SERVER_METADATA_BEGIN; i < MP_SERVER_METADATA_END; ++i) {
			if (GT_COLUMNS[i].equals("_mp_header_content_length") || GT_COLUMNS[i].equals("server_timestamp"))
				p.add(GAME_TELEMETRY_FAMILY, Bytes.toBytes(GT_COLUMNS[i]), Bytes.toBytes(((Long)gameTelemetry.getGtMpServerMetadata().get(j++))));
			else
				p.add(GAME_TELEMETRY_FAMILY, Bytes.toBytes(GT_COLUMNS[i]), Bytes.toBytes(((String)gameTelemetry.getGtMpServerMetadata().get(j++))));
		}
		
		
		p.add(GAME_TELEMETRY_FAMILY, GT_RECORDNAME_COL, Bytes.toBytes(gameTelemetry.getRecordname()));
		p.add(GAME_TELEMETRY_FAMILY, GT_GAME_COL, Bytes.toBytes(gameTelemetry.getGame()));
		
		//p.add(GAME_TELEMETRY_FAMILY, GT_EXTENDED_DATA_FIELDS_COL, toByteArray(gameTelemetry.getExtendedDataFields()));
		
		//Extended data fields are added as top level columns
		Map<String, String> extendedFields = gameTelemetry.getExtendedDataFields();
		Set<String> keySet = extendedFields.keySet();
		
		//Add extended data fields as top level columns and build '%' separated extended data field keys string
		StringBuilder extendedDataFieldKeys = new StringBuilder();
		for (String colName : keySet) {
			if (extendedDataFieldKeys.length() != 0) {
				//append %, the key value string separator
				extendedDataFieldKeys.append('%');
			}
			extendedDataFieldKeys.append(colName);
			p.add(GAME_TELEMETRY_FAMILY, Bytes.toBytes(colName), Bytes.toBytes(extendedFields.get(colName)));
		}
		
		if (extendedDataFieldKeys.length() != 0) {
			p.add(GAME_TELEMETRY_FAMILY, GT_EXTENDED_DATA_FIELD_KEYS_COL, Bytes.toBytes(extendedDataFieldKeys.toString()));
		}
		
		
		gameTelTbl.put(p);
		gameTelTbl.close();
	}

	public GameTelemetry getGameTelemetry(String gameTelKey)
			throws IOException {
		HTableInterface gameTelTbl = this.connection.getTable(Bytes.toBytes(this.tableName));
		Get g = mkGet(Bytes.toBytes(gameTelKey));
		GameTelemetry gameTelRecord = null;

		g.addFamily(GAME_TELEMETRY_FAMILY);

		Result result = gameTelTbl.get(g);

		if (result.isEmpty()) {
			return null;
		}
		try {
			gameTelRecord = buildGameTelemetry(result);
		}
        catch (IOException ioException) {
        	System.err.println("IO exception while building game telemetry record, table: " + this.tableName);
        	return null;
        }
        catch (ClassNotFoundException classNotFoundException) {
        	System.err.println("Unable to convert byte array to an object");
        	return null;
        }
		gameTelTbl.close();
		return gameTelRecord;
	}
	
	public boolean existsGameTelemetry(String gameTelKey)
			throws IOException {
		HTableInterface gameTelTbl = this.connection.getTable(Bytes.toBytes(this.tableName));
		Get g = mkGet(Bytes.toBytes(gameTelKey));
		GameTelemetry gameTelRecord = null;

		//g.addFamily(GAME_TELEMETRY_FAMILY);

		Result result = gameTelTbl.get(g);

		if (result.isEmpty()) {
			return false;
		}
		
		gameTelTbl.close();
		return true;
	}
	
	/**
	 * Retrieves all objects in the specified range. If startRow and stopRow are null then all objects in the table 
	 * will be returned.
	 * @param startRowKey
	 * @param stopRowKey
	 * @return collection of rows, null if table empty
	 */
	public Collection<GameTelemetry> getGameTelemetryRecords(byte[] startRowKey, byte[] stopRowKey, Filter filterList, int limit) {
		Result result = null;
        List<GameTelemetry> gameTelList = new ArrayList<GameTelemetry>();
        int recordCount = 0;
        
        Scan scan = mkScan(startRowKey, stopRowKey, filterList);
        try {
        	HTableInterface gameTelTbl = this.connection.getTable(Bytes.toBytes(this.tableName));
        	ResultScanner rs = gameTelTbl.getScanner(scan);
    		while ((result = rs.next()) != null && recordCount < limit) {
/*    			if ((recordCount % 10) == 0) {
    	        	System.out.println("Scanned record count: " + recordCount);
    			}*/
    			gameTelList.add(buildGameTelemetry(result));
    			++recordCount;
    		}
    		gameTelTbl.close();
        }
        catch (IOException ioException) {
        	System.err.println("IO exception while opening/getting a scan, or scanning table: " + this.tableName);
        	return null;
        }
        catch (ClassNotFoundException classNotFoundException) {
        	System.err.println("Unable to convert byte array to an object");
        	return null;
        }
		
		return gameTelList;
	}
	
	public void deleteGameTelemetry(String gameTelGuid) throws IOException {
		HTableInterface gameTelTbl = this.connection.getTable(Bytes.toBytes(this.tableName));
		Delete d = mkDel(Bytes.toBytes(gameTelGuid));
		gameTelTbl.delete(d);
		gameTelTbl.close();
		return;
	}
	
	public void deltaProcess(game_telemetry rawGameTelemetry)  {
		GameTelemetry gameTel = GameTelemetryTransformer.doTransformation(DATA_FIELD_COUNT,
				MP_SERVER_METADATA_FIELD_COUNT, rawGameTelemetry);
		
		GameTelemetry gameTelFromHBase = null;
		
		try {
			gameTelFromHBase = getGameTelemetry(this.getKey(gameTel));
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (gameTelFromHBase == null) {
			try {
				// Insert row
				addGameTelemetry(gameTel);
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		else {
			if (compareGameTels(gameTelFromHBase, gameTel)) {
				// Record already exists 
				// Ignore
			}
			else {
				// Check each column, update only changed columns
				// We just don't update now - TBD
				incrementalUpdateGameTelemetry(gameTelFromHBase, gameTel);
			}
		}
	}
	
	private void incrementalUpdateGameTelemetry(GameTelemetry existingGameTel, GameTelemetry newGameTel) {
		return;
	}
	
	public String getKey(GameTelemetry gameTel) {
		return (gameTel.getAdvertiserId() + KEY_COLUMN_SEPARATOR +
				gameTel.getUserId() + KEY_COLUMN_SEPARATOR + gameTel.getGuid());
	}
	
	private GameTelemetry buildGameTelemetry(Result result) throws IOException, ClassNotFoundException  {
		// Build up gt__mp_server_metadata
		gt__mp_server_metadata mpServerMetadata = gt__mp_server_metadata.newBuilder().build();
		
		// Fill in mp_server_metadata fields
		//System.out.println("Building gt__mp-server_metadata...");
		int j = 0;
		for (int i = MP_SERVER_METADATA_BEGIN; i < MP_SERVER_METADATA_END; i++) {
			byte[] columnVal = result.getValue(GAME_TELEMETRY_FAMILY, Bytes.toBytes(GT_COLUMNS[i]));
			if (columnVal == null)
				continue;
			if (GT_COLUMNS[i].equals("_mp_header_content_length") || GT_COLUMNS[i].equals("server_timestamp")) {
				mpServerMetadata.put(j++, Bytes.toLong(columnVal));
			}
			else {
				mpServerMetadata.put(j++, new String(columnVal));
			}
		}
		
		//System.out.println("Building extended data fields map...");
		Map<String, String> extendedDataFields = new HashMap<String, String>();
		byte[] extendedDataFieldBytes = result.getValue(GAME_TELEMETRY_FAMILY, GT_EXTENDED_DATA_FIELD_KEYS_COL);
		if (extendedDataFieldBytes != null) {
			String extendedDataFieldKeys = new String(extendedDataFieldBytes);
			String[] extendedDataFieldKeysArray = extendedDataFieldKeys.split("%");
			for (String extendedDataFieldKey : extendedDataFieldKeysArray) {
				extendedDataFields.put(extendedDataFieldKey, new String(result.getValue(GAME_TELEMETRY_FAMILY, 
						Bytes.toBytes(extendedDataFieldKey))));
			}
		}
		
		//System.out.println("Filling in data fields...");
		GameTelemetry.Builder gameTelBuilder = GameTelemetry.newBuilder()
	    .setGtMpServerMetadata(mpServerMetadata)
		.setRecordname(new String(result.getValue(GAME_TELEMETRY_FAMILY, GT_RECORDNAME_COL)))
		.setGame(new String(result.getValue(GAME_TELEMETRY_FAMILY, GT_GAME_COL)))
		
		.putAllExtended_data_fields(extendedDataFields)
		;
		GameTelemetry gameTel = gameTelBuilder.build();
		
		for (int i = 0; i < DATA_FIELD_COUNT; ++i) {
			byte[] columnVal = result.getValue(GAME_TELEMETRY_FAMILY, Bytes.toBytes(GT_COLUMNS[i]));
			if (columnVal == null)
				continue;
			switch (i) {
			case 5:
			case 6:
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 17:
			case 19:
			case 20:
			case 26:
			case 27:
			case 29:
			case 33:
			case 34:
			case 35:
			case 36:
			case 37:
			case 40:
			case 49:
			case 51:
			case 52:
				gameTel.put(i, Bytes.toLong(columnVal));
				//System.out.println("GameTelemetry column: " + GT_COLUMNS[i] + " , Value: " + gameTel.get(i));
				break;
			case 9:
			case 30:
			case 31:
			case 32:
			case 43:
				gameTel.put(i, Bytes.toBoolean(columnVal));
				//System.out.println("GameTelemetry column: " + GT_COLUMNS[i] + " , Value: " + gameTel.get(i));
				break;
			default:
				gameTel.put(i, new String(columnVal));
				//System.out.println("GameTelemetry column: " + GT_COLUMNS[i] + " , Value: " + gameTel.get(i));
				break;
			}
		}
		
		return gameTel;
	}

	public boolean compareGameTels(GameTelemetry gt1, GameTelemetry gt2) {
		
		//System.out.println("Game1: " + gt1.getGame() + " Game2: " + gt2.getGame());
		if (!(gt1.getGame().equals(gt2.getGame()))) {
			return false;
		}
		
		//System.out.println("Recordname1: " + gt1.getRecordname() + " Recordname2: " + gt2.getRecordname());
		if (!(gt1.getRecordname().equals(gt2.getRecordname()))) {
			return false;
		}
		
		for (int i = 0; i < DATA_FIELD_COUNT; ++i) {
			switch (i) {
			case 5:
			case 6:
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 17:
			case 19:
			case 20:
			case 26:
			case 27:
			case 29:
			case 33:
			case 34:
			case 35:
			case 36:
			case 37:
			case 40:
			case 49:
			case 51:
			case 52:
			case 9:
			case 30:
			case 31:
			case 32:
			case 43:
				//System.out.println("Numeric/boolean Data Field " + i + ": Value1: " + gt1.get(i) + " Value2: " + gt2.get(i));
				if (gt1.get(i) != gt2.get(i)) {
					return false;
				}
				break;
			default:
				//System.out.println("String Data Field " + i + ": Value1: " + gt1.get(i) + " Value2: " + gt2.get(i));
				if (!(gt1.get(i).equals(gt2.get(i))))
					return false;
				break;
			}
		}

		//Compare _mp_server_metadata
		for (int i = 0; i < MP_SERVER_METADATA_FIELD_COUNT; ++i) {
			if (i == 8 || i == 11) {
				if (!(gt1.getGtMpServerMetadata().get(i) == gt2.getGtMpServerMetadata().get(i))) {
					return false;
				}
			}
			else {
				if (!(gt1.getGtMpServerMetadata().get(i).equals(gt2.getGtMpServerMetadata().get(i)))) {
					return false;
				}
			}
		}
		
		//System.out.println("Comparing extended data fields....");
		// Add comparision for Map
		if (gt1.getExtendedDataFields().size() != gt2.getExtendedDataFields().size()) {
			return false;
		}
		
		Map<String, String> extendedFields1 = gt1.getExtendedDataFields();
		Map<String, String> extendedFields2 = gt2.getExtendedDataFields();
		Set<String> keySet2 = extendedFields2.keySet();
		
		for (String key1 : extendedFields1.keySet()) {
			if (keySet2.contains(key1) && extendedFields1.get(key1).equals(extendedFields2.get(key1))) {
				continue;
			}
			else {
				return false;
			}
		}
		
		return true;
	}
}