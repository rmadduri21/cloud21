package io.speednscale.snsdp.DAO;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hbase.TableExistsException;
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

import io.google.common.base.Optional;
import io.google.inject.Inject;
import io.google.inject.assistedinject.Assisted;

import io.speednscale.snsdp.snsdp_record_models.standard.LineItem;


/**
 * This class is responsible for providing data access features for LINEITEM table.
 */
public class LineItemDao extends AbstractDao {
	private HConnection  connection;
	private String  tableName;
	private String  nameSpace;

	
	public static final byte[] LINEITEM_FAMILY = Bytes.toBytes("l");
	public static final String KEY_SEPARATOR = "_";
	
	private static final byte[] L_ORDERKEY_COL =
			Bytes.toBytes("L_ORDERKEY");
	private static final byte[] L_PARTKEY_COL =
			Bytes.toBytes("L_PARTKEY");
	private static final byte[] L_SUPPKEY_COL =
			Bytes.toBytes("L_SUPPKEY");
	private static final byte[] L_LINENUMBER_COL =
			Bytes.toBytes("L_LINENUMBER");
	private static final byte[] L_QUANTITY_COL =
			Bytes.toBytes("L_QUANTITY");
	private static final byte[] L_EXTENDEDPRICE_COL =
			Bytes.toBytes("L_EXTENDEDPRICE");
	private static final byte[] L_DISCOUNT_COL =
			Bytes.toBytes("L_DISCOUNT");
	private static final byte[] L_TAX_COL =
			Bytes.toBytes("L_TAX");
	private static final byte[] L_RETURNFLAG_COL =
			Bytes.toBytes("L_RETURNFLAG");
	private static final byte[] L_LINESTATUS_COL =
			Bytes.toBytes("L_LINESTATUS");
	private static final byte[] L_SHIPDATE_COL =
			Bytes.toBytes("L_SHIPDATE");
	private static final byte[] L_COMMITDATE_COL =
			Bytes.toBytes("L_COMMITDATE");
	private static final byte[] L_RECEIPTDATE_COL =
			Bytes.toBytes("L_RECEIPTDATE");
	private static final byte[] L_SHIPINSTRUCT_COL =
			Bytes.toBytes("L_SHIPINSTRUCT");
	private static final byte[] L_SHIPMODE_COL =
			Bytes.toBytes("L_SHIPMODE");
	private static final byte[] L_COMMENT_COL =
			Bytes.toBytes("L_COMMENT");
	
	/**
	 * Constructor - instantiates an object of the class.
	 * 
	 * @param dataSource
	 *            configures HBase data source, maintains configuration, provides access to HBase table through
	 *            connection to HBase.
	 */
	public LineItemDao(final HConnection connection) {
		this.connection = connection;

		this.tableName = "tpch.LINEITEM";
		this.nameSpace = "tpch";
	}

	/**
	 * Returns the name of the LineItem table
	 * 
	 * @return Name of the LineItem table
	 */
	public String getTableName() {
		return this.tableName;
	}

	/**
	 * Adds the tumor board object to the rapid:LineItem
	 * @param LineItem
	 * @throws IOException
	 */
	public void addLineItem(LineItem lineItem)
			throws IOException {
		HTableInterface liTable = this.connection.getTable(Bytes.toBytes(this.tableName));
		
		Put p = mkPut(Bytes.toBytes(lineItem.getL_ORDERKEY() + KEY_SEPARATOR + lineItem.getL_LINENUMBER()));
		p.add(LINEITEM_FAMILY, L_ORDERKEY_COL, Bytes.toBytes(LineItem.getL_ORDERKEY()));
		p.add(LINEITEM_FAMILY, L_PARTKEY_COL, Bytes.toBytes(LineItem.getL_PARTKEY()));
		p.add(LINEITEM_FAMILY, L_SUPPKEY_COL, Bytes.toBytes(LineItem.getL_SUPPKEY()));
		p.add(LINEITEM_FAMILY, L_QUANTITY_COL, Bytes.toBytes(LineItem.getL_QUANTITY()));
		p.add(LINEITEM_FAMILY, L_LINENUMBER_COL, Bytes.toBytes(LineItem.getL_LINENUMBER()));
		p.add(LINEITEM_FAMILY, L_EXTENDEDPRICE_COL, Bytes.toBytes(LineItem.getL_EXTENDEDPRICE()));
		p.add(LINEITEM_FAMILY, L_DISCOUNT_COL, Bytes.toBytes(LineItem.getL_DISCOUNT()));
		p.add(LINEITEM_FAMILY, L_TAX_COL, Bytes.toBytes(LineItem.getL_TAX()));
		p.add(LINEITEM_FAMILY, L_RETURNFLAG_COL, Bytes.toBytes(LineItem.getL_RETURNFLAG()));
		p.add(LINEITEM_FAMILY, L_LINESTATUS_COL, Bytes.toBytes(LineItem.getL_LINESTATUS()));
		p.add(LINEITEM_FAMILY, L_SHIPDATE_COL, Bytes.toBytes(LineItem.getL_SHIPDATE()));
		p.add(LINEITEM_FAMILY, L_COMMITDATE_COL, Bytes.toBytes(LineItem.getL_COMMITDATE()));
		p.add(LINEITEM_FAMILY, L_RECEIPTDATE_COL, Bytes.toBytes(LineItem.getL_RECEIPTDATE()));
		p.add(LINEITEM_FAMILY, L_SHIPINSTRUCT_COL, Bytes.toBytes(LineItem.getL_SHIPINSTRUCT()));
		p.add(LINEITEM_FAMILY, L_SHIPMODE_COL, Bytes.toBytes(LineItem.getL_SHIPMODE()));
		p.add(LINEITEM_FAMILY, L_COMMENT_COL, Bytes.toBytes(LineItem.getL_COMMENT()));
		
		liTable.put(p);
		liTable.close();
	}
	
	
	/**
	 * Given a key, this method returns LineItem object
	 * @param lineItemKey
	 * @return LineItem
	 * @throws IOException
	 */
	public LineItem getLineItem(String lineItemKey)
			throws IOException {
		HTableInterface liTable = this.connection.getTable(Bytes.toBytes(this.tableName));
		Get g = mkGet(Bytes.toBytes(lineItemKey));
		
		g.addFamily(LINEITEM_FAMILY);
		
		Result result = liTable.get(g);

		if (result.isEmpty()) {
			return null;
		}

		LineItem lineItem = buildLineItem(result);
		
		liTable.close();
		return lineItem;
	}
	
	/**
	 * Gets LineItem object with the key components L_ORDERKEY and L_LINENUMBER
	 * @param lOrderKey
	 * @param lLineNumber
	 * @return Instance of LineItem object
	 * @throws IOException
	 */
	public LineItem getLineItem(long lOrderKey, int lLineNumber)
			throws IOException {
		HTableInterface liTable = this.connection.getTable(Bytes.toBytes(this.tableName));
		Get g = mkGet(Bytes.toBytes(lOrderKey + KEY_SEPARATOR + lLineNumber));
		
		g.addFamily(LINEITEM_FAMILY);
		
		Result result = liTable.get(g);

		if (result.isEmpty()) {
			return null;
		}
		
		LineItem lineItem = buildLineItem(result);
		
		liTable.close();
		return lineItem;
	}
	
	/**
	 * Retrieves all objects in the specified range. If startRow and stopRow are null then all objects in the table will be returned.
	 * @param startRow
	 * @param stopRow
	 * @return
	 */
	public Collection<LineItem> getLineItems(String startRow, String stopRow, Filter filterList) {
		Result result = null;
        List<LineItem> lineItemList = new ArrayList<LineItem>();
        byte[] startRowBytes = null;
        byte[] stopRowBytes = null;
        
        if (startRow != null) {
        	startRowBytes = Bytes.toBytes(startRow);
        }
        
        if (stopRow != null) {
        	stopRowBytes = Bytes.toBytes(startRow);
        }
		
        Scan scan = mkScan(startRowBytes, stopRowBytes, filterList);
        try {
        	HTableInterface liTable = this.connection.getTable(Bytes.toBytes(this.tableName));
        	ResultScanner rs = liTable.getScanner(scan);
    		while ((result = rs.next()) != null) {
    			lineItemList.add(buildLineItem(result));
    		}
            liTable.close();
        }
        catch (IOException ioException) {
        	System.err.println("IO exception while opening and getting a scan, and scanning table: " + this.tableName);
        	System.exit(1);
        }
		
		return lineItemList;
	}
	
	/**
	 * Deletes the LineItem instance identified by the key components.
	 * @param tumorType
	 * @param LineItemDate
	 * @throws IOException
	 */
	public void deleteLineItem(String tumorType, String LineItemDate) throws IOException {
		HTableInterface liTable = this.connection.getTable(Bytes.toBytes(this.tableName));
		Delete d = mkDel(Bytes.toBytes(tumorType + KEY_SEPARATOR + LineItemDate));
		liTable.delete(d);
		liTable.close();
		return;
	}
	
	/**
	 * Deletes the LineItem instance identified by the key.
	 * @param LineItemKey
	 * @throws IOException
	 */
	public void deleteLineItem(String LineItemKey) throws IOException {
		HTableInterface liTable = this.connection.getTable(Bytes.toBytes(this.tableName));
		Delete d = mkDel(Bytes.toBytes(LineItemKey));
		liTable.delete(d);
		liTable.close();
		return;
	}
	
	private LineItem buildLineItem(Result result) {
		byte[] rowKey = result.getRow();
		String keyString = new String(rowKey);
		LineItem.Builder lineItemBuilder = LineItem.newBuilder();
		try {
			lineItemBuilder.setL_ORDERKEY(Bytes.toLong(result.getValue(LINEITEM_FAMILY, L_ORDERKEY_COL)))
			.setL_ORDERKEY(Bytes.toLong(result.getValue(LINEITEM_FAMILY, L_ORDERKEY_COL)))
			.setL_ORDERKEY(Bytes.toLong(result.getValue(LINEITEM_FAMILY, L_PARTKEY_COL)))
			.setL_ORDERKEY(Bytes.toLong(result.getValue(LINEITEM_FAMILY, L_SUPPKEY_COL)))
			.setL_ORDERKEY(Bytes.toInt(result.getValue(LINEITEM_FAMILY, L_LINENUMBER_COL)))
			.setL_ORDERKEY(Bytes.toDouble(result.getValue(LINEITEM_FAMILY, L_QUANTITY_COL)))
			.setL_ORDERKEY(Bytes.toDouble(result.getValue(LINEITEM_FAMILY, L_EXTENDEDPRICE_COL)))
			.setL_ORDERKEY(Bytes.toDouble(result.getValue(LINEITEM_FAMILY, L_DISCOUNT_COL)))
			.setL_ORDERKEY(Bytes.toDouble(result.getValue(LINEITEM_FAMILY, L_TAX_COL)))
			
			
			.setL_RETURNFLAG(new String(result.getValue(LINEITEM_FAMILY, L_RETURNFLAG_COL)))
			.setL_LINESTATUS(new String(result.getValue(LINEITEM_FAMILY, L_LINESTATUS_COL)))
			
			.setL_SHIPDATE((Date)toObject(result.getValue(LINEITEM_FAMILY, L_SHIPDATE_COL)))
			.setL_COMMITDATE((Date)toObject(result.getValue(LINEITEM_FAMILY, L_COMMITDATE_COL)))
			.setL_RECEIPTDATE((Date)toObject(result.getValue(LINEITEM_FAMILY, L_RECEIPTDATE_COL)))
			
			.setL_SHIPINSTRUCT(new String(result.getValue(LINEITEM_FAMILY, L_SHIPINSTRUCT_COL)))
			.setL_SHIPMODE(new String(result.getValue(LINEITEM_FAMILY, L_SHIPMODE_COL)))
			.setL_COMMENT(new String(result.getValue(LINEITEM_FAMILY, L_COMMENT_COL)));
		}
		catch (IOException ioException) {
			System.err.println("IO exception while LineItem AVRO object generation");
			return null;
		}
		catch (ClassNotFoundException clNotFoundException) {
			System.err.println("Class not found exception while LineItem AVRO object generation");
			return null; 
		}

		return lineItemBuilder.build();
	}
}
