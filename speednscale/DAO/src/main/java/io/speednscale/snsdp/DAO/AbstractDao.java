package io.speednscale.snsdp.DAO;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * The common functionality of DAO's (Data ACcess Objects) is abstracted into the AbstractDao class.
 * Many DAO classes are derived from the AbstractDao.
 */
public abstract class AbstractDao {
	
	public static final String KEY_COLUMN_SEPARATOR = "_";
	public static final String COLUMN_SEPARATOR = ",";

	/**
	 * Returns a habse Put object given byte array key
	 * @param key
	 * @return Put
	 */
	public Put mkPut(byte[] key) {
		Put p = new Put(key);
		return p;
	}
	
	/**
	 * Returns a hbase Delete object given byte array key
	 * @param key
	 * @return Delete object
	 */
	public Delete mkDel(byte[] key) {
		Delete d = new Delete(key);
		return d;
	}
	
	/**
	 * Returns a hbase Get object given byte array key
	 * @param key
	 * @return Hbase Get object
	 */
	public Get mkGet(byte[] key) {
		Get g = new Get(key);
		return g;
	}
	
	/**
	 * Creates a scan object for the row range and sets the filter specified.
	 * @param startRow
	 * @param stopRow
	 * @param filterList
	 * @return scan object
	 */
	public Scan mkScan(byte[] startRow, byte[] stopRow, Filter filterList) {
		Scan s = new Scan();
		
		if (startRow != null) {
			s = s.setStartRow(startRow);
		}
		
		if (stopRow != null) {
			s = s.setStopRow(stopRow);
		}
		
		if (filterList != null) {
			s = s.setFilter(filterList);
		}
		
		return s;
	}
	
	// This belongs in common utils
    public static byte[] toByteArray(Object obj) throws IOException {
        byte[] bytes = null;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
        } finally {
            if (oos != null) {
                oos.close();
            }
            if (bos != null) {
                bos.close();
            }
        }
        return bytes;
    }
    
    // This belongs in common utils
    public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        Object obj = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            obj = ois.readObject();
        } finally {
            if (bis != null) {
                bis.close();
            }
            if (ois != null) {
                ois.close();
            }
        }
        return obj;
    }

    public static String toString(byte[] bytes) {
        return new String(bytes);
    }
}
