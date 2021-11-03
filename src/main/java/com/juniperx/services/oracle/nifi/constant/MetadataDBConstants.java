package com.juniperx.services.oracle.nifi.constant;

public class MetadataDBConstants {
	
	protected MetadataDBConstants(){
		
	}
	
	public static final String JUNIPERSCHEMA = "JUNIPERX";
	public static final String ORACLE_DRIVER="oracle.jdbc.driver.OracleDriver";
	public static final String SYSTEMTABLE = "JUNIPER_SYSTEM_MASTER";
	public static final String PROJECTTABLE = "JUNIPER_PROJECT_MASTER";
	public static final String KEYTABLE = "JUNIPER_EXT_KEY_MASTER";
	public static final String CONNECTIONTABLE="JUNIPER_EXT_SRC_CONN_MASTER";
	public static final String GETSEQUENCEID="Select  DATA_DEFAULT from USER_TAB_COLUMNS where TABLE_NAME = '${tableName}' and COLUMN_NAME='${columnName}'";
	public static final String CONNECTIONTABLEKEY="SRC_CONN_SEQUENCE";
	public static final String INSERTQUERY = "insert into {$table}({$columns}) values({$data})";
	public static final String GETLASTROWID="SELECT ${id}.currval from dual";
	public static final String COMMA = ",";
	public static final String TEMPTABLEDETAILSTABLE = "JUNIPER_EXT_TABLE_MASTER_TEMP";
	public static final String QUOTE="'";
	public static final String FEEDSRCTGTLINKTABLE = "JUNIPER_EXT_FEED_SRC_TGT_LINK";
	public static final String TABLEDETAILSTABLE = "JUNIPER_EXT_TABLE_MASTER";
	public static final String FEEDTABLE = "JUNIPER_EXT_FEED_MASTER";
	public static final String NIFISTATUSTABLE = "JUNIPER_EXT_NIFI_STATUS";
	public static final String CUSTDATATYPEMAPTABLE="JUNIPER_EXT_CUST_DATATYP_MAP";
	public static final String CUSTTARGETTABLE="JUNIPER_EXT_CUSTOMIZE_TARGET";
	public static final String DEFAULTDTCASTINGCLAUSE=" WHEN c.data_type like '%FLOAT%' THEN 'CAST('|| c.column_name ||' as NUMBER(38,9)) as ' || c.column_name ||','"
			+ "WHEN c.data_type like 'TIMESTAMP%' THEN 'CAST(SYS_EXTRACT_UTC('|| c.column_name ||') AS TIMESTAMP(6)) as ' || c.column_name ||','"
			+"WHEN c.data_type = 'DATE' THEN 'SYS_EXTRACT_UTC(CAST('|| c.column_name ||' AS TIMESTAMP(6))) as ' || c.column_name ||','";
	public static final String NIFIPGMASTERTABLE = "JUNIPER_NIFI_PG_MASTER";
	public static final String POSTGRES_DRIVER = "org.postgresql.Driver";
}