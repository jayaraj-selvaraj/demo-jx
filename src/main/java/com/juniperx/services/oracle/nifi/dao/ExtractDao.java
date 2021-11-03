package com.juniperx.services.oracle.nifi.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.juniperx.services.oracle.nifi.constant.MetadataDBConstants;
import com.juniperx.services.oracle.nifi.dto.ConnectionDto;
import com.juniperx.services.oracle.nifi.dto.ExtractDto;
import com.juniperx.services.oracle.nifi.dto.FeedDto;
import com.juniperx.services.oracle.nifi.dto.TableInfoDto;
import com.juniperx.services.oracle.nifi.dto.TableMetadataDto;
import com.juniperx.services.oracle.nifi.util.MetadataDBConnectionUtils;

public class ExtractDao {

	static Logger logger = LoggerFactory.getLogger(ExtractDao.class);

	public static ConnectionDto getConnectionObject(final Connection conn, final String feedName) throws SQLException {

		int connId = 0;
		ConnectionDto connDto = new ConnectionDto();
		try {
			ExtractDao extDao= new ExtractDao();
			connId = extDao.getConnectionId(conn, feedName);
		} catch (SQLException e) {
			ExtractDao.logger.error("Error occured in getConnectionObject :" + e);
			throw new SQLException("Exception ocurred while fetching source connection Id");
		} 
		String query =
				"select src_conn_type,host_name,port_no,username,credential_id,database_name,service_name,system_sequence,project_sequence from "
						+MetadataDBConstants.JUNIPERSCHEMA+"." + MetadataDBConstants.CONNECTIONTABLE + " where src_conn_sequence=" + connId;

		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if (rs.isBeforeFirst()) {
			rs.next();
			connDto.setConn_type(rs.getString(1));
			connDto.setHostName(rs.getString(2));
			connDto.setPort(rs.getString(3));
			connDto.setUserName(rs.getString(4));
			connDto.setCredentialId(rs.getInt(5));
			connDto.setDbName(rs.getString(6));
			connDto.setServiceName(rs.getString(7));
			connDto.setSys_seq(rs.getInt(8));
			connDto.setProj_seq(rs.getInt(9));

		}

		if (connDto.getServiceName() == null) {
			connDto.setConnection_string(
					"jdbc:oracle:thin:@" + connDto.getHostName() + ":" + connDto.getPort() + ":" + connDto.getDbName());
		} else {

			connDto.setConnection_string(
					"jdbc:oracle:thin:@//" + connDto.getHostName() + ":" + connDto.getPort() + "/" + connDto.getServiceName());
		}
		return connDto;
	}

	public  int getConnectionId(final Connection conn, final String feedName) throws SQLException {

		int connectionId = 0;
		String query = "select src_conn_sequence from " +MetadataDBConstants.JUNIPERSCHEMA+"."+ MetadataDBConstants.FEEDSRCTGTLINKTABLE
				+ " where feed_sequence=(select feed_sequence from " +MetadataDBConstants.JUNIPERSCHEMA+"."+ MetadataDBConstants.FEEDTABLE + " where feed_unique_name='"
				+ feedName + "')";
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if (rs.isBeforeFirst()) {

			rs.next();
			connectionId = rs.getInt(1);

		}
		return connectionId;

	}

	public static FeedDto getFeedObject(final Connection conn, final String feedName) throws SQLException {

		FeedDto feedDto = new FeedDto();

		String query = "select feed_sequence,feed_unique_name,country_code,project_sequence from "+MetadataDBConstants.JUNIPERSCHEMA+"." + MetadataDBConstants.FEEDTABLE
				+ " where feed_unique_name='" + feedName + "'";

		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			if (rs.isBeforeFirst()) {
				rs.next();

				feedDto.setFeed_id(Integer.parseInt(rs.getString(1)));
				feedDto.setFeed_name(rs.getString(2));
				feedDto.setCountry_code(rs.getString(3));
				String projectSequence = rs.getString(4);
				if (!(projectSequence == null)) {
					feedDto.setProject_sequence(Integer.parseInt(projectSequence));
				}
			}

		} catch (SQLException e) {
			ExtractDao.logger.error("Exception occured while retrieving feed details :" + e);
			throw new SQLException("Exception occured while retrieving feed details");
		}

		String query2="select case_stmt from "+MetadataDBConstants.JUNIPERSCHEMA+"."+MetadataDBConstants.CUSTDATATYPEMAPTABLE+" where feed_id="+feedDto.getFeed_id();
		StringBuffer castingClause= new StringBuffer();
		try{
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query2);
			if(rs.isBeforeFirst()){
				while(rs.next()){

					castingClause.append(rs.getString(1));

				}
				feedDto.setDatatype_casting_clause(castingClause.toString());
			}else{
				feedDto.setDatatype_casting_clause(MetadataDBConstants.DEFAULTDTCASTINGCLAUSE);
			}

			String query4="select tgt_data_typ from "+MetadataDBConstants.JUNIPERSCHEMA+"."+MetadataDBConstants.CUSTDATATYPEMAPTABLE+" where feed_id="+feedDto.getFeed_id()
			+" and upper(src_data_typ) in ('TIMESTAMP','DATE')";

			rs=statement.executeQuery(query4);
			if(rs.isBeforeFirst()){
				while(rs.next()){

					if(rs.getString(1).equalsIgnoreCase("BIGINT")){

						feedDto.setTs_bi_flg("Y");
						break;
					}
				}

			}else{
				feedDto.setTs_bi_flg("N");
			}
			if(null==feedDto.getTs_bi_flg()){
				feedDto.setTs_bi_flg("N");
			}
			String query5="select FILE_FORMAT_TYPE, compression_type from "+MetadataDBConstants.JUNIPERSCHEMA+"."+MetadataDBConstants.CUSTTARGETTABLE+" where feed_sequence="+feedDto.getFeed_id();
			rs=statement.executeQuery(query5);
			if(rs.isBeforeFirst()){
				rs.next();

				feedDto.setFile_format(rs.getString(1));
				feedDto.setCompressionType(rs.getString(2));

			}else{
				feedDto.setFile_format("AVRO");
			}
		}catch(SQLException e){
			ExtractDao.logger.error("Exception occured while retrieving datatype casting details :" + e.getMessage());
			throw new SQLException("Exception occured while  retrieving datatype casting details");
		}

		StringBuffer tableList = new StringBuffer();
		String query3 =
				"select table_sequence from "+MetadataDBConstants.JUNIPERSCHEMA+"." + MetadataDBConstants.TABLEDETAILSTABLE + " where feed_sequence=" + feedDto.getFeed_id();
		try {
			ExtractDao.logger.info("query for table is "+query3);
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query3);
			if (rs.isBeforeFirst()) {
				while (rs.next()) {
					tableList.append(rs.getString(1) + ",");
				}
			}
		} catch (SQLException e) {
			ExtractDao.logger.error("Exception occured while fetching table information of the feed :" + e.getMessage());
			throw new SQLException("Exception occured while fetching table information of the feed");
		}
		tableList.setLength(tableList.length() - 1);
		feedDto.setTableList(tableList.toString());

		return feedDto;

	}

	public static TableInfoDto getTableInfoObject(final Connection conn, final String table_list) throws Exception {

		TableInfoDto tableInfoDto = new TableInfoDto();
		ArrayList<TableMetadataDto> tableMetadataArr = new ArrayList<TableMetadataDto>();
		String[] tableIds = table_list.split(",");
		Statement statement = null;
		try {
			for (String tableId : tableIds) {
				String query =
						"select table_name,columns,where_clause,fetch_type,incr_col,view_flag,view_source_schema,all_cols from "
								+MetadataDBConstants.JUNIPERSCHEMA+"."+ MetadataDBConstants.TABLEDETAILSTABLE + " where table_sequence=" + tableId;

				statement = conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if (rs.isBeforeFirst()) {
					rs.next();
					TableMetadataDto tableMetadata = new TableMetadataDto();
					tableMetadata.setTable_name(rs.getString(1));
					tableMetadata.setColumns(rs.getString(2));
					tableMetadata.setWhere_clause(rs.getString(3));
					tableMetadata.setFetch_type(rs.getString(4));
					tableMetadata.setIncr_col(rs.getString(5));
					tableMetadata.setView_flag(rs.getString(6));
					tableMetadata.setView_source_schema(rs.getString(7));
					tableMetadata.setAll_cols(rs.getString(8));
					tableMetadataArr.add(tableMetadata);
				}

				rs.close();
			}

			for (TableMetadataDto table : tableMetadataArr) {
				if (table.getFetch_type().equalsIgnoreCase("incr")) {
					tableInfoDto.setIncr_flag("Y");
					break;
				} else {
					tableInfoDto.setIncr_flag("N");
				}
			}
			ExtractDao.logger.info("Fetched teble list from Metadata DB");
		} catch (SQLException e) {
			ExtractDao.logger.error("Exception occured while fetching table list from Metadata DB :" + e);
			throw new SQLException("Exception ocurred while getting table list");
		} finally {
			if(statement != null)
				statement.close();
		}

		tableInfoDto.setTableMetadataArr(tableMetadataArr);
		return tableInfoDto;

	}

	public static int getProcessGroup(final Connection conn, final String feed_name, final String country_code) throws Exception {

		String query = "select distinct nifi_pg from " +MetadataDBConstants.JUNIPERSCHEMA+"."+ MetadataDBConstants.NIFISTATUSTABLE + " where feed_unique_name='"
				+ feed_name + "' and country_code='" + country_code + "'";

		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if (rs.isBeforeFirst()) {
			rs.next();
			return rs.getInt(1);
		} else {
			return 0;
		}

	}

	public  String checkProcessGroupStatus(final Connection conn, final int index,final String feedName)
			throws SQLException {

		String query = "select status from " +MetadataDBConstants.JUNIPERSCHEMA+"."+ MetadataDBConstants.NIFIPGMASTERTABLE + " where pg_index=" + index
				+" and lower(pg_type)='oracle'";
		Statement statement = conn.createStatement();
		try {
			statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			if (rs.isBeforeFirst()) {
				rs.next();
				if (rs.getString(1).equalsIgnoreCase("A")) {

					ExtractDao.logger.info("PG"+index+ " is available");
					String updateQuery="update "+MetadataDBConstants.JUNIPERSCHEMA+"."+MetadataDBConstants.NIFIPGMASTERTABLE+" set status='U' , feed_name='"+feedName+"' where pg_index="+index
							+" and lower(pg_type)='oracle'";
					ExtractDao.logger.info(updateQuery);
					statement.executeUpdate(updateQuery);
					return "Free";
				}else{
					ExtractDao.logger.info("PG"+index+ " is NOT available. Checking availability of other PG");
				}

			}
		} catch (SQLException e) {
			ExtractDao.logger.error("Exception occured while checking process group status :" + e.getMessage()
			+ " At line number :" + e.getStackTrace()[0].getLineNumber());
			throw e;
		} finally {
			statement.close();
		}
		return "Not Free";

	}

	public  void updateNifiProcessgroupDetails(final Connection conn, final ExtractDto extractDto, final String path,
			final String date, final String runId, final String index) throws SQLException {

		String deleteQuery = "delete from "+MetadataDBConstants.JUNIPERSCHEMA+"." + MetadataDBConstants.NIFISTATUSTABLE + " where feed_unique_name='"
				+ extractDto.getFeedDto().getFeed_name() + "' and run_id='" + runId + "' and job_type='R'";

		Statement statement = conn.createStatement();
		statement.execute(deleteQuery);

		String insertQuery = MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.JUNIPERSCHEMA+"."+MetadataDBConstants.NIFISTATUSTABLE)
				.replace("{$columns}",	"country_code,feed_id,feed_unique_name,run_id,nifi_pg,pg_type,extracted_date,project_sequence,job_type,job_name,status")
				.replace("{$data}",
						MetadataDBConstants.QUOTE + extractDto.getFeedDto().getCountry_code() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + extractDto.getFeedDto().getFeed_id() + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + extractDto.getFeedDto().getFeed_name() + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + runId + MetadataDBConstants.QUOTE
						+ MetadataDBConstants.COMMA + index + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ extractDto.getConnDto().getConn_type() + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + date + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ extractDto.getFeedDto().getProject_sequence() + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE + "R"
						+ MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA + MetadataDBConstants.QUOTE
						+ extractDto.getFeedDto().getFeed_name() + "_read" + MetadataDBConstants.QUOTE + MetadataDBConstants.COMMA
						+ MetadataDBConstants.QUOTE + "running" + MetadataDBConstants.QUOTE);

		statement.execute(insertQuery);

	}

	public void updateNifiPgMaster(final Connection conn, int pgIndex) throws SQLException{

		String query2="update "+MetadataDBConstants.JUNIPERSCHEMA+"."+MetadataDBConstants.NIFIPGMASTERTABLE
				+" set status='A' where lower(pg_type)=? and pg_index=?";

		PreparedStatement statement2 = conn.prepareStatement(query2);
		statement2.setString(1, "oracle");
		statement2.setInt(2, pgIndex);
		statement2.executeUpdate();

	}

	public  String getSourceVersion(String connectionString, String userName, String pwd) throws Exception {
		MetadataDBConnectionUtils connUtil = new MetadataDBConnectionUtils();
		Connection srcConn = null;
		String query = "select * from v$version";
		String version = "12c";
		
		try{
			srcConn = connUtil.connectToSourceDb(connectionString, userName, pwd);
			Statement stmt = srcConn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			if(rs.isBeforeFirst()){
				rs.next();

				if(!(rs.getString(1).split("Oracle Database ")[1].startsWith("12"))){
					version = "11g";
				}

			}
			return version;
		}catch(SQLException e){
			throw new SQLException("Exception while connecting to source DB -"+e.getMessage());
		}finally{
			if(srcConn!=null){
				srcConn.close();
			}

		}

	}

}