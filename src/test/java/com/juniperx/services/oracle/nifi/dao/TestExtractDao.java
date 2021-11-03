package com.juniperx.services.oracle.nifi.dao;


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.juniperx.services.oracle.nifi.dto.ConnectionDto;
import com.juniperx.services.oracle.nifi.dto.ExtractDto;
import com.juniperx.services.oracle.nifi.dto.FeedDto;
import com.juniperx.services.oracle.nifi.dto.TableInfoDto;
import com.juniperx.services.oracle.nifi.dto.TableMetadataDto;
import com.juniperx.services.oracle.nifi.util.MetadataDBConnectionUtils;

public class TestExtractDao {
	
	@InjectMocks
	ExtractDao extractDao;
	
	@Mock
	Connection conn;
	
	@Mock
	private Statement statement;
	
	@Mock
	private PreparedStatement pstm;
	
	@Mock
	ResultSet rs;
	
	@Mock
	MetadataDBConnectionUtils metaDbCon;
	
	@Before
	public void setupData() throws ClassNotFoundException, SQLException, Exception {

		MockitoAnnotations.initMocks(this);
		//mocking up DB connection
		given(metaDbCon.connectToMetadataDbPostgres(anyString())).willReturn(conn);
		given(conn.createStatement()).willReturn(statement);
		given(statement.executeQuery(anyString())).willReturn(rs);
		//given(rs.next()).willReturn(true).willReturn(true).willReturn(false);
		//given(rs.isBeforeFirst()).willReturn(true);
		//given(rs.next()).willReturn(true);	
   }
	
	@Test
	public void getConnectionObjectValidTest() throws Exception{
		ConnectionDto connDto=new ConnectionDto();
		connDto.setConn_type("dummy");
        connDto.setHostName("dummy");
        connDto.setPort("123");
        connDto.setUserName("dummy");
        byte[] encrypted_password=new byte[] {1,2,3};
        byte[] encrypted_key=new byte[] {1,2,3};
        connDto.setDbName("dummy");
        connDto.setServiceName("dummy");
       
     
        ExtractDao extDao = Mockito.spy(new ExtractDao());
        when(rs.isBeforeFirst()).thenReturn(true);
        Mockito.when(extDao.getConnectionId(conn, "ITMFR_CBDM_UAT")).thenReturn(639);
        
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn(connDto.getConn_type());
		when(rs.getString(2)).thenReturn(connDto.getHostName());
		when(rs.getString(3)).thenReturn(connDto.getPort());
		when(rs.getString(4)).thenReturn(connDto.getUserName());
		when(rs.getString(5)).thenReturn(connDto.getDbName());
		when(rs.getString(6)).thenReturn(connDto.getServiceName());
		
		ConnectionDto actualConn= ExtractDao.getConnectionObject(conn, "ITMFR_CBDM_UAT");
		Assert.assertEquals("dummy", actualConn.getConn_type());
	}
	
	@Test
	public void getConnectionObjectValidTest2() throws Exception{
		ConnectionDto connDto=new ConnectionDto();
		connDto.setConn_type("dummy");
        connDto.setHostName("dummy");
        connDto.setPort("123");
        connDto.setUserName("dummy");
        byte[] encrypted_password=new byte[] {1,2,3};
        byte[] encrypted_key=new byte[] {1,2,3};
        connDto.setDbName("dummy");
        connDto.setServiceName(null);
       
     
        ExtractDao extDao = Mockito.spy(new ExtractDao());
        when(rs.isBeforeFirst()).thenReturn(true);
        Mockito.when(extDao.getConnectionId(conn, "ITMFR_CBDM_UAT")).thenReturn(639);
        
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn(connDto.getConn_type());
		when(rs.getString(2)).thenReturn(connDto.getHostName());
		when(rs.getString(3)).thenReturn(connDto.getPort());
		when(rs.getString(4)).thenReturn(connDto.getUserName());
		when(rs.getString(5)).thenReturn(connDto.getDbName());
		when(rs.getString(6)).thenReturn(connDto.getServiceName());
		
		ConnectionDto actualConn= ExtractDao.getConnectionObject(conn, "ITMFR_CBDM_UAT");
		Assert.assertEquals("dummy", actualConn.getConn_type());
	}
	
	@Test(expected=Exception.class)
	public void getConnectionObjectInvalidTest() throws Exception{
		
		ConnectionDto connDto=new ConnectionDto();
		connDto.setConn_type("dummy");
        connDto.setHostName("dummy");
        connDto.setPort("123");
        connDto.setUserName("dummy");
        byte[] encrypted_password=new byte[] {1,2,3};
        byte[] encrypted_key=new byte[] {1,2,3};
        connDto.setDbName("dummy");
        connDto.setServiceName("dummy");
        when(rs.isBeforeFirst()).thenReturn(true);
        when(rs.next()).thenReturn(true);
        ExtractDao extDao = Mockito.spy(new ExtractDao());
        Mockito.when(extDao.getConnectionId(conn, "ITMFR_CBDM_UAT")).thenReturn(639);
        when(rs.getString(1)).thenThrow(new Exception("Error Occured"));
        ConnectionDto actualConn= ExtractDao.getConnectionObject(conn, "ITMFR_CBDM_UAT");
		Assert.assertEquals("dummy", actualConn.getConn_type());       
	}
	
	/*@Test(expected=SQLException.class)
	public void getConnectionObjectInvalidIdTest() throws Exception{
		
		ConnectionDto connDto=new ConnectionDto();
		connDto.setConn_type("dummy");
        connDto.setHostName("dummy");
        connDto.setPort("123");
        connDto.setUserName("dummy");
        byte[] encrypted_password=new byte[] {1,2,3};
        connDto.setEncrypted_password(encrypted_password);
        byte[] encrypted_key=new byte[] {1,2,3};
        connDto.setEncr_key(encrypted_key);
        connDto.setDbName("dummy");
        connDto.setServiceName("dummy");
        
        ExtractDao extDao = Mockito.spy(new ExtractDao());
       // Mockito.doThrow(new SQLException("abc")).when(extDao.getConnectionId(Mockito.anyObject(), "ITMFR_CBDM_UAT"));
        when(extDao.getConnectionId(conn, "ITMFR_CBDM_UAT")).thenThrow(SQLException.class);
        when(rs.getString(1)).thenThrow(new Exception("Error Occured"));
        ConnectionDto actualConn= ExtractDao.getConnectionObject(conn, "ITMFR_CBDM_UAT");
		Assert.assertEquals("dummy", actualConn.getConn_type());
        
	}*/
	
	@Test
	public void getConnectionIdValidTest() throws Exception{
		
		
		when(rs.isBeforeFirst()).thenReturn(true);
		when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(1);
        ExtractDao dao=new ExtractDao();
        int id= dao.getConnectionId(conn, "ITMFR_CBDM_UAT");
		Assert.assertEquals(1, id);
	}
	
	@Test(expected=Exception.class)
	public void getConnectionIdInvalidTest() throws Exception{
		
		
		when(rs.isBeforeFirst()).thenReturn(true);
		when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenThrow(new Exception("invalidname"));
        ExtractDao dao=new ExtractDao();
        int id= dao.getConnectionId(conn, "ITMFR_CBDM_UAT");
		Assert.assertEquals(1, id);  
	}
	
	@Test
	public void getFeedObjectValidTest() throws Exception{
		
		FeedDto feedDto = new FeedDto();
		 feedDto.setFeed_id(123);
         feedDto.setFeed_name("dummy");
         feedDto.setCountry_code("dummy");
         feedDto.setProject_sequence(123);
         feedDto.setTableList("123");
         
         
         when(rs.isBeforeFirst()).thenReturn(true);
         when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
         when(rs.getString(1)).thenReturn(String.valueOf(feedDto.getFeed_id())).thenReturn("castingClause").thenReturn("bigint").thenReturn("csv").thenReturn("123");
 		 when(rs.getString(2)).thenReturn(feedDto.getFeed_name());
 		 when(rs.getString(3)).thenReturn(feedDto.getCountry_code());
 		 when(rs.getString(4)).thenReturn(String.valueOf(feedDto.getProject_sequence()));
 		 
 		 FeedDto actualFeedDto= ExtractDao.getFeedObject(conn, "abc");
 		 Assert.assertEquals(123, actualFeedDto.getFeed_id());
 		 Assert.assertEquals("123", actualFeedDto.getTableList());     
	}
	
	@Test(expected=Exception.class)
	public void getFeedObjectInvalidValidTest() throws Exception{
		
		FeedDto feedDto = new FeedDto();
		 feedDto.setFeed_id(123);
         feedDto.setFeed_name("dummy");
         feedDto.setCountry_code("dummy");
         feedDto.setProject_sequence(123);
         feedDto.setTableList("123");
         
         when(rs.isBeforeFirst()).thenReturn(true);
         when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
         when(rs.getString(1)).thenReturn(String.valueOf(feedDto.getFeed_id()));
 		 when(rs.getString(2)).thenThrow(SQLException.class);
 		 
 		 
 		 FeedDto actualFeedDto= ExtractDao.getFeedObject(conn, "abc");
 		 Assert.assertEquals(123, actualFeedDto.getFeed_id());
 		 Assert.assertEquals("123", actualFeedDto.getTableList());
         
	}
	
	@Test(expected=Exception.class)
	public void getFeedObjectInvalidValidTest2() throws Exception{
		
		FeedDto feedDto = new FeedDto();
		 feedDto.setFeed_id(123);
         feedDto.setFeed_name("dummy");
         feedDto.setCountry_code("dummy");
         feedDto.setProject_sequence(123);
         feedDto.setTableList("123");
         
         when(rs.isBeforeFirst()).thenReturn(true);
         when(rs.next()).thenReturn(true).thenThrow(SQLException.class);
         when(rs.getString(1)).thenReturn(String.valueOf(feedDto.getFeed_id()));
         when(rs.getString(2)).thenReturn(feedDto.getFeed_name());
         
 		 
 		 
 		 FeedDto actualFeedDto= ExtractDao.getFeedObject(conn, "abc");
 		 Assert.assertEquals(123, actualFeedDto.getFeed_id());
 		 Assert.assertEquals("123", actualFeedDto.getTableList());
         
	}
	
	@Test
	public void getTableInfoObjectValidTest() throws Exception{
		
		 TableInfoDto tableInfoDto = new TableInfoDto();
		 ArrayList<TableMetadataDto> tableMetadataArr = new ArrayList<TableMetadataDto>();
		 TableMetadataDto tableMetadata= new TableMetadataDto();
		 tableMetadata.setTable_name("table1");
         tableMetadata.setColumns("all");
         tableMetadata.setWhere_clause("1=1");
         tableMetadata.setFetch_type("full");
         tableMetadata.setIncr_col(null);
         tableMetadata.setView_flag("n");
         tableMetadata.setView_source_schema(null);
         tableMetadata.setAll_cols(null);
         tableMetadataArr.add(tableMetadata);
         TableMetadataDto tableMetadata2= new TableMetadataDto();
		 tableMetadata2.setTable_name("table2");
         tableMetadata2.setColumns("all");
         tableMetadata2.setWhere_clause("1=1");
         tableMetadata2.setFetch_type("full");
         tableMetadata2.setIncr_col(null);
         tableMetadata2.setView_flag("n");
         tableMetadata2.setView_source_schema(null);
         tableMetadata2.setAll_cols(null);
         tableMetadataArr.add(tableMetadata2);
         tableInfoDto.setTableMetadataArr(tableMetadataArr);
         
         when(rs.isBeforeFirst()).thenReturn(true);
         when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
         
         when(rs.getString(1)).thenReturn(tableMetadata.getTable_name()).thenReturn(tableMetadata2.getTable_name());
         when(rs.getString(2)).thenReturn(tableMetadata.getColumns()).thenReturn(tableMetadata2.getColumns());
         when(rs.getString(3)).thenReturn(tableMetadata.getWhere_clause()).thenReturn(tableMetadata2.getWhere_clause());
         when(rs.getString(4)).thenReturn(tableMetadata.getFetch_type()).thenReturn(tableMetadata2.getFetch_type());
         when(rs.getString(5)).thenReturn(tableMetadata.getIncr_col()).thenReturn(tableMetadata2.getIncr_col());
         when(rs.getString(6)).thenReturn(tableMetadata.getView_flag()).thenReturn(tableMetadata2.getView_flag());
         when(rs.getString(7)).thenReturn(tableMetadata.getView_source_schema()).thenReturn(tableMetadata2.getView_source_schema());
         when(rs.getString(8)).thenReturn(tableMetadata.getAll_cols()).thenReturn(tableMetadata2.getAll_cols());
         
         TableInfoDto actualTableInfoDto=ExtractDao.getTableInfoObject(conn, "table1,table2");
        
         //Assert.assertEquals(actualTableInfoDto.getTableMetadataArr().toArray()[0], tableInfoDto.getTableMetadataArr().toArray()[0]);
         String[] actualTableName=new String[2];
         String[] tableName=new String[2];
         int index=0;
         int index2=0;
         for(TableMetadataDto tblMetaDto:actualTableInfoDto.getTableMetadataArr()){
        	 
        	 actualTableName[index]=tblMetaDto.getTable_name();
        	 index++;
        	 
         }
         for(TableMetadataDto tblMetaDto: tableInfoDto.getTableMetadataArr()){
        	 tableName[index2]=tblMetaDto.getTable_name();
        	 index2++;
         }
         assertArrayEquals(actualTableName, tableName);
	}
	
	@Test
	public void getTableInfoObjectValidTest2() throws Exception{
		
		 TableInfoDto tableInfoDto = new TableInfoDto();
		 ArrayList<TableMetadataDto> tableMetadataArr = new ArrayList<TableMetadataDto>();
		 TableMetadataDto tableMetadata= new TableMetadataDto();
		 tableMetadata.setTable_name("table1");
         tableMetadata.setColumns("all");
         tableMetadata.setWhere_clause("1=1");
         tableMetadata.setFetch_type("incr");
         tableMetadata.setIncr_col(null);
         tableMetadata.setView_flag("n");
         tableMetadata.setView_source_schema(null);
         tableMetadata.setAll_cols(null);
         tableMetadataArr.add(tableMetadata);
         TableMetadataDto tableMetadata2= new TableMetadataDto();
		 tableMetadata2.setTable_name("table2");
         tableMetadata2.setColumns("all");
         tableMetadata2.setWhere_clause("1=1");
         tableMetadata2.setFetch_type("incr");
         tableMetadata2.setIncr_col(null);
         tableMetadata2.setView_flag("n");
         tableMetadata2.setView_source_schema(null);
         tableMetadata2.setAll_cols(null);
         tableMetadataArr.add(tableMetadata2);
         tableInfoDto.setTableMetadataArr(tableMetadataArr);
         
         when(rs.isBeforeFirst()).thenReturn(true);
         when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
         
         when(rs.getString(1)).thenReturn(tableMetadata.getTable_name()).thenReturn(tableMetadata2.getTable_name());
         when(rs.getString(2)).thenReturn(tableMetadata.getColumns()).thenReturn(tableMetadata2.getColumns());
         when(rs.getString(3)).thenReturn(tableMetadata.getWhere_clause()).thenReturn(tableMetadata2.getWhere_clause());
         when(rs.getString(4)).thenReturn(tableMetadata.getFetch_type()).thenReturn(tableMetadata2.getFetch_type());
         when(rs.getString(5)).thenReturn(tableMetadata.getIncr_col()).thenReturn(tableMetadata2.getIncr_col());
         when(rs.getString(6)).thenReturn(tableMetadata.getView_flag()).thenReturn(tableMetadata2.getView_flag());
         when(rs.getString(7)).thenReturn(tableMetadata.getView_source_schema()).thenReturn(tableMetadata2.getView_source_schema());
         when(rs.getString(8)).thenReturn(tableMetadata.getAll_cols()).thenReturn(tableMetadata2.getAll_cols());
         
         TableInfoDto actualTableInfoDto=ExtractDao.getTableInfoObject(conn, "table1,table2");


         //Assert.assertEquals(actualTableInfoDto.getTableMetadataArr().toArray()[0], tableInfoDto.getTableMetadataArr().toArray()[0]);
         String[] actualTableName=new String[2];
         String[] tableName=new String[2];
         int index=0;
         int index2=0;
         for(TableMetadataDto tblMetaDto:actualTableInfoDto.getTableMetadataArr()){
        	 
        	 actualTableName[index]=tblMetaDto.getTable_name();
        	 index++;
        	 
         }
         for(TableMetadataDto tblMetaDto: tableInfoDto.getTableMetadataArr()){
        	 tableName[index2]=tblMetaDto.getTable_name();
        	 index2++;
         }
         assertArrayEquals(actualTableName, tableName);
	}
	
	@Test(expected=Exception.class)
	public void getTableInfoObjectInvalidTest() throws Exception{
		
		 TableInfoDto tableInfoDto = new TableInfoDto();
		 ArrayList<TableMetadataDto> tableMetadataArr = new ArrayList<TableMetadataDto>();
		 TableMetadataDto tableMetadata= new TableMetadataDto();
		 tableMetadata.setTable_name("table1");
         tableMetadata.setColumns("all");
         tableMetadata.setWhere_clause("1=1");
         tableMetadata.setFetch_type("incr");
         tableMetadata.setIncr_col(null);
         tableMetadata.setView_flag("n");
         tableMetadata.setView_source_schema(null);
         tableMetadata.setAll_cols(null);
         tableMetadataArr.add(tableMetadata);
         TableMetadataDto tableMetadata2= new TableMetadataDto();
		 tableMetadata2.setTable_name("table2");
         tableMetadata2.setColumns("all");
         tableMetadata2.setWhere_clause("1=1");
         tableMetadata2.setFetch_type("incr");
         tableMetadata2.setIncr_col(null);
         tableMetadata2.setView_flag("n");
         tableMetadata2.setView_source_schema(null);
         tableMetadata2.setAll_cols(null);
         tableMetadataArr.add(tableMetadata2);
         tableInfoDto.setTableMetadataArr(tableMetadataArr);
         
         when(rs.isBeforeFirst()).thenReturn(true);
         when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
         
         when(rs.getString(1)).thenReturn(tableMetadata.getTable_name()).thenThrow(SQLException.class);
        
         
         TableInfoDto actualTableInfoDto=ExtractDao.getTableInfoObject(conn, "table1,table2");


         //Assert.assertEquals(actualTableInfoDto.getTableMetadataArr().toArray()[0], tableInfoDto.getTableMetadataArr().toArray()[0]);
         String[] actualTableName=new String[2];
         String[] tableName=new String[2];
         int index=0;
         int index2=0;
         for(TableMetadataDto tblMetaDto:actualTableInfoDto.getTableMetadataArr()){
        	 
        	 actualTableName[index]=tblMetaDto.getTable_name();
        	 index++;
        	 
         }
         for(TableMetadataDto tblMetaDto: tableInfoDto.getTableMetadataArr()){
        	 tableName[index2]=tblMetaDto.getTable_name();
        	 index2++;
         }
         assertArrayEquals(actualTableName, tableName);
	}
	
	@Test
	public void getProcessGroupValidTest() throws Exception{
		
		when(rs.isBeforeFirst()).thenReturn(true);
		when(rs.getInt(1)).thenReturn(1);
		int index=0;
		index=ExtractDao.getProcessGroup(conn, "abc", "uk");
		Assert.assertEquals(index, 1);
	}
	
	@Test
	public void getProcessGroupValidTest2() throws Exception{
		
		when(rs.isBeforeFirst()).thenReturn(false);
		when(rs.getInt(1)).thenReturn(1);
		int index=0;
		index=ExtractDao.getProcessGroup(conn, "abc", "uk");
		Assert.assertEquals(index, 0);
	}
	
	@Test
	public void checkProcessGroupStatusvalid() throws Exception{
		
		when(rs.isBeforeFirst()).thenReturn(true);
		when(rs.next()).thenReturn(true);
		when(rs.getString(1)).thenReturn("U");
		ExtractDao dao = new ExtractDao();
		String status=dao.checkProcessGroupStatus(conn, 1,"test");
		Assert.assertEquals(status, "Not Free");
	}
	
	@Test
	public void checkProcessGroupStatusvalid2() throws Exception{
		
		when(rs.isBeforeFirst()).thenReturn(true);
		when(rs.next()).thenReturn(true);
		when(rs.getString(1)).thenReturn(("A"));
		when(statement.execute(anyString())).thenReturn(true);
		ExtractDao dao = new ExtractDao();
		String status=dao.checkProcessGroupStatus(conn, 1,"test");
		System.out.println(status);
		Assert.assertEquals(status, "Free");
	}
	
	/*@Test
	public void checkProcessGroupStatusvalid3() throws Exception{
		
		when(rs.isBeforeFirst()).thenReturn(true);
		when(rs.next()).thenReturn(true);
		when(rs.getString(1)).thenReturn("not running");
		ExtractDao dao = new ExtractDao();
		String status=dao.checkProcessGroupStatus(conn, 1);
		Assert.assertEquals(status, "Free");
	}*/
	
	@Test
	public void updateNifiProcessgroupDetailsValid() throws SQLException{
		
		ExtractDto extractDto = new ExtractDto();
		FeedDto feedDto = new FeedDto();
		feedDto.setFeed_name("dummy");
		feedDto.setFeed_id(123);
		feedDto.setProject_sequence(1);
		ConnectionDto connDto= new ConnectionDto();
		connDto.setConn_type("dummy");
		extractDto.setFeedDto(feedDto);
		extractDto.setConnDto(connDto);
		when(statement.execute(anyString())).thenReturn(true);
		ExtractDao dao = new ExtractDao();
		dao.updateNifiProcessgroupDetails(conn, extractDto, "abc", "20190809", "1234", "1");
	    assertThat(true);
	}
	
	@Test
	public void testUpdateNifiPgMaster() throws Exception {
		given(conn.prepareStatement(Mockito.anyString())).willReturn(pstm);
		given(pstm.executeQuery()).willReturn(rs);
		extractDao.updateNifiPgMaster(conn, 1);
	}
}
