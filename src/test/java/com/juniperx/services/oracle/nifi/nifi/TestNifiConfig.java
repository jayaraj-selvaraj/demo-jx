package com.juniperx.services.oracle.nifi.nifi;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.http.client.ClientProtocolException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.juniperx.services.oracle.nifi.dao.ExtractDao;
import com.juniperx.services.oracle.nifi.dto.ConnectionDto;
import com.juniperx.services.oracle.nifi.dto.ExtractDto;
import com.juniperx.services.oracle.nifi.dto.FeedDto;
import com.juniperx.services.oracle.nifi.dto.KmsRequestDto;
import com.juniperx.services.oracle.nifi.dto.TableInfoDto;
import com.juniperx.services.oracle.nifi.dto.TableMetadataDto;
import com.juniperx.services.oracle.nifi.util.EncryptUtils;
import com.juniperx.services.oracle.nifi.util.MetadataDBConnectionUtils;
import com.juniperx.services.oracle.nifi.util.NiFiRestClient;

public class TestNifiConfig {

	@Mock
	NiFiRestClient restClient;

	@Mock
	NifiConfig nifiConfig;

	@Mock
	ExtractDao dao;

	@Mock
	EncryptUtils encryptUtil;

	@Mock
	Connection conn;

	@Mock
	Statement statement;

	@Mock
	ResultSet rs;

	@Mock
	MetadataDBConnectionUtils metaDbCon;

	@Before
	public void setupData() throws ClassNotFoundException, SQLException, Exception {

		MockitoAnnotations.initMocks(this);
		// mocking up DB connection
		given(metaDbCon.connectToMetadataDbPostgres(anyString())).willReturn(conn);
		given(conn.createStatement()).willReturn(statement);
		given(statement.execute(anyString())).willReturn(true);
		given(statement.executeQuery(anyString())).willReturn(rs);
		given(rs.next()).willReturn(true).willReturn(true).willReturn(false);
		given(rs.getString(1)).willReturn("NOTRUNNING");
		given(rs.isBeforeFirst()).willReturn(true);
		given(rs.next()).willReturn(true).willReturn(false);
	}

	@Test
	public void getRandomNumberInRangeValidTest() {
		int min = 1;
		int max = 10;
		int num = NifiConfig.getRandomNumberInRange(min, max);
		assertTrue(min <= num && num <= max);
	}

	@Test(expected = IllegalArgumentException.class)
	public void getRandomNumberInRangeInvalidTest() {
		int min = 100;
		int max = 10;
		int num = NifiConfig.getRandomNumberInRange(min, max);
		assertTrue(min <= num && num <= max);
	}

	@Test
	public void getProcessGroupDetailsTest() throws Exception {

		Mockito.doReturn("success").when(restClient).invokeGetRequest(anyString(), anyString());
		NifiConfig nifiConfig = new NifiConfig();
		String response = nifiConfig.getProcessGroupDetails("dummy", "dummy", "dummy", restClient);
		Assert.assertEquals("success", response);

	}

	@Test
	public void getControllerObjectTest() throws ClientProtocolException, IOException, ParseException {
		String json = "{\"controllerServices\":[{\"component\":{\"name\":\"SourceController\"}},{\"component\":{\"name\":\"MetaController\"}}]}";
		JSONObject actualJson = NifiConfig.getControllerObject(json);
		JSONObject controllerComponent = (JSONObject) actualJson.get("component");
		String name = (String) controllerComponent.get("name");
		Assert.assertEquals("SourceController", name);
	}

	@Test
	public void getControllerInfoTest() throws Exception {

		String jsonString = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\"}}";
		JSONObject controllerJsonObject = (JSONObject) new JSONParser().parse(jsonString);
		String output = NifiConfig.getControllerInfo(controllerJsonObject);
		Assert.assertEquals("1234,dummy,1,Enabled", output);

	}

	@Test
	public void processorFreeTest() throws ParseException, org.apache.http.ParseException, IOException {

		String jsonString = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":0}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}}";
		JSONObject controllerJsonObject = (JSONObject) new JSONParser().parse(jsonString);
		NifiConfig config = new NifiConfig();
		String output = config.processorFree(controllerJsonObject);
		Assert.assertEquals("123~1,456~2", output);

	}

	@Test
	public void processorFreeTest2() throws ParseException, org.apache.http.ParseException, IOException {

		String jsonString = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}}";
		JSONObject controllerJsonObject = (JSONObject) new JSONParser().parse(jsonString);
		NifiConfig config = new NifiConfig();
		String output = config.processorFree(controllerJsonObject);
		Assert.assertEquals("Not free", output);

	}

	@Test
	public void getControllerServiceDetailsTest() throws Exception {

		Mockito.doReturn("success").when(restClient).invokeGetRequest(anyString(), anyString());
		NifiConfig config = new NifiConfig();
		String response = config.getControllerServiceDetails("dummy", "dummy", "", restClient);
		Assert.assertEquals("success", response);

	}

	@Test
	public void getClientIdTest() throws Exception {

		String jsonString = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\"}}";
		// JSONObject controllerJsonObject = (JSONObject) new
		// JSONParser().parse(jsonString);
		String output = NifiConfig.getClientId(jsonString);
		Assert.assertEquals("dummy,1", output);

	}

	@Test
	public void getClientIdTest2() throws Exception {

		String jsonString = "{\"id\":1234,\"revision\":{\"version\":1},\"component\":{\"state\":\"Enabled\"}}";

		String output = NifiConfig.getClientId(jsonString);

	}

	@Test
	public void stopProcessorTest() throws Exception {

		Mockito.doReturn(200).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		config.stopProcessor("123", "dummy", "dummy", "token", "src/main/resources/config.properties", restClient);

	}

	@Test(expected = Exception.class)
	public void stopProcessorInvalidTest() throws Exception {
		Mockito.doReturn(201).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		config.stopProcessor("123", "dummy", "dummy", "token", "config.properties", restClient);
	}

	@Test
	public void stopReferencingComponentsTest() throws Exception {

		Mockito.doNothing().when(nifiConfig).stopProcessor(anyString(), anyString(), anyString(), anyString(), anyString(), Mockito.any());
		NifiConfig config = new NifiConfig();
		config.stopReferencingComponents("123~1,345~2", "dummy", "dummy", "dummy", restClient, nifiConfig);

	}

	@Test
	public void disableControllerTest() throws Exception {

		String response = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\"}}";
		Mockito.doReturn(response).when(nifiConfig).getControllerServiceDetails("dummy", "nifi-api/controller-services/id", "token", restClient);
		Mockito.doReturn(200).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		config.disableController("id", "token", "dummy", restClient, nifiConfig);
	}

	@Test(expected = Exception.class)
	public void disableControllerTest2() throws Exception {

		String response = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\"}}";
		Mockito.doReturn(response).when(nifiConfig).getControllerServiceDetails("dummy", "nifi-api/controller-services/id", "token", restClient);
		Mockito.doReturn(201).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		config.disableController("id", "token", "dummy", restClient, nifiConfig);
	}

	@Test
	public void updateControllerTest() throws Exception {
		String response = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Disabled\"}}";
		Mockito.doReturn(response).when(nifiConfig).getControllerServiceDetails("dummy", "nifi-api/controller-services/id", "token", restClient);
		Mockito.doReturn("password").when(encryptUtil).invokeDecryptionServices(Mockito.any(), anyString());
		Mockito.doReturn(200).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		KmsRequestDto kmsRequestDto = new KmsRequestDto();
		config.updateController("dummy", "dummy", 1, "id", "token", "dummy", "dummy", restClient, nifiConfig, encryptUtil, kmsRequestDto);
	}

	@Test(expected = Exception.class)
	public void updateControllerTest2() throws Exception {
		String response = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Disabled\"}}";
		Mockito.doReturn(response).when(nifiConfig).getControllerServiceDetails("dummy", "nifi-api/controller-services/id", "token", restClient);
		Mockito.doReturn("password").when(encryptUtil).decyptPassword(Mockito.any(), Mockito.any(), anyString());
		Mockito.doReturn(201).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		KmsRequestDto kmsRequestDto = new KmsRequestDto();
		config.updateController("dummy", "dummy", 1, "id", "token", "dummy", "dummy", restClient, nifiConfig, encryptUtil, kmsRequestDto);

	}

	@Test
	public void enableControllerTest() throws Exception {

		String response = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\"}}";
		Mockito.doReturn(response).when(nifiConfig).getControllerServiceDetails("dummy", "nifi-api/controller-services/id", "token", restClient);
		Mockito.doReturn(200).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		config.enableController("id", "token", "dummy", restClient, nifiConfig);
	}

	@Test(expected = Exception.class)
	public void enableControllerTest2() throws Exception {

		String response = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\"}}";
		Mockito.doReturn(response).when(nifiConfig).getControllerServiceDetails("dummy", "nifi-api/controller-services/id", "token", restClient);
		Mockito.doReturn(201).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		config.enableController("id", "token", "dummy", restClient, nifiConfig);
	}

	@Test
	public void startReferencingComponentsTest() throws Exception {

		String response = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\"}}";
		String jsonString = "{\"controllerServices\":[{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"name\":\"SourceController\",\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}},{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"name\":\"metaController\",\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}}]}";
		Mockito.doReturn(response).when(nifiConfig).getControllerServiceDetails("dummy", "nifi-api/controller-services/id", "token", restClient);
		Mockito.doReturn(jsonString).when(nifiConfig).getProcessGroupDetails("dummy", "dummy", "token", restClient);
		Mockito.doReturn(200).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		config.startReferencingComponents("id", "dummy", "token", "dummy", restClient, nifiConfig);

	}

	@Test(expected = Exception.class)
	public void startReferencingComponentsTest2() throws Exception {

		String response = "{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\"}}";
		String jsonString = "{\"controllerServices\":[{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"name\":\"SourceController\",\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}},{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}}]}";
		Mockito.doReturn(response).when(nifiConfig).getControllerServiceDetails("dummy", "nifi-api/controller-services/id", "token", restClient);
		Mockito.doReturn(jsonString).when(nifiConfig).getProcessGroupDetails("dummy", "dummy", "token", restClient);
		Mockito.doReturn(201).when(restClient).invokePutRequest(anyString(), anyString(), anyString());
		NifiConfig config = new NifiConfig();
		config.startReferencingComponents("id", "dummy", "token", "dummy", restClient, nifiConfig);

	}

	@Test
	public void createJSONObjectTest() throws Exception {

		NifiConfig config = new NifiConfig();
		ExtractDto extractDto = new ExtractDto();
		FeedDto feedDto = new FeedDto();
		ConnectionDto connDto = new ConnectionDto();
		TableInfoDto tableInfoDto = new TableInfoDto();
		ArrayList<TableMetadataDto> tableArr = new ArrayList<TableMetadataDto>();
		TableMetadataDto tableMetaDto1 = new TableMetadataDto();
		tableMetaDto1.setTable_name("table1");
		tableMetaDto1.setColumns("all");
		tableMetaDto1.setFetch_type("full");
		tableMetaDto1.setWhere_clause("1=1");
		tableArr.add(tableMetaDto1);
		TableMetadataDto tableMetaDto2 = new TableMetadataDto();
		tableMetaDto2.setTable_name("table2");
		tableMetaDto2.setColumns("col1,col2,col3");
		tableMetaDto2.setFetch_type("incr");
		tableMetaDto2.setIncr_col("col3");
		tableMetaDto2.setWhere_clause("1=1");
		tableMetaDto2.setAll_cols("col1,col2,col3");
		tableArr.add(tableMetaDto2);
		feedDto.setCountry_code("uk");
		feedDto.setFeed_id(1);
		feedDto.setFeed_name("dummyFeed");
		feedDto.setProject_sequence(10);
		connDto.setHostName("dummyHostName");
		connDto.setPort("port");
		connDto.setServiceName("serviceName");
		connDto.setUserName("user");
		connDto.setPassword("pwd");
		extractDto.setFeedDto(feedDto);
		extractDto.setConnDto(connDto);
		tableInfoDto.setTableMetadataArr(tableArr);
		tableInfoDto.setFeed_id(1);
		extractDto.setTableInfoDto(tableInfoDto);
		JSONArray actualArr = new JSONArray();
		actualArr = config.createJsonObject("1", extractDto, "dummy", "dummypath", "20190624", "dummy", "dummy", "dummy");
		Mockito.doReturn(201).when(restClient).invokePutRequest(anyString(), anyString(), anyString());

	}

	@SuppressWarnings("unchecked")
	@Test
	public void invokeNifiTest() throws UnsupportedOperationException, Exception {

		NifiConfig config = new NifiConfig();
		JSONArray arr = new JSONArray();
		JSONObject json1 = new JSONObject();
		json1.put("project_sequence", 1);
		json1.put("process_group", 1);
		json1.put("country_code", "uk");
		json1.put("feed_id", 1);
		json1.put("feed_name", "dummyFeed");
		json1.put("date", 20190624);
		json1.put("tableName", "table1");
		arr.add(json1);
		JSONObject json2 = new JSONObject();
		json2.put("project_sequence", 1);
		json2.put("process_group", 1);
		json2.put("country_code", "uk");
		json2.put("feed_id", 1);
		json2.put("feed_name", "dummyFeed");
		json2.put("date", 20190624);
		json2.put("tableName", "table1");
		arr.add(json2);
		Mockito.doReturn(200).when(restClient).invokePostRequest(anyString(), anyString());
		config.invokeNifiFull(arr, "8015", "token", "src/main/resources/config.properties", restClient);

	}

	@SuppressWarnings("unchecked")
	@Test(expected = Exception.class)
	public void invokeNifiTest2() throws UnsupportedOperationException, Exception {

		NifiConfig config = new NifiConfig();
		JSONArray arr = new JSONArray();
		JSONObject json1 = new JSONObject();
		json1.put("project_sequence", 1);
		json1.put("process_group", 1);
		json1.put("country_code", "uk");
		json1.put("feed_id", 1);
		json1.put("feed_name", "dummyFeed");
		json1.put("date", 20190624);
		json1.put("tableName", "table1");
		arr.add(json1);
		JSONObject json2 = new JSONObject();
		json2.put("project_sequence", 1);
		json2.put("process_group", 1);
		json2.put("country_code", "uk");
		json2.put("feed_id", 1);
		json2.put("feed_name", "dummyFeed");
		json2.put("date", 20190624);
		json2.put("tableName", "table1");
		arr.add(json2);
		Mockito.doReturn(201).when(restClient).invokePostRequest(anyString(), anyString());
		config.invokeNifiFull(arr, "8015", "token", "src/main/resources/config.properties", restClient);

	}

	@Test
	public void selectNifiProcessGroupTest() throws Exception {

		Connection conn = metaDbCon.connectToMetadataDbPostgres("src/main/resources/config.properties");
		ExtractDto extractDto = new ExtractDto();
		FeedDto feedDto = new FeedDto();
		ConnectionDto connDto = new ConnectionDto();
		TableInfoDto tableInfoDto = new TableInfoDto();
		ArrayList<TableMetadataDto> tableArr = new ArrayList<TableMetadataDto>();
		TableMetadataDto tableMetaDto1 = new TableMetadataDto();
		tableMetaDto1.setTable_name("table1");
		tableMetaDto1.setColumns("all");
		tableMetaDto1.setFetch_type("full");
		tableMetaDto1.setWhere_clause("1=1");
		tableArr.add(tableMetaDto1);
		TableMetadataDto tableMetaDto2 = new TableMetadataDto();
		tableMetaDto2.setTable_name("table2");
		tableMetaDto2.setColumns("col1,col2,col3");
		tableMetaDto2.setFetch_type("incr");
		tableMetaDto2.setIncr_col("col3");
		tableMetaDto2.setWhere_clause("1=1");
		tableMetaDto2.setAll_cols("col1,col2,col3");
		tableArr.add(tableMetaDto2);
		feedDto.setCountry_code("uk");
		feedDto.setFeed_id(1);
		feedDto.setFeed_name("dummyFeed");
		feedDto.setProject_sequence(10);
		connDto.setHostName("dummyHostName");
		connDto.setPort("port");
		connDto.setServiceName("serviceName");
		connDto.setUserName("user");
		connDto.setPassword("pwd");
		connDto.setConn_type("oracle");
		extractDto.setFeedDto(feedDto);
		extractDto.setConnDto(connDto);
		tableInfoDto.setTableMetadataArr(tableArr);
		tableInfoDto.setFeed_id(1);
		tableInfoDto.setIncr_flag("N");
		extractDto.setTableInfoDto(tableInfoDto);
		// NifiConfig config = new NifiConfig();
		String jsonString = "{\"controllerServices\":[{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"name\":\"SourceController\",\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}},{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"name\":\"AvroRecordSetWriter\",\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}},{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"name\":\"metaController\",\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}}]}\n" + 
				"";
		System.out.println(jsonString);
		Mockito.doReturn("free").when(dao).checkProcessGroupStatus(Mockito.any(), Mockito.anyInt(), Mockito.any());
		Mockito.doReturn(jsonString).when(nifiConfig).getProcessGroupDetails(anyString(), anyString(), anyString(), Mockito.any());
		Mockito.doReturn("123~1,456~2").when(nifiConfig).processorFree(Mockito.any());
		String[] response = NifiConfig.selectNifiProcessGroup(conn, extractDto, "token", "dummy", "src/main/resources/config.properties", restClient,
				nifiConfig, dao);
	}

	@Test(expected = Exception.class)
	public void selectNifiProcessGroupTest2() throws Exception {

		MetadataDBConnectionUtils dbConUtil = new MetadataDBConnectionUtils();
		Connection conn = dbConUtil.connectToMetadataDbPostgres("config.properties");
		ExtractDto extractDto = new ExtractDto();
		FeedDto feedDto = new FeedDto();
		ConnectionDto connDto = new ConnectionDto();
		TableInfoDto tableInfoDto = new TableInfoDto();
		ArrayList<TableMetadataDto> tableArr = new ArrayList<TableMetadataDto>();
		TableMetadataDto tableMetaDto1 = new TableMetadataDto();
		tableMetaDto1.setTable_name("table1");
		tableMetaDto1.setColumns("all");
		tableMetaDto1.setFetch_type("full");
		tableMetaDto1.setWhere_clause("1=1");
		tableArr.add(tableMetaDto1);
		TableMetadataDto tableMetaDto2 = new TableMetadataDto();
		tableMetaDto2.setTable_name("table2");
		tableMetaDto2.setColumns("col1,col2,col3");
		tableMetaDto2.setFetch_type("incr");
		tableMetaDto2.setIncr_col("col3");
		tableMetaDto2.setWhere_clause("1=1");
		tableMetaDto2.setAll_cols("col1,col2,col3");
		tableArr.add(tableMetaDto2);
		feedDto.setCountry_code("uk");
		feedDto.setFeed_id(1);
		feedDto.setFeed_name("dummyFeed");
		feedDto.setProject_sequence(10);
		connDto.setHostName("dummyHostName");
		connDto.setPort("port");
		connDto.setServiceName("serviceName");
		connDto.setUserName("user");
		connDto.setPassword("pwd");
		connDto.setConn_type("oracle");
		extractDto.setFeedDto(feedDto);
		extractDto.setConnDto(connDto);
		tableInfoDto.setTableMetadataArr(tableArr);
		tableInfoDto.setFeed_id(1);
		tableInfoDto.setIncr_flag("N");
		extractDto.setTableInfoDto(tableInfoDto);
		// NifiConfig config = new NifiConfig();
		// String jsonString=
		// "{\"controllerServices\":[{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"name\":\"SourceController\",\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}},{\"id\":1234,\"revision\":{\"clientId\":\"dummy\",\"version\":1},\"component\":{\"name\":\"metaController\",\"state\":\"Enabled\",\"referencingComponents\":[{\"id\":123,\"revision\":{\"version\":1},\"component\":{\"activeThreadCount\":1}},{\"id\":456,\"revision\":{\"version\":2},\"component\":{\"activeThreadCount\":0}}]}}]}";
		Mockito.doThrow(Exception.class).when(nifiConfig).getProcessGroupDetails(anyString(), anyString(), anyString(), Mockito.any());
		Mockito.doReturn("123~1,456~2").when(nifiConfig).processorFree(Mockito.any());
		String response[] = NifiConfig.selectNifiProcessGroup(conn, extractDto, "token", "dummy", "config.properties", restClient, nifiConfig, dao);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void callNifiTest() throws Exception {

		JSONArray arr = new JSONArray();
		JSONObject json1 = new JSONObject();
		json1.put("project_sequence", 1);
		json1.put("process_group", 1);
		json1.put("country_code", "uk");
		json1.put("feed_id", 1);
		json1.put("feed_name", "dummyFeed");
		json1.put("date", 20190624);
		json1.put("tableName", "table1");
		arr.add(json1);
		JSONObject json2 = new JSONObject();
		json2.put("project_sequence", 1);
		json2.put("process_group", 1);
		json2.put("country_code", "uk");
		json2.put("feed_id", 1);
		json2.put("feed_name", "dummyFeed");
		json2.put("date", 20190624);
		json2.put("tableName", "table1");
		arr.add(json2);
		ExtractDto extractDto = new ExtractDto();
		FeedDto feedDto = new FeedDto();
		ConnectionDto connDto = new ConnectionDto();
		TableInfoDto tableInfoDto = new TableInfoDto();
		ArrayList<TableMetadataDto> tableArr = new ArrayList<TableMetadataDto>();
		TableMetadataDto tableMetaDto1 = new TableMetadataDto();
		tableMetaDto1.setTable_name("table1");
		tableMetaDto1.setColumns("all");
		tableMetaDto1.setFetch_type("full");
		tableMetaDto1.setWhere_clause("1=1");
		tableArr.add(tableMetaDto1);
		TableMetadataDto tableMetaDto2 = new TableMetadataDto();
		tableMetaDto2.setTable_name("table2");
		tableMetaDto2.setColumns("col1,col2,col3");
		tableMetaDto2.setFetch_type("incr");
		tableMetaDto2.setIncr_col("col3");
		tableMetaDto2.setWhere_clause("1=1");
		tableMetaDto2.setAll_cols("col1,col2,col3");
		tableArr.add(tableMetaDto2);
		feedDto.setCountry_code("uk");
		feedDto.setFeed_id(1);
		feedDto.setFeed_name("dummyFeed");
		feedDto.setProject_sequence(10);
		connDto.setHostName("dummyHostName");
		connDto.setPort("port");
		connDto.setServiceName("serviceName");
		connDto.setUserName("user");
		connDto.setPassword("pwd");
		connDto.setConn_type("oracle");
		connDto.setConnection_string("connectionString");
		byte[] encrypted_key = new byte[] { 1, 2, 3 };
		byte[] encrypted_password = new byte[] { 1, 2, 3 };
		extractDto.setFeedDto(feedDto);
		extractDto.setConnDto(connDto);
		tableInfoDto.setTableMetadataArr(tableArr);
		tableInfoDto.setFeed_id(1);
		tableInfoDto.setIncr_flag("N");
		extractDto.setTableInfoDto(tableInfoDto);

		Mockito.doNothing().when(nifiConfig).stopReferencingComponents("processorInfo", "clientId", "accessToken", "config.properties", restClient, nifiConfig);
		Mockito.doNothing().when(nifiConfig).disableController("controllerId", "accessToken", "nifi_url", restClient, nifiConfig);
		KmsRequestDto kmsRequestDto = new KmsRequestDto();
		Mockito.doReturn("password").when(nifiConfig).updateController(extractDto.getConnDto().getConnection_string(), extractDto.getConnDto().getUserName(),
				extractDto.getConnDto().getCredentialId(), "controllerId", "accessToken", "config.properties", "nifi_url", restClient, nifiConfig, encryptUtil,
				kmsRequestDto);
		Mockito.doNothing().when(nifiConfig).enableController("controllerId", "accessToken", "nifi_url", restClient, nifiConfig);
		Mockito.doNothing().when(nifiConfig).startReferencingComponents("controllerId", "processGroupUrl", "accessToken", "nifi_url", restClient, nifiConfig);
		Mockito.doReturn(arr).when(nifiConfig).createJsonObject("1", extractDto, extractDto.getConnDto().getConnection_string(), "uk/dummyFeed/20190624/1234/",
				"20190624", "1234", "password", "dummy");
		Mockito.doNothing().when(nifiConfig).invokeNifiFull(arr, "8113", "accessToken", "config.properties", restClient);
		Mockito.doReturn("success").when(nifiConfig).startProcessGroup(1, "accessToken", "src/main/resources/config.properties", restClient, nifiConfig,
				"https://<<nifi-server-url>>:9091/", "controllerId", dao, conn);
		String message = NifiConfig.callNifi(conn, extractDto,
				new String[] { "1---clientId---controllerId---processorInfo", "1---clientId---controllerId---processorInfo" }, "1234", "accessToken",
				"src/main/resources/config.properties", restClient, nifiConfig, dao, "dummy");
		Assert.assertEquals("success", message);

	}

}
