package com.juniperx.services.oracle.nifi.nifi;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.http.ParseException;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.juniperx.services.oracle.nifi.constant.NifiConstants;
import com.juniperx.services.oracle.nifi.dao.ExtractDao;
import com.juniperx.services.oracle.nifi.dto.ExtractDto;
import com.juniperx.services.oracle.nifi.dto.KmsRequestDto;
import com.juniperx.services.oracle.nifi.dto.TableMetadataDto;
import com.juniperx.services.oracle.nifi.util.EncryptUtils;
import com.juniperx.services.oracle.nifi.util.NiFiRestClient;

public class NifiConfig {

	static Logger logger = LoggerFactory.getLogger(NifiConfig.class);

	static Properties prop = new Properties();
	static InputStream input = null;

	@SuppressWarnings("static-access")
	public static String[] selectNifiProcessGroup(final Connection conn, final ExtractDto extractDto, final String accessToken, final String runId,
			final String filePath, NiFiRestClient restClient, NifiConfig nifiConfig, ExtractDao dao) throws Exception {

		int index = 0;
		String triggerFlag = "N";
		String controllerId = "";
		String clientId = "";
		String controllerVersion = "";
		String controllerStatus = "";
		String processorInfo = "";
		String respEntity = null;
		String avroControllerId = "";
		NifiConfig.input = new FileInputStream(filePath);
		NifiConfig.prop.load(NifiConfig.input);

		try {
			do {
				Thread.currentThread();
				Thread.sleep(10000);
				index = getRandomNumberInRange(1, Integer.parseInt(NifiConfig.prop.getProperty("oracle.process.group.count").trim()));

				String processGroupStatus = dao.checkProcessGroupStatus(conn, index, extractDto.getFeedDto().getFeed_name());
				if (processGroupStatus.equalsIgnoreCase("FREE")) {
					String varname = "oracle.process.group.url." + index;
					String processGroupUrl = NifiConfig.prop.getProperty(varname).trim();

					respEntity = nifiConfig.getProcessGroupDetails(NifiConfig.prop.getProperty("nifi.url").trim(), processGroupUrl, accessToken, restClient);
					//System.out.println(respEntity);
					if (respEntity != null) {
						String content = respEntity;
						JSONObject controllerObj = getControllerObject(content);
						String controllerInfo = getControllerInfo(controllerObj);
						controllerId = controllerInfo.split(",")[0];
						clientId = controllerInfo.split(",")[1];
						controllerStatus = controllerInfo.split(",")[3];

						//// get AvroRecord Set Writer for Snappy compression

						JSONObject controllerObjAvro = getAvroControllerObject(content);
						String controllerInfoAvro = getControllerInfo(controllerObjAvro);
						avroControllerId = controllerInfoAvro.split(",")[0];
						String avroClientId = controllerInfoAvro.split(",")[1];
						String avroControllerStatus = controllerInfoAvro.split(",")[3];

						if (controllerStatus.equalsIgnoreCase("ENABLED") && avroControllerStatus.equalsIgnoreCase("ENABLED")) {

							processorInfo = nifiConfig.processorFree(controllerObj);
							if (!processorInfo.equalsIgnoreCase("NOT FREE")) {
								NifiConfig.logger.info("PG" + index + " details have been fetched");
								triggerFlag = "Y";

							}
						}
					}
				}
			} while (triggerFlag.equalsIgnoreCase("N"));

		} catch (Exception e) {
			dao.updateNifiPgMaster(conn, index);
			NifiConfig.logger.error("Exception ocurred while retrieving Nifi PG details:" + e);
			throw new Exception("Exception ocurred while retrieving Nifi process group details");
		}

		String[] controllers = new String[2];
		controllers[0] = index + "---" + clientId + "---" + controllerId + "---" + processorInfo;
		controllers[1] = index + "---" + clientId + "---" + avroControllerId + "---" + processorInfo;
		return controllers;
	}

	public static String callNifi(final Connection conn, final ExtractDto extractDto, final String[] processGroupInfo, final String runId,
			final String accessToken, final String filePath, NiFiRestClient restClient, NifiConfig nifiConfig, ExtractDao dao, String date) throws Exception {

		EncryptUtils encryptutil = new EncryptUtils();
		String index = processGroupInfo[0].split("---")[0];
		String controllerId = processGroupInfo[0].split("---")[2];
		String avroControllerId = processGroupInfo[1].split("---")[2];

		NifiConfig.input = new FileInputStream(filePath);
		NifiConfig.prop.load(NifiConfig.input);
		String nifi_url = NifiConfig.prop.getProperty("nifi.url");
		String version = null;
		String pwd = null;

		//// update avro controller
		try {
			nifiConfig.disableController(avroControllerId, accessToken, nifi_url, restClient, nifiConfig);
			
			String compressionType = extractDto.getFeedDto().getCompressionType();
			if (compressionType == null) {
				compressionType = "SNAPPY";
			}
 
			nifiConfig.updateControllerForAvro(compressionType, avroControllerId, accessToken, nifi_url, restClient, nifiConfig);
			
			nifiConfig.enableController(avroControllerId, accessToken, nifi_url, restClient, nifiConfig);
		} catch (Exception e) {
			dao.updateNifiPgMaster(conn, Integer.parseInt(index));
			throw new Exception("Execption occured while updating the nifi controller for avro");
		}

		try {
			nifiConfig.disableController(controllerId, accessToken, nifi_url, restClient, nifiConfig);
			KmsRequestDto dto = nifiConfig.getKmsDtoForDecr(extractDto.getConnDto().getSys_seq(), extractDto.getConnDto().getProj_seq(),
					extractDto.getConnDto().getCredentialId());
			pwd = nifiConfig.updateController(extractDto.getConnDto().getConnection_string(), extractDto.getConnDto().getUserName(),
					extractDto.getConnDto().getCredentialId(), controllerId, accessToken, NifiConfig.prop.getProperty("kms.services.decryption.url"), nifi_url,
					restClient, nifiConfig, encryptutil, dto);
			nifiConfig.enableController(controllerId, accessToken, nifi_url, restClient, nifiConfig);
			version = dao.getSourceVersion(extractDto.getConnDto().getConnection_string(), extractDto.getConnDto().getUserName(), pwd);
		} catch (Exception e) {
			e.printStackTrace();
			dao.updateNifiPgMaster(conn, Integer.parseInt(index));
			throw new Exception("Execption occured while updating the nifi controller");
		}

		String startStatus = nifiConfig.startProcessGroup(Integer.parseInt(index), accessToken, filePath, restClient, nifiConfig, nifi_url, controllerId, dao,
				conn);
		if (startStatus.equalsIgnoreCase("error")) {
			throw new Exception("Error while starting Nifi PG");
		} else {
			try {
				String path = extractDto.getFeedDto().getCountry_code() + "/" + extractDto.getFeedDto().getFeed_name() + "/" + date + "/" + runId + "/";
				JSONArray arr = nifiConfig.createJsonObject(index, extractDto, extractDto.getConnDto().getConnection_string(), path, date, runId, version, pwd);
				nifiConfig.invokeNifiFull(arr, prop.getProperty("oracle.listen.http.port"), accessToken, filePath, restClient);
				dao.updateNifiProcessgroupDetails(conn, extractDto, path, date, runId, index);
				return "success";
			} catch (Exception e) {
				logger.error("Exception occured :" + e);
				String var = "oracle.process.group.id." + index;
				String processGroupId = prop.getProperty(var);
				nifiConfig.stopProcessGroup(prop.getProperty("nifi.url"), processGroupId, accessToken, restClient);
				dao.updateNifiPgMaster(conn, Integer.parseInt(index));
				throw new Exception("Exception ocuured while invoking Nifi");
			}
		}

	}

	private KmsRequestDto getKmsDtoForDecr(int sysSeq, int projSeq, int credId) {

		KmsRequestDto dto = new KmsRequestDto();
		dto.setSystemSeq(sysSeq);
		dto.setProjectId(projSeq);
		dto.setCredentialId(credId);

		return dto;
	}

	public static int getRandomNumberInRange(final int min, final int max) {
		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}
		Random r = new Random();
		return r.nextInt((max - min) + 1) + min;
	}

	public String getProcessGroupDetails(final String nifiUrl, final String processGroupUrl, final String accessToken, NiFiRestClient restClient)
			throws Exception {
		return restClient.invokeGetRequest(nifiUrl + processGroupUrl, accessToken);
	}

	@SuppressWarnings("rawtypes")
	public static JSONObject getControllerObject(final String content) throws org.json.simple.parser.ParseException {

		JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
		JSONArray jsonArray = (JSONArray) jsonObject.get("controllerServices");
		Iterator i = jsonArray.iterator();
		while (i.hasNext()) {
			JSONObject controllerObject = (JSONObject) i.next();
			JSONObject controllerComponent = (JSONObject) controllerObject.get("component");
			String name = (String) controllerComponent.get("name");
			if (name.equalsIgnoreCase("SourceController")) {
				return controllerObject;
			}

		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	public static JSONObject getAvroControllerObject(final String content) throws org.json.simple.parser.ParseException {

		JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
		JSONArray jsonArray = (JSONArray) jsonObject.get("controllerServices");
		Iterator i = jsonArray.iterator();
		while (i.hasNext()) {
			JSONObject controllerObject = (JSONObject) i.next();
			JSONObject controllerComponent = (JSONObject) controllerObject.get("component");
			String name = (String) controllerComponent.get("name");
			if (name.equalsIgnoreCase("AvroRecordSetWriter")) {
				return controllerObject;
			}

		}
		return null;
	}

	public static String getControllerInfo(final JSONObject controllerJsonObject) {

		String controllerId = "";
		String controllerClientId = "";
		String controllerVersion = "";
		String controllerStatus = "";

		controllerId = String.valueOf(controllerJsonObject.get("id"));
		JSONObject controllerRevision = (JSONObject) controllerJsonObject.get("revision");
		JSONObject controllerComponent = (JSONObject) controllerJsonObject.get("component");
		controllerClientId = String.valueOf(controllerRevision.get("clientId"));
		controllerVersion = String.valueOf(controllerRevision.get("version"));
		controllerStatus = String.valueOf(controllerComponent.get("state"));
		return controllerId + "," + controllerClientId + "," + controllerVersion + "," + controllerStatus;

	}

	public String processorFree(final JSONObject controllerJsonObject) throws ParseException, IOException, org.json.simple.parser.ParseException {

		int activeThreadCount = 0;
		StringBuffer refComponents = new StringBuffer();
		JSONObject controllerComponent = (JSONObject) controllerJsonObject.get("component");
		JSONArray arrayOfReferencingComponents = (JSONArray) controllerComponent.get("referencingComponents");
		for (int i = 0; i < arrayOfReferencingComponents.size(); i++) {
			JSONObject refComp = (JSONObject) arrayOfReferencingComponents.get(i);
			String refCompId = String.valueOf(refComp.get("id"));
			JSONObject refCompRevision = (JSONObject) refComp.get("revision");
			String refCompVersion = String.valueOf(refCompRevision.get("version"));
			refComponents.append(refCompId + "~" + refCompVersion + ",");
			JSONObject refCompComponent = (JSONObject) refComp.get("component");
			if (!String.valueOf(refCompComponent.get("activeThreadCount")).equals("0")) {
				activeThreadCount++;
			} else {

				NifiConfig.logger.debug("No active thread for Nifi Processor " + refCompId);
			}

		}
		refComponents.setLength(refComponents.length() - 1);
		if (activeThreadCount == 0) {

			return refComponents.toString();
		} else {

			return "Not free";

		}

	}

	public String getControllerServiceDetails(final String nifiUrl, final String controllerServiceUrl, final String accessToken, NiFiRestClient restClient)
			throws Exception {
		return restClient.invokeGetRequest(nifiUrl + controllerServiceUrl, accessToken);

	}

	public static String getClientId(final String content) throws Exception {

		JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
		JSONObject jsonVersionObject = (JSONObject) jsonObject.get("revision");
		String version = jsonVersionObject.get("version").toString();
		Object clientID = jsonVersionObject.get("clientId");

		if (clientID == null) {
			clientID = UUID.randomUUID().toString();
		}

		return clientID + "," + version;

	}

	public void stopReferencingComponents(final String processorInfo, final String clientId, final String accessToken, final String filePath,
			NiFiRestClient restClient, NifiConfig nifiConfig) throws Exception {
		NifiConfig.logger.info("Processor Info is " + processorInfo);
		for (String processor : processorInfo.split(",")) {

			String processorId = processor.split("~")[0];
			String processorVersion = processor.split("~")[1];
			nifiConfig.stopProcessor(processorId, processorVersion, clientId, accessToken, filePath, restClient);
		}
	}

	public void disableController(final String controllerId, final String accessToken, String nifiUrl, NiFiRestClient restClient, NifiConfig nifiConfig)
			throws Exception {
		String respEntity = null;
		String clientId = "";
		String controllerVersion = "";

		respEntity = nifiConfig.getControllerServiceDetails(nifiUrl, "nifi-api/controller-services/" + controllerId, accessToken, restClient);
		if (respEntity != null) {
			String content = respEntity;
			String clientIdVersion = getClientId(content);
			clientId = clientIdVersion.split(",")[0];
			controllerVersion = clientIdVersion.split(",")[1];

		}

		String disableCommand = NifiConstants.DISABLECONTROLLERCOMMAND.replace("${clientid}", clientId).replace("${version}", controllerVersion)
				.replace("${id}", controllerId);
		int statusCode = restClient.invokePutRequest(nifiUrl + "nifi-api/controller-services/" + controllerId, disableCommand, accessToken);

		if (statusCode != 200) {
			throw new Exception("exception occured while disabling controller");
		} else {
			NifiConfig.logger.info("Nifi Controller disabled");
		}

	}

	@SuppressWarnings("static-access")
	public String updateController(final String connUrl, final String uname, int credentialId, final String controllerId, final String accessToken,
			final String decryptionUrl, final String nifi_url, NiFiRestClient restClient, NifiConfig nifiConfig, EncryptUtils encryptutil, KmsRequestDto kmsDto)
			throws Exception {

		String respEntity = null;
		String state = "";
		String clientId = "";
		String controllerVersion = "";

		String password = encryptutil.invokeDecryptionServices(kmsDto, decryptionUrl);

		do {
			Thread.currentThread();
			Thread.sleep(5000);
			respEntity = nifiConfig.getControllerServiceDetails(nifi_url, "nifi-api/controller-services/" + controllerId, accessToken, restClient);
			if (respEntity != null) {
				String content = respEntity;
				state = getControllerState(content);
				if (state.equalsIgnoreCase("DISABLED")) {
					String controllerInfo = getClientId(content);
					clientId = controllerInfo.split(",")[0];
					controllerVersion = controllerInfo.split(",")[1];

				}
			}

		} while (!state.equalsIgnoreCase("DISABLED"));
		StringEntity input = new StringEntity(NifiConstants.UPDATEDBCONNECTIONPOOL.replace("${clientId}", clientId).replace("${ver}", controllerVersion)
				.replace("${contId}", controllerId).replace("${conUrl}", connUrl).replace("${user}", uname).replace("${pasword}", password));

		int statusCode = restClient.invokePutRequest(nifi_url + "nifi-api/controller-services/" + controllerId, EntityUtils.toString(input), accessToken);

		if (statusCode != 200) {
			throw new Exception("exception occured while updating controller");
		} else {
			NifiConfig.logger.info("controller updated");
			return password;
		}

	}

	@SuppressWarnings("static-access")
	public boolean updateControllerForAvro(final String compFormat, final String controllerId, final String accessToken,
			final String nifi_url, NiFiRestClient restClient, NifiConfig nifiConfig) throws Exception {
		String respEntity = null;
		String state = "";
		String clientId = "";
		String controllerVersion = "";


		do {
			Thread.currentThread();
			Thread.sleep(5000);
			respEntity = nifiConfig.getControllerServiceDetails(nifi_url, "nifi-api/controller-services/" + controllerId, accessToken, restClient);
			if (respEntity != null) {
				String content = respEntity;
				state = getControllerState(content);
				if (state.equalsIgnoreCase("DISABLED")) {
					String controllerInfo = getClientId(content);
					clientId = controllerInfo.split(",")[0];
					controllerVersion = controllerInfo.split(",")[1];

				}
			}

		} while (!state.equalsIgnoreCase("DISABLED"));
		StringEntity input = new StringEntity(NifiConstants.AVROCONTROLLER.replace("${clientId}", clientId).replace("${ver}", controllerVersion)
				.replace("${contId}", controllerId).replace("${compFormat}", compFormat));
		
		int statusCode = restClient.invokePutRequest(nifi_url + "nifi-api/controller-services/" + controllerId, EntityUtils.toString(input), accessToken);

		if (statusCode != 200) {
			throw new Exception("exception occured while updating controller");
		} else {
			NifiConfig.logger.info("controller updated");
			return true;
		}

	}

	private String getControllerState(final String content) throws org.json.simple.parser.ParseException {
		JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
		JSONObject controllerComponent = (JSONObject) jsonObject.get("component");
		return controllerComponent.get("state").toString();
	}

	public void enableController(final String controllerId, final String accessToken, String nifi_url, NiFiRestClient restClient, NifiConfig nifiConfig)
			throws Exception {

		String respEntity = null;
		String controllerInfo = "";
		String clientId = "";
		String controllerVersion = "";
		respEntity = nifiConfig.getControllerServiceDetails(nifi_url, "nifi-api/controller-services/" + controllerId, accessToken, restClient);
		if (respEntity != null) {
			String content = respEntity;
			controllerInfo = getClientId(content);
			clientId = controllerInfo.split(",")[0];
			controllerVersion = controllerInfo.split(",")[1];
		}

		StringEntity input = new StringEntity(
				NifiConstants.ENABLEDBCONNECTIONPOOL.replace("${clientId}", clientId).replace("${ver}", controllerVersion).replace("${contId}", controllerId));
		
		int statusCode = restClient.invokePutRequest(nifi_url + "nifi-api/controller-services/" + controllerId, EntityUtils.toString(input), accessToken);
		
		int count=1;

		if(statusCode == 409){
			while(count<=5){
				NifiConfig.logger.warn("Got a 409 response code. Sleeping for 4sec before retrying");
				Thread.sleep(4000);
				statusCode = restClient.invokePutRequest(nifi_url + "nifi-api/controller-services/" + controllerId, EntityUtils.toString(input), accessToken);
				if(statusCode == 200){
					break;
				}
				count++;
			}
		}
		
		if (statusCode != 200) {
			throw new Exception("Exception occured while enabling controller, Status Code :"+statusCode);
		} else {
			NifiConfig.logger.info("Controller enabling started");
		}
	}

	@SuppressWarnings("static-access")
	public void startReferencingComponents(final String controllerId, final String processGroupUrl, final String accessToken, final String nifi_url,
			NiFiRestClient restClient, NifiConfig nifiConfig) throws Exception {

		String respEntity = null;
		String state = "";
		String clientId = "";

		do {
			Thread.currentThread();
			Thread.sleep(5000);
			respEntity = nifiConfig.getControllerServiceDetails(nifi_url, "nifi-api/controller-services/" + controllerId, accessToken, restClient);
			if (respEntity != null) {
				String content = respEntity;
				state = getControllerState(content);

			}

		} while (!state.equalsIgnoreCase("ENABLED"));
		NifiConfig.logger.info("Controller is now enabled");
		respEntity = nifiConfig.getProcessGroupDetails(nifi_url, processGroupUrl, accessToken, restClient);
		if (respEntity != null) {
			String content = respEntity;

			JSONObject controllerObj = getControllerObject(content);
			StringBuffer refComponents = new StringBuffer();
			JSONObject controllerComponent = (JSONObject) controllerObj.get("component");
			JSONArray arrayOfReferencingComponents = (JSONArray) controllerComponent.get("referencingComponents");
			for (int i = 0; i < arrayOfReferencingComponents.size(); i++) {
				JSONObject refComp = (JSONObject) arrayOfReferencingComponents.get(i);
				String refCompId = String.valueOf(refComp.get("id"));
				JSONObject refCompRevision = (JSONObject) refComp.get("revision");
				String refCompVersion = String.valueOf(refCompRevision.get("version"));
				refComponents.append(refCompId + "~" + refCompVersion + ",");
			}
			refComponents.setLength(refComponents.length() - 1);
			for (String processor : refComponents.toString().split(",")) {
				String processorId = processor.split("~")[0];
				String processorVersion = processor.split("~")[1];
				StringEntity input = new StringEntity(
						NifiConstants.STARTPROCESSOR.replace("${id}", processorId).replace("${version}", processorVersion).replace("${clientId}", clientId));

				int statusCode = restClient.invokePutRequest(nifi_url + NifiConstants.PROCESSORURL.replace("${id}", processorId), EntityUtils.toString(input),
						accessToken);

				if (statusCode != 200) {
					throw new Exception("Exception occured while starting Nifi Processor");
				}

			}

		}
	}

	@SuppressWarnings("unchecked")
	public JSONArray createJsonObject(final String index, final ExtractDto extractDto, final String conn_string, final String path, final String date,
			final String runId, final String version, final String pwd) throws Exception {

		JSONArray arr = new JSONArray();
		HashMap<String, JSONObject> map = new HashMap<String, JSONObject>();

		for (TableMetadataDto tableMetadata : extractDto.getTableInfoDto().getTableMetadataArr()) {

			JSONObject json = new JSONObject();
			StringBuffer columnsWithQuotes = new StringBuffer();
			String[] columns = tableMetadata.getColumns().split(",");
			for (String column : columns) {
				column = new StringBuilder().append('\'').append(column).append('\'').append(',').toString();
				columnsWithQuotes.append(column);
			}
			columnsWithQuotes.setLength((columnsWithQuotes.length() - 1));

			json.put("table_name", tableMetadata.getTable_name());
			if (!(tableMetadata.getAll_cols() == null || tableMetadata.getAll_cols().isEmpty())) {

				json.put("columnName", tableMetadata.getAll_cols().split(",")[0]);
			}

			if (!(tableMetadata.getColumns().equalsIgnoreCase("all"))) {

				json.put("columns_where_clause", "upper(column_name) in(" + columnsWithQuotes.toString().toUpperCase() + ")");

			}

			json.put("where_clause", tableMetadata.getWhere_clause());
			if (tableMetadata.getFetch_type().equalsIgnoreCase("INCR")) {
				json.put("incremental_column", tableMetadata.getIncr_col());
			}
			json.put("dt_casting_clause", extractDto.getFeedDto().getDatatype_casting_clause());
			json.put("ts_bi_flg", extractDto.getFeedDto().getTs_bi_flg());
			json.put("file_format", extractDto.getFeedDto().getFile_format());
			json.put("project_sequence", extractDto.getFeedDto().getProject_sequence());
			json.put("process_group", index);
			json.put("country_code", extractDto.getFeedDto().getCountry_code());
			json.put("feed_id", Integer.toString(extractDto.getFeedDto().getFeed_id()));
			json.put("feed_name", extractDto.getFeedDto().getFeed_name());
			json.put("date", date);
			json.put("run_id", runId);
			json.put("path", path);
			json.put("source_host", extractDto.getConnDto().getHostName());
			json.put("source_port", extractDto.getConnDto().getPort());
			json.put("source_service_name", extractDto.getConnDto().getServiceName());
			json.put("source_user", extractDto.getConnDto().getUserName());
			json.put("version", version);
			json.put("source_password", pwd);
			map.put(tableMetadata.getTable_name() + "_obj", json);
			arr.add(map.get(tableMetadata.getTable_name() + "_obj"));

		}

		return arr;

	}

	@SuppressWarnings("rawtypes")
	public void invokeNifiFull(JSONArray arr, String listenHttpPort, String token, String filePath, NiFiRestClient restClient)
			throws UnsupportedOperationException, Exception {
		Iterator it = arr.iterator();
		Properties prop = new Properties();
		InputStream input = null;
		input = new FileInputStream(filePath);
		prop.load(input);
		int processor_cnt = Integer.parseInt(prop.getProperty("listen.http.processor.count"));
		int index = 0;
		while (it.hasNext()) {
			JSONObject json = (JSONObject) it.next();
			index = index + 1;
			if (index > processor_cnt) {
				index = 1;
			}
			String var = "listen.http.url." + index;
			String listenHttpUrl = prop.getProperty(var) + ":" + listenHttpPort + "/contentListener";
			logger.info("listenHttpUrl" + listenHttpUrl);
			int statusCode = restClient.invokePostRequest(listenHttpUrl, json.toString());

			if (statusCode != 200) {
				NifiConfig.logger.error("Nifi could not be triggered: " + statusCode);
				throw new Exception("Nifi not running or some problem in sending HTTP request. Return code is " + statusCode);
			} else {
				NifiConfig.logger.info("Nifi Triggered at URL: " + listenHttpUrl);
			}
		}

	}

	public void stopProcessor(final String id, final String version, final String clientId, final String accessToken, final String filePath,
			NiFiRestClient restClient) throws Exception {
		NifiConfig.input = new FileInputStream(filePath);
		NifiConfig.prop.load(NifiConfig.input);

		StringEntity input = new StringEntity(NifiConstants.STOPPROCESSOR.replace("${id}", id).replace("${version}", version).replace("${clientId}", clientId));

		int statusCode = restClient.invokePutRequest(
				NifiConfig.prop.getProperty("nifi.url") + NifiConfig.prop.getProperty("nifi.processor.url").replace("${processorId}", id),
				EntityUtils.toString(input), accessToken);
		if (statusCode != 200) {
			throw new Exception("Exception occured while stopping Nifi Processor");
		} else {
			NifiConfig.logger.info("Processor with ID " + id + " stopped ");
		}

	}

	public String startProcessGroup(int processGroup, String accessToken, String configFilePath, NiFiRestClient restClient, NifiConfig nifiConfig,
			String nifi_url, String controllerId, ExtractDao dao, Connection conn) throws Exception {

		String respEntity = null;
		String state = "";

		do {
			Thread.currentThread();
			Thread.sleep(5000);
			respEntity = nifiConfig.getControllerServiceDetails(nifi_url, "nifi-api/controller-services/" + controllerId, accessToken, restClient);
			if (respEntity != null) {
				String content = respEntity;
				state = getControllerState(content);

			}

		} while (!state.equalsIgnoreCase("ENABLED"));
		NifiConfig.logger.info("Controller is now enabled");
		Properties prop = new Properties();
		InputStream input = null;
		input = new FileInputStream(configFilePath);
		prop.load(input);
		String processGroupId = "";
		int statusCode = 0;
		String var = "oracle.process.group.id." + processGroup;
		processGroupId = prop.getProperty(var);
		StringEntity request = new StringEntity(NifiConstants.STARTPROCESSGROUP.replace("${id}", processGroupId));

		try {
			statusCode = restClient.invokePutRequest(prop.getProperty("nifi.url") + NifiConstants.PROCESSGROUPURL.replace("${id}", processGroupId),
					EntityUtils.toString(request), accessToken);
		} catch (Exception e) {

			NifiConfig.logger.warn("Exception occured while starting Nifi PG");
			dao.updateNifiPgMaster(conn, processGroup);
			return "Error";
		}

		if (statusCode != 200) {
			NifiConfig.logger.error("Exception occured while starting Nifi PG");
			dao.updateNifiPgMaster(conn, processGroup);
			return "Error";
		} else {
			NifiConfig.logger.info(("Nifi PG with ID " + processGroupId + " has been started."));
			return "success";
		}

	}

	public void stopProcessGroup(String nifiUrl, String processGroupId, String accessToken, NiFiRestClient restClient) throws IOException {

		int statusCode = 0;
		StringEntity request = new StringEntity(NifiConstants.STOPPROCESSGROUP.replace("${id}", processGroupId));
		try {
			statusCode = restClient.invokePutRequest(nifiUrl + NifiConstants.PROCESSGROUPURL.replace("${id}", processGroupId), EntityUtils.toString(request),
					accessToken);
		} catch (Exception e) {

			NifiConfig.logger.warn("Exception occured while stoping Nifi PG. Stop Manually");

		}

		if (statusCode != 200) {
			NifiConfig.logger.warn("Exception occured while stoping Nifi PG. Stop Manually");

		} else {
			NifiConfig.logger.info("Proces group  with ID " + processGroupId + " stopped ");

		}

	}

}
