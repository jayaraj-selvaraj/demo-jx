package com.juniperx.services.oracle.nifi.extract;

import java.sql.Connection;

import java.sql.SQLException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.juniperx.services.oracle.nifi.dao.ExtractDao;
import com.juniperx.services.oracle.nifi.dto.ExtractDto;
import com.juniperx.services.oracle.nifi.nifi.NifiConfig;
import com.juniperx.services.oracle.nifi.util.MetadataDBConnectionUtils;
import com.juniperx.services.oracle.nifi.util.NiFiRestClient;

public class App {
	static Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(final String[] args) {

		String feedName = args[0];
		String runId = args[1];
		String path = args[2];
		String date = args[3];

		ExtractDto extractDto = new ExtractDto();
		Connection conn = null;

		try {
			MetadataDBConnectionUtils metaDbConUtil = new MetadataDBConnectionUtils();
			conn = metaDbConUtil.connectToMetadataDbPostgres(path);
			extractDto.setConnDto(ExtractDao.getConnectionObject(conn, feedName));
			logger.info("connection details fetched");
			extractDto.setFeedDto(ExtractDao.getFeedObject(conn, feedName));
			logger.info("feed details fetched");
			String tableList = extractDto.getFeedDto().getTableList();
			logger.info("Oracle-to-Staging JAR called for FEED_NAME: " + feedName + ", RUN_ID: " + runId);
			extractDto.setTableInfoDto(ExtractDao.getTableInfoObject(conn, tableList));
			NiFiRestClient client= new NiFiRestClient();
			NifiConfig nifiConfig= new NifiConfig();
			ExtractDao dao = new ExtractDao();
			String accessToken = NiFiRestClient.getRestAccessToken(path,client,conn);
			String[] processGroupInfo = NifiConfig.selectNifiProcessGroup(conn, extractDto, accessToken, runId, path,client,nifiConfig,dao);

			String status = NifiConfig.callNifi(conn, extractDto, processGroupInfo, runId, accessToken, path,client,nifiConfig,dao,date);
			if (status.equalsIgnoreCase("success")) {
				App.logger.info("Oracle-to-Staging job has been fully triggered for FEED_NAME: " + feedName );
			}
		} catch (Exception e) {
			App.logger.error("Exception occured in main method : " + e);
			return;
		} finally {
			try {
				if(null != conn)
					conn.close();	
			} catch (SQLException e) {
				App.logger.error("Exception occured while closing connection : " + e);
			}
		}
	}
}