package com.juniperx.services.oracle.nifi.util;

import static org.mockito.ArgumentMatchers.anyString;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.juniperx.services.oracle.nifi.constant.MetadataDBConstants;
import com.juniperx.services.oracle.nifi.util.MetadataDBConnectionUtils;

public class MetadataDBConnectionUtilsTest {

	@Mock
	Connection conn;

	@Before
	public void setupData() throws ClassNotFoundException, SQLException, Exception {

		MockitoAnnotations.initMocks(this);

		// given(DriverManager.getConnection(anyString(), anyString(),
		// anyString())).willReturn(conn);

	}

//	 @Test
	public void connectToMetadataDbTest() throws Exception {
//		Class.forName(MetadataDBConstants.ORACLE_DRIVER);
		MetadataDBConnectionUtils util = new MetadataDBConnectionUtils();
		// when(DriverManager.getConnection(anyString(), anyString(),
		// anyString())).thenReturn(conn);
		Connection actualCon = util.connectToMetadataDbPostgres("src/test/resources/config.properties");
		// Assert.assertEquals(conn, actualCon);
	}

}
