package com.juniperx.services.oracle.nifi.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.juniperx.services.oracle.nifi.constant.MetadataDBConstants;

public class MetadataDBConnectionUtils {

    public  Connection connectToMetadataDbPostgres(final String path) throws Exception {

        Properties prop = new Properties();
        InputStream input = null;
        input = new FileInputStream(path);
        prop.load(input);
        
        Class.forName(MetadataDBConstants.POSTGRES_DRIVER);
        String content = EncryptUtils.readFile(prop.getProperty("master.key.path"));
        String connectionUrl = prop.getProperty("postgres.jdbc.url").replaceAll("#pg_ip", prop.getProperty("pg.ip.port.sid"));
        byte[] basePwd = org.apache.commons.codec.binary.Base64.decodeBase64(prop.getProperty("pg.encrypt.pwd"));
        String decodedPwd = EncryptUtils.decryptText(basePwd, EncryptUtils.decodeKeyFromString(content));
        return DriverManager.getConnection(connectionUrl, prop.getProperty("pg.user.name"), decodedPwd);

    }
    
    public Connection connectToSourceDb(String url, String userName, String password) throws ClassNotFoundException, SQLException{
    	  Class.forName(MetadataDBConstants.ORACLE_DRIVER);
    	 return DriverManager.getConnection(url, userName, password);
    	 	  
    }

}