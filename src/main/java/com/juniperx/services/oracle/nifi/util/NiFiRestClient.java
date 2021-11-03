package com.juniperx.services.oracle.nifi.util;

import java.io.BufferedReader;

import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.juniperx.services.oracle.nifi.dto.NiFiAccessTokenDto;

public class NiFiRestClient {
	
    /**
     * Method to make GET Rest with Access Token.
     *
     *
     *
     * @param restURL
     * @param accessToken
     * @return
     * @throws Exception
     */
    public  String invokeGetRequest(final String restURL, final String accessToken) throws Exception {
 
        URL url = new URL(restURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        if(accessToken != null) {
        	conn.setRequestProperty("Authorization", "Bearer " + accessToken);
        }
        
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestMethod("GET");
        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String output;
        StringBuffer responseNew = new StringBuffer();
        while ((output = in.readLine()) != null) {
            responseNew.append(output);
        }
        in.close();
        return responseNew.toString();
    }

    /**
     * Method to make Put Request with Secure Access Token.
     *
     *
     * @param restURL
     * @param inputMessage
     * @param accessToken
     * @return
     * @throws Exception
     */
    public  int invokePutRequest(final String restURL, final String jsonMessage, final String accessToken) throws Exception {

        URL url = new URL(restURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        if( accessToken != null) {
        	conn.setRequestProperty("Authorization", "Bearer " + accessToken);
        }
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
        writer.write(jsonMessage);
        writer.flush();
        writer.close();
        return conn.getResponseCode();
    }

    /**
     * Method to make Post Request with Secure Access Token.
     *
     *
     * @param restURL
     * @param inputMessage
     * @param accessToken
     * @return
     * @throws Exception
     */
    public  int invokePostRequest(final String restURL, final String jsonMessage) throws Exception {

        URL url = new URL(restURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("charset", "utf-8");
        byte[] postData = jsonMessage.getBytes(StandardCharsets.UTF_8);
        conn.setRequestProperty("Content-Length", Integer.toString(postData.length));
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);

        DataOutputStream writer = new DataOutputStream(conn.getOutputStream());
        writer.write(postData);
        return conn.getResponseCode();
    }

    /**
     * Method to get Access Token to make Rest Call to HDF.
     *
     * @return
     * @throws Exception
     */
    public static String getRestAccessToken(final String path,NiFiRestClient client,Connection conn) throws Exception {
    	
        NiFiAccessTokenDto dto = client.getNiFiAccessTokenDetails(path,conn);
        if( dto !=null ) {
        	 String nifiTokenUrl = dto.getUrl();
             CloseableHttpClient httpClient = HttpClientBuilder.create().build();
             HttpPost httpPost = new HttpPost(nifiTokenUrl);
             java.util.List<NameValuePair> params = new ArrayList<>(2);
             params.add(new BasicNameValuePair("username", dto.getUserName()));
             params.add(new BasicNameValuePair("password", dto.getPassword()));

             httpPost.setEntity(new UrlEncodedFormEntity(params));
             HttpResponse response = httpClient.execute(httpPost);
             HttpEntity respEntity = response.getEntity();
             return EntityUtils.toString(respEntity);
        }else {
        	return null;
        }
               
    }
   
    public  NiFiAccessTokenDto getNiFiAccessTokenDetails(final String path,final Connection conn) throws Exception {

        NiFiAccessTokenDto dto = new NiFiAccessTokenDto();
       
        String query = "select ACCESS_TOKEN_URL, ACCESS_TOKEN_USER_NAME, ACCESS_TOKEN_PASSWORD from JUNIPERX.NIFI_ACCESS_TOKEN_PROVIDER";
        String encryptedPassword = null;
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        if (rs.isBeforeFirst()) {
            rs.next();
            dto.setUrl(rs.getString(1));
            dto.setUserName(rs.getString(2));
            encryptedPassword = rs.getString(3);
        }else {
        	return null;
        }
        dto.setPassword(encryptedPassword);
        return dto;
    }
}