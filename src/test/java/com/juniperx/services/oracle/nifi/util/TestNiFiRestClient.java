package com.juniperx.services.oracle.nifi.util;
import java.io.IOException;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.juniperx.services.oracle.nifi.dao.ExtractDao;
import com.juniperx.services.oracle.nifi.dto.NiFiAccessTokenDto;
import com.juniperx.services.oracle.nifi.util.MetadataDBConnectionUtils;
import com.juniperx.services.oracle.nifi.util.NiFiRestClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertArrayEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNiFiRestClient {
	
	
	
	@Mock
	Connection conn;
	
	@Mock
	private Statement statement;
	
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
	public void invokeGetRequestTest() throws Exception{
		
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(8000), 0);
        httpServer.createContext("/api/endpoint", new HttpHandler() {
               @Override
               public void handle(HttpExchange exchange) throws IOException {
                     byte[] response = "{\"success\": true}".getBytes();
                     exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
                     exchange.getResponseBody().write(response);
                     exchange.close();
               }
        });
        httpServer.start();
        try{
        	NiFiRestClient restClient= new NiFiRestClient();
        	String status = restClient.invokeGetRequest("http://localhost:8000/api/endpoint", "abc");
        	Assert.assertEquals("{\"success\": true}", status);
        }finally{
        	httpServer.stop(0);
        	Thread.sleep(2000);
        }

	}
	
	@Test
	public void invokePutRequestTest() throws Exception{
		
		String json="{\"message\":\"abc\",\"body\":\"xyz\"}";
		 HttpServer httpServer = HttpServer.create(new InetSocketAddress(8000), 0);
	        httpServer.createContext("/api/endpoint", new HttpHandler() {
	               @Override
	               public void handle(HttpExchange exchange) throws IOException {
	                     byte[] response = "{\"success\": true}".getBytes();
	                     exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
	                     exchange.getResponseBody().write(response);
	                     exchange.close();
	               }
	        });
	        httpServer.start();
	        try{
	        	NiFiRestClient restClient= new NiFiRestClient();
	        	int  code = restClient.invokePutRequest("http://localhost:8000/api/endpoint", json, "token");
	        	Assert.assertEquals(200, code);
	        }finally{
	        	httpServer.stop(0);
	        	Thread.sleep(2000);
	        }
	}
	
	@Test
	public void invokePostRequestTest() throws Exception{
		String json="{\"message\":\"abc\",\"body\":\"xyz\"}";
		 HttpServer httpServer = HttpServer.create(new InetSocketAddress(8000), 0);
	        httpServer.createContext("/api/endpoint", new HttpHandler() {
	               @Override
	               public void handle(HttpExchange exchange) throws IOException {
	                     byte[] response = "{\"success\": true}".getBytes();
	                     exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
	                     exchange.getResponseBody().write(response);
	                     exchange.close();
	               }
	        });
	        httpServer.start();
	        try{
	        	NiFiRestClient restClient= new NiFiRestClient();
	        	int  code = restClient.invokePostRequest("http://localhost:8000/api/endpoint", json);
	        	Assert.assertEquals(200, code);
	        }finally{
	        	httpServer.stop(0);
	        	Thread.sleep(2000);
	        }
	}
	
	@Test
	public void getRestAccessTokenTest() throws Exception{
		
		 HttpServer httpServer = HttpServer.create(new InetSocketAddress(8000), 0);
	        httpServer.createContext("/api/endpoint", new HttpHandler() {
	               @Override
	               public void handle(HttpExchange exchange) throws IOException {
	                     byte[] response = "{\"success\": true}".getBytes();
	                     exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
	                     exchange.getResponseBody().write(response);
	                     exchange.close();
	               }
	        });
	     httpServer.start();
		 NiFiAccessTokenDto dto = new NiFiAccessTokenDto();
		 dto.setUserName("user");
		 dto.setPassword("pwd");
		 dto.setUrl("http://localhost:8000/api/endpoint");
		 
		 NiFiRestClient client = Mockito.mock(NiFiRestClient.class);
	//	 NiFiRestClient client = Mockito.spy(new NiFiRestClient());
		 //when(client.getRestAccessToken("abc")).then
		 //doReturn(NiFiAccessTokenDto.class).when(methodParameter).getParameterType();
		 Mockito.doReturn(dto).when(client).getNiFiAccessTokenDetails("path",conn);
		 
		 try{
			 String response=NiFiRestClient.getRestAccessToken("path",client,conn);
			 Assert.assertEquals("{\"success\": true}", response);
		 }finally{
			 httpServer.stop(0);
			 Thread.sleep(2000);
		 }
	}
	
	@Test
	public void getNiFiAccessTokenDetailsTest() throws Exception{
		
		NiFiAccessTokenDto dto = new NiFiAccessTokenDto();
		 dto.setUserName("user");
		 dto.setPassword("pwd");
		 dto.setUrl("http://localhost:8000/api/endpoint");
		
		when(rs.isBeforeFirst()).thenReturn(true);
		when(rs.next()).thenReturn(true);
		when(rs.getString(1)).thenReturn(dto.getUrl());
		when(rs.getString(2)).thenReturn(dto.getUserName());
		when(rs.getString(3)).thenReturn(dto.getPassword());
		
		 NiFiRestClient client = new NiFiRestClient();
		NiFiAccessTokenDto actualDto= client.getNiFiAccessTokenDetails("path", conn);
		Assert.assertEquals("user", actualDto.getUserName());
		
		
	}
	
}
