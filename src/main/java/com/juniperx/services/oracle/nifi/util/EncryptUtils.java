package com.juniperx.services.oracle.nifi.util;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Properties;
import java.util.Scanner;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.juniperx.services.oracle.nifi.constant.StringConstants;
import com.juniperx.services.oracle.nifi.dto.KmsRequestDto;

public class EncryptUtils {

    public static SecretKey decodeKeyFromString(final String keyStr) {
        /* Decodes a Base64 encoded String into a byte array */
        String keyString = keyStr.trim();
        byte[] decodedKey = Base64.getDecoder().decode(keyString);

        /* Constructs a secret key from the given byte array */
        return new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");

    }

    public static String readFile(final String pathname) throws IOException {
        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
        String lineSeparator = System.getProperty("line.separator");

        try {
            if (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine());
            }
            while (scanner.hasNextLine()) {
                fileContents.append(lineSeparator + scanner.nextLine());
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

    public static String decryptText(final byte[] byteCipherText, final SecretKey secKey) throws Exception {

        // AES defaults to AES/ECB/PKCS5Padding in Java 7

        Cipher aesCipher = Cipher.getInstance("AES");
        aesCipher.init(Cipher.DECRYPT_MODE, secKey);
        byte[] bytePlainText = aesCipher.doFinal(byteCipherText);
        return new String(bytePlainText);

    }

    public static byte[] encryptText(final String plainText, final String key) throws Exception {

        // AES defaults to AES/ECB/PKCS5Padding in Java 7
        SecretKey secKey = decodeKeyFromString(key);
        Cipher aesCipher = Cipher.getInstance("AES");
        aesCipher.init(Cipher.ENCRYPT_MODE, secKey);
        return aesCipher.doFinal(plainText.getBytes());

    }

    public  String decyptPassword(final byte[] encryptedKey, final byte[] encryptedPassword, final String filePath)
        throws Exception {

        Properties prop = new Properties();
        InputStream input = null;
        input = new FileInputStream(filePath);
        prop.load(input);
        String content = readFile(prop.getProperty("master.key.path"));
        SecretKey secKey = decodeKeyFromString(content);
        String decryptedKey = decryptText(encryptedKey, secKey);
        SecretKey secKey2 = decodeKeyFromString(decryptedKey);
        return decryptText(encryptedPassword, secKey2);

    }
    
    public String invokeDecryptionServices(KmsRequestDto kmsDto,String decryptionUrl) throws UnsupportedOperationException, Exception {
   	  	
    	String str="{ \"projectId\":\""+kmsDto.getProjectId()+"\" ,";
    	str=str+"\"systemSeq\":\""+kmsDto.getSystemSeq()+"\",";
    	str=str+" \"credentialId\":\""+kmsDto.getCredentialId()+"\" }";
    	
    	String res=invokeRest(str,decryptionUrl);
    	JSONObject jsonResponse = (JSONObject) new JSONParser().parse(res);
    	String status = (String)jsonResponse.get("status");
    	if(status.equalsIgnoreCase("success")) {
    		return (String)jsonResponse.get("message");
        	
    	}else {
    		return StringConstants.FAILED;
    	}
    	
    }
    
    public String invokeRest(final String json, final String url) throws Exception {
        String resp = null;
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost postRequest = new HttpPost(url);
        postRequest.setHeader("Content-Type", "application/json");
        StringEntity input = new StringEntity(json);
        postRequest.setEntity(input);
        HttpResponse response = httpClient.execute(postRequest);
        String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new Exception("Error" + responseString);
        } else {
            resp = responseString;
        }
        return resp;
    }

}