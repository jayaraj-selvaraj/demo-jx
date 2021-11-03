
package com.juniperx.services.oracle.nifi.util;

import static org.hamcrest.CoreMatchers.is;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.junit.Assert;
import org.junit.Test;

import com.juniperx.services.oracle.nifi.util.EncryptUtils;

/**
 * <p>
 * <b> TODO : Insert description of the class's responsibility/role. </b>
 * </p>
 */
public class EncryptUtilityTest {


    @Test
    public void readFileValidTest() throws IOException {
        String testPath = "src/test/resources/testfile.txt";
        Assert.assertEquals("dummy", EncryptUtils.readFile(testPath));
    }

    @Test
    public void readFileInvalidTest() throws IOException {
        String testPath = "src/test/resources/testfile.txt";
        assertThat("dummyy", is(not(EncryptUtils.readFile(testPath))));
    }

    @Test(expected = IOException.class)
    public void readFileExceptionTest() throws IOException {
        String testPath = "src/test/resources/testfilenotpresent.txt";
        EncryptUtils.readFile(testPath);
    }

    @Test
    public void decryptTextTest() throws Exception {

        byte[] decodedKey = Base64.getDecoder().decode("F62K927314690V8630BCE683B7AC8F55");
        SecretKey secretKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");
        byte[] base_pwd = org.apache.commons.codec.binary.Base64.decodeBase64("YgEVegVzl72+ete91ml2QQ==");

        Assert.assertEquals("Dummy", EncryptUtils.decryptText(base_pwd, secretKey));
    }

    @Test(expected = Exception.class)
    public void decryptTextFailureTest() throws Exception {

        byte[] decodedKey = Base64.getDecoder().decode("F62K927314690V8630BCE683B7AC8F55");
        SecretKey secretKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");
        byte[] base_pwd = org.apache.commons.codec.binary.Base64.decodeBase64("xU7ORjQWKRalKC4rK9Zf4w==");

        Assert.assertEquals("DummyPassword", EncryptUtils.decryptText(base_pwd, secretKey));
    }

    @Test
    public void decodeKeyFromString() throws IOException {
        Assert.assertEquals(SecretKeySpec.class, EncryptUtils.decodeKeyFromString("F62K927314190K8030BGP683B7NC0F55").getClass());

    }

    @Test
    public void encryptTextTest() throws Exception {
        Assert.assertNotNull(EncryptUtils.encryptText("Dummy", "F62K927314690V8630BCE683B7AC8F55"));
    }
    
    @Test(expected = BadPaddingException.class)
   	public void decryptPasswordTest() throws Exception {
   		byte[] key = org.apache.commons.codec.binary.Base64
   				.decodeBase64("3zNVyphgugPWNY7aZDJIrA9nPXACEsU5lAJVh/hlvV8D1b7JshKuSaGpLvSa+EO1");
   		byte[] password = org.apache.commons.codec.binary.Base64
   				.decodeBase64("3zNVyphgugPWNY7aZDJIrA9nPXACEsU5lAJVh/hlvV8D1b7JshKuSaGpLvSa+EO1");

   		EncryptUtils encryptUtils = new EncryptUtils();
   		
   		Assert.assertEquals("EB52D443CA726D7C0C24E9EE469EF43F",
   				encryptUtils.decyptPassword(key, password, "src/test/resources/config.properties"));
   	}
}
