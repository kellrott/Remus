package org.remus.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.remus.ConnectionException;
import org.remus.RemusAttach;
import org.remus.thrift.TableRef;
import org.remus.thrift.NotImplemented;

public class AttachTest {

	RemusAttach fs;

	String driver = "org.remus.fs.FileServer";
	@Before public void setUp() throws FileNotFoundException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, ConnectionException {
		
		Class<RemusAttach> cls = (Class<RemusAttach>) Class.forName( driver );
		
		fs = cls.newInstance();
		fs.init(null);
		
		//ds = new ThriftStore();//"testCluster", "localhost:9160", "remus", "remusTable" );
		//ds.initMPStore(new JsonSerializer(), prop );
	}

	
	
	@Test public void attachTest() throws TException, IOException, NoSuchAlgorithmException, NotImplemented {
		String instance1 = "00-testing-01";
		String instance2 = "00-testing-02";

		String applet1 = "@testfile_1";
		String applet2 = "@testfile_2";

		TableRef aRef1 = new TableRef(instance1, applet1);
		TableRef aRef2 = new TableRef(instance2, applet2);

		InputStream is = AttachTest.class.getResourceAsStream("test.txt");
		
		BufferedReader br = new BufferedReader( new InputStreamReader(is) );
		String curLine = null;

		File fTemp = File.createTempFile("remus", "tmp");
		FileOutputStream fos = new FileOutputStream(fTemp);
		
		MessageDigest sha1 = MessageDigest.getInstance("sha-1");
		while ( (curLine = br.readLine()) != null ) {
			sha1.reset();
			
			byte []digest = sha1.digest( curLine.getBytes() );
			Formatter format = new Formatter();
			for (byte b : digest) {
				format.format("%02x", b);
			}
			fos.write( format.toString().getBytes() );
			fos.write("\n".getBytes());
		}
		is.close();
		fos.close();
		String key1 = "key_1";

		fs.copyTo( fTemp, aRef1, key1, "test_file" );
		fTemp.delete();

		File fTemp2 = File.createTempFile("remus", "tmp");
		fs.copyFrom(fTemp2, aRef1, key1, "test_file" );

		is = AttachTest.class.getResourceAsStream("test.txt");	
		br = new BufferedReader( new InputStreamReader(is) );
		
		BufferedReader br2 = new BufferedReader( new InputStreamReader( new FileInputStream(fTemp2) ) );
		String curLine2;
		while ( (curLine = br.readLine()) != null ) {
			
			curLine2 = br2.readLine();
			sha1.reset();
			
			byte []digest = sha1.digest( curLine.getBytes() );
			Formatter format = new Formatter();
			for (byte b : digest) {
				format.format("%02x", b);
			}
			Assert.assertEquals(curLine2, format.toString());
		}
		fTemp2.delete();	
	}
	
	
}
