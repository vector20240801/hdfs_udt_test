package org.tje.test.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.IOUtils;

public class TestWrite {
	public static void main(String[] args) throws IOException {
		/*Configuration conf = createConfiguration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = null;
		OutputStream fileWriteStream = null;
		//final File readFile = new File("test3");
		try {
			fileWriteStream = fs.create(new Path("hdfs://58.96.170.4:9000/usr/taojiaen/bbbb.wmv"), true);
			in = new FileInputStream(new File("/Users/taojiaen/Desktop/bbbb.wmv"));
			//IOUtils.skipFully(in, 100);
			IOUtils.copyBytes(in, fileWriteStream, 4096, false);
			//fileWriteStream = new FileOutputStream(readFile);
			//IOUtils.copyBytes(in, fileWriteStream, 32L, true);
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(fileWriteStream);
		}*/
		System.out.println("ok");
	}
	private static Configuration createConfiguration() throws IOException {
		final Configuration conf = new Configuration();
		/*conf.addResource("resource/root.keytab");*/
		conf.addResource("resource/core-site.xml");
		conf.set("fs.hdfs.impl", 
		        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		    );
		conf.set(DFSConfigKeys.DFS_CLIENT_USE_UDT, "true");
		//System.setProperty("java.security.krb5.conf", "resource/krb5.conf");
		/*if (UserGroupInformation.isSecurityEnabled()) {
			conf.set(KEYTAB_FILE_KEY, "resource/root.keytab");
			conf.set(USER_NAME_KEY, "root/centos@TAOJIAEN.COM");
		}
		SecurityUtil.login(conf, KEYTAB_FILE_KEY, USER_NAME_KEY, "121.42.153.240");*/
		delete();
		return conf;
	}
	
	public static void delete() throws IOException {
		Configuration conf = createConfiguration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = null;
		OutputStream fileWriteStream = null;
		//final File readFile = new File("test3");
		fs.delete(new Path("hdfs://58.96.170.4:9000/usr/taojiaen/bbbb.wmv"));
		System.out.println("ok");
	}
}
