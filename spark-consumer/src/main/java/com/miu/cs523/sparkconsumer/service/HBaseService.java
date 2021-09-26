package com.miu.cs523.sparkconsumer.service;
import java.io.IOException;
import java.util.Date;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

@Service 
public class HBaseService {
	private static final String TABLE_NAME = "tweets";
	private static final String CF_HashTag = "tweet";
	private static final String CF_popularity = "Popularity";
	
	public void createTable(Connection connection ) throws IOException {
		Admin admin = connection.getAdmin();
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
		if (!admin.tableExists(table.getTableName()))
		{
			System.out.print("Creating table.... ");
//			admin.disableTable(table.getTableName());
//			admin.deleteTable(table.getTableName());
			table.addFamily(new HColumnDescriptor(CF_popularity).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_HashTag).setCompressionType(Algorithm.NONE));
			admin.createTable(table);
			System.out.println("Done creating table");
		}
		else
		  System.out.println("Table already exists");
		
	
		
	}
	public void saveData(Connection connection, String table, String hash, String c) throws IOException {
	    Table htable = connection.getTable(TableName.valueOf(table));
		Put put1 = new Put(Bytes.toBytes(new Date().getTime() + ""));
		put1.addColumn(Bytes.toBytes(CF_HashTag), Bytes.toBytes("hashTages"), Bytes.toBytes(hash));
		put1.addColumn(Bytes.toBytes(CF_popularity), Bytes.toBytes("count"), Bytes.toBytes(c));

		try {
			htable.put(put1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

