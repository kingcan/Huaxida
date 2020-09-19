package com.sdmc.www

import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Scan}
import org.apache.hadoop.hbase.util.Bytes



class HBaseUtil3() extends Serializable {
   ///这是一行一行的读
	def getValuesByRowKey(tableName:String,
												cf:String,
												rowKey:String
											 ):ConcurrentHashMap[String,String]= {

		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		hbaseConfig.set("zookeeper.znode.parent", "/hbase")
		// 对HBase进行读写，建议关闭Hadoop的Speculative Execution功能
		hbaseConfig.setBoolean("mapred.map.tasks.speculative.execution", false)
		hbaseConfig.setBoolean("mapred.reduce.tasks.speculative.execution", false)
		hbaseConfig.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,120000)
		val connection = ConnectionFactory.createConnection(hbaseConfig)
		val table = connection.getTable(TableName.valueOf(tableName))
		val get = new Get(Bytes.toBytes(rowKey))
		get.addFamily(Bytes.toBytes(cf))
		val scan = new Scan(get)
		val rs = table.getScanner(scan)
		val row = new ConcurrentHashMap[String, String]
		try {
			var r = rs.next
			while ( {
				r != null
			}) {
				val cells = r.rawCells

				for (cell <- cells) {
					row.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell), "UTF-8"))
				}

				r = rs.next
			}
		} finally rs.close()
		row

	}
	///准备尝试一个根据rowkey的列表进行读取的方法
	def getValuesByRowKey2(tableName:String,
												cf:String,
												rowKey:String
											 ):ConcurrentHashMap[String,String]= {

		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		hbaseConfig.set("zookeeper.znode.parent", "/hbase")
		// 对HBase进行读写，建议关闭Hadoop的Speculative Execution功能
		hbaseConfig.setBoolean("mapred.map.tasks.speculative.execution", false)
		hbaseConfig.setBoolean("mapred.reduce.tasks.speculative.execution", false)
		hbaseConfig.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,120000)
		val connection = ConnectionFactory.createConnection(hbaseConfig)
		val table = connection.getTable(TableName.valueOf(tableName))
		val get = new Get(Bytes.toBytes(rowKey))
		get.addFamily(Bytes.toBytes(cf))
		val scan = new Scan(get)
		val rs = table.getScanner(scan)
		val row = new ConcurrentHashMap[String, String]
		try {
			var r = rs.next
			while ( {
				r != null
			}) {
				val cells = r.rawCells

				for (cell <- cells) {
					row.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell), "UTF-8"))
				}

				r = rs.next
			}
		} finally rs.close()
		row

	}



}
object  HBaseUtil3 extends Serializable {
	def apply():HBaseUtil3 = new HBaseUtil3(){

	}
}