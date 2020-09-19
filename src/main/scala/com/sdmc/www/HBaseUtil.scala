package com.sdmc.www

import java.security.MessageDigest

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class HBaseUtil(spark:SparkSession) extends Serializable{

	/**
		* Spark 读写 HBase 有4种方式
		*  1. Java Api
		*  2. saveAsNewAPIHadoopDataset 使用Job
		*  3  saveAsHadoopDataset    使用JobConf
		*  4. BulkLoad (这里是使用开源插件)
		*
		*  在视频里演示的是第3种(旧API)方式
		*  在注释里会演示第2种(新API)方式
		*
		*  但效率最高的应该是第4种
		*
		*  这个类应该独立出来，因为在其他项目都会用到这个公共类
		*  这里并没有封装的很严谨
		*
		*/



	//val sc = new JavaSparkContext(spark.sparkContext)
	//val sc = new SparkContext(spark.sparkContext)
	//  private  val hbaseConfig = HBaseConfiguration.create()
	//  //hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
	//  hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
	//  hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
	 @transient val sc = spark.sparkContext

	//读取数据
	def getData(tableName:String,
							cf:String,
							column:String):DataFrame={
		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		//val sc = spark.sparkContext
		hbaseConfig.set(TableInputFormat.INPUT_TABLE,
			tableName)
		val hbaseRDD:RDD[(ImmutableBytesWritable,Result)]
		= sc.newAPIHadoopRDD(hbaseConfig,
			classOf[TableInputFormat],
			classOf[ImmutableBytesWritable],
			classOf[Result])

		import spark.implicits._
		val rs = hbaseRDD.map(_._2)
			.map(r=>{
				r.getValue(
					Bytes.toBytes(cf),
					Bytes.toBytes(column)
				)
			})
			.toDF("value")

		rs

	}
	//读取数据(把wapperedarray转成了array)
	def getData2(tableName:String,
							cf:String,
							column:String):DataFrame={
		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		hbaseConfig.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,120000)
		//val sc = spark.sparkContext
		hbaseConfig.set(TableInputFormat.INPUT_TABLE,
			tableName)
		val hbaseRDD:RDD[(ImmutableBytesWritable,Result)]
		= sc.newAPIHadoopRDD(hbaseConfig,
			classOf[TableInputFormat],
			classOf[ImmutableBytesWritable],
			classOf[Result])
		import spark.implicits._
		val rs = hbaseRDD.map(row =>{
			//这个rowkey是召回模块中putData进来的，你去看看它的rowkey是啥格式
			val rs1 = Bytes.toString(row._1.get())
			//val rs2 = row._2.get()
			val rs2 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf), Bytes.toBytes(column)))
				.substring(12)
			val rs3 = rs2.substring(1,rs2.length-1).trim.split(",").map(_.trim.toInt)
			//val rs3  = rs2.map(_.toInt)
			// println(Bytes.toString(rs2))
//			val rs3  = rs2..map(_.toInt)
			//val rs3 = Arrays.stream(Bytes.toString(rs2).split(",")).mapToInt(row=>Integer.parseInt(row)).toArray //转int数组

			(rs1,rs3)
		}).toDF("uid","itemlist")
		rs

	}

	//读取数据(把userfeature读取到的表转成dataframe)
	def getUserFeatureData(cf:String
							 ):DataFrame={
		val tableName = "hbase_user_feature"
		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		hbaseConfig.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,120000)
		//val sc = spark.sparkContext
		hbaseConfig.set(TableInputFormat.INPUT_TABLE,
			tableName)
		val hbaseRDD:RDD[(ImmutableBytesWritable,Result)]
		= sc.newAPIHadoopRDD(hbaseConfig,
			classOf[TableInputFormat],
			classOf[ImmutableBytesWritable],
			classOf[Result])
		import spark.implicits._
		val rs = hbaseRDD.map(row =>{
			val rs1 = Bytes.toString(row._1.get())
			//val rs2 = row._2.get()
			val rs2 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("ageRegistered"))).toInt
			val rs3 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("sex"))).toInt
			val rs4 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("area_id"))).toInt
			val rs5 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("type"))).toInt
			val rs6 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("age"))).toInt
			(rs1,rs2,rs3,rs4,rs5,rs6)
		}).toDF("uid","ageRegistered","sex","area_id","type","age")
		rs
	}

	//读取数据(把userfeature读取到的表转成dataframe)
	def getItemFeatureData(cf:String
												):DataFrame={
		val tableName = "hbase_item_feature"
		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		hbaseConfig.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,120000)
		//val sc = spark.sparkContext
		hbaseConfig.set(TableInputFormat.INPUT_TABLE,
			tableName)
		val hbaseRDD:RDD[(ImmutableBytesWritable,Result)]
		= sc.newAPIHadoopRDD(hbaseConfig,
			classOf[TableInputFormat],
			classOf[ImmutableBytesWritable],
			classOf[Result])
		import spark.implicits._
		val rs = hbaseRDD.map(row =>{
			val rs1 = Bytes.toString(row._1.get())
			//val rs2 = row._2.get()
			val rs2 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("star"))).toFloat
			val rs3 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("area_tag_id"))).toInt
			val rs4 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("type"))).toInt
			val rs5 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("duration"))).toInt
			val rs6 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("category_id"))).toInt
			val rs7 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("online_year_tag_id"))).toInt
			val rs8 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("grade"))).toInt
			val rs9 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("actor_id"))).toInt
			val rs10 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("tags_id"))).toInt
			val rs11 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("hit_count"))).toInt
			val rs12 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("writer_id"))).toInt
			val rs13 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("director_id"))).toInt
			val rs14 = Bytes.toString(row._2.getValue(Bytes.toBytes(cf),Bytes.toBytes("episode_total"))).toInt
			(rs1,rs2,rs3,rs4,rs5,rs6,rs7,rs8,rs9,rs10,rs11,rs12,rs13,rs14)
		}).toDF("itemid","star","area_tag_id","type","duration","category_id","online_year_tag_id",
			"grade","actor_id","tags_id","hit_count","writer_id","director_id","episode_total")
		rs
	}

	//写入数据
	def putData(tableName:String,
							data:DataFrame,
							cf:String,
							column:String
						 ):Unit={

		//初始化Job,设置输出格式TableOutputFormat，hbase.mapred.jar
		/* @transient val jobConf = new JobConf(hbaseConfig,this.getClass)
	 jobConf.setOutputFormat(classOf[TableOutputFormat])
	 jobConf.set(TableOutputFormat.OUTPUT_TABLE,
		 tableName)*/
		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		//val sc = spark.sparkContext
		// 使用新API
		val jobConf = new JobConf(hbaseConfig, this.getClass)
		jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
		val job =Job.getInstance(jobConf)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
		//job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])




		val _data = data.rdd.map(x=> {
			val uid = x.getInt(0)
			val itemList = x.get(1)
			//在视频里没讲到，应该将rowKey散列
			val rowKey = uid
			val put = new Put(Bytes.toBytes(rowKey))
			put.addColumn(Bytes.toBytes(cf),
				Bytes.toBytes(column),
				Bytes.toBytes(itemList.toString))
			(new ImmutableBytesWritable, put)
		})
		//_data.saveAsHadoopDataset(jobConf)
		_data.saveAsNewAPIHadoopDataset(job.getConfiguration)
		/*
		新API
		_data.saveAsNewAPIHadoopDataset(job.getConfiguration)
	 */
	}
	//写入数据(专门为了把LR的排序结果写入到到一张表里)
	def putData2(tableName:String,
							data:DataFrame,
							cf:String,
							column:String
						 ):Unit={

		//初始化Job,设置输出格式TableOutputFormat，hbase.mapred.jar
		/* @transient val jobConf = new JobConf(hbaseConfig,this.getClass)
	 jobConf.setOutputFormat(classOf[TableOutputFormat])
	 jobConf.set(TableOutputFormat.OUTPUT_TABLE,
		 tableName)*/
		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		//val sc = spark.sparkContext
		// 使用新API
		val jobConf = new JobConf(hbaseConfig, this.getClass)
		jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
		val job =Job.getInstance(jobConf)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
		//job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

		val _data = data.rdd.map(x=> {
			val uid = x.getInt(0)
			val itemid = x.get(1)
			val probability = x.getAs("probability")
			val prediction =  x.getAs("prediction")
			//在视频里没讲到，应该将rowKey散列
			val rowKey = rowKeyHash(uid.toString)
			val put = new Put(Bytes.toBytes(rowKey))
			put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("columnName1"),Bytes.toBytes("columnValue1"))
				put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("columnName2"),Bytes.toBytes("columnValue2"))
				put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("columnName3"),Bytes.toBytes("columnValue3"))
//			put.addColumn(Bytes.toBytes(cf),
//				Bytes.toBytes(column),
//				Bytes.toBytes(itemid.toString))
			(new ImmutableBytesWritable, put)
		})
		//_data.saveAsHadoopDataset(jobConf)
		_data.saveAsNewAPIHadoopDataset(job.getConfiguration)
		/*
		新API
		_data.saveAsNewAPIHadoopDataset(job.getConfiguration)
	 */
	}
	//写入数据
	def putData3(tableName:String,
							data:DataFrame,
							cf:String,
							column:String
						 ):Unit={

		//初始化Job,设置输出格式TableOutputFormat，hbase.mapred.jar
		/* @transient val jobConf = new JobConf(hbaseConfig,this.getClass)
	 jobConf.setOutputFormat(classOf[TableOutputFormat])
	 jobConf.set(TableOutputFormat.OUTPUT_TABLE,
		 tableName)*/
		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		//val sc = spark.sparkContext
		// 使用新API
		val jobConf = new JobConf(hbaseConfig, this.getClass)
		jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
		val job =Job.getInstance(jobConf)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
		//job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
		val _data = data.rdd.map(x=> {
			val uid = x.getAs[String](0)
			val itemList = x.get(1)
			//在视频里没讲到，应该将rowKey散列
			val rowKey = uid
			val put = new Put(Bytes.toBytes(rowKey))
			put.addColumn(Bytes.toBytes(cf),
				Bytes.toBytes(column),
				Bytes.toBytes(itemList.toString))
			(new ImmutableBytesWritable, put)
		})
		//_data.saveAsHadoopDataset(jobConf)
		_data.saveAsNewAPIHadoopDataset(job.getConfiguration)
		/*
		新API
		_data.saveAsNewAPIHadoopDataset(job.getConfiguration)
	 */
	}

	//读取特定数据
	def scanData(tableName:String,
							 cf:String,
							 column:String,
							 start:String,
							 end:String):Unit={
		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		//val sc = spark.sparkContext
		hbaseConfig.set(TableInputFormat.INPUT_TABLE,
			tableName)
		val scan = new Scan(Bytes.toBytes(start),
			Bytes.toBytes(end))
		scan.addFamily(Bytes.toBytes(cf))
		scan.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column))
		val scanStr = TableMapReduceUtil.convertScanToString(scan)
		hbaseConfig.set(TableInputFormat.SCAN,scanStr)
		val hbaseRDD:RDD[(ImmutableBytesWritable,Result)]
		= sc.newAPIHadoopRDD(hbaseConfig,
			classOf[TableInputFormat],
			classOf[ImmutableBytesWritable],
			classOf[Result])

		val rs = hbaseRDD.map(_._2)
			.map(r=>{
				(r.getValue(
					Bytes.toBytes(cf),
					Bytes.toBytes(column)
				))
			})
			.collect()
	}

	//读取特定数据(有返回值的)
	def scanData2(tableName:String,
								cf:String,
								column:String,
								start:String,
								end:String):Array[Array[Byte]]={
		val hbaseConfig = HBaseConfiguration.create()
		//hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
		hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
		// val sc = spark.sparkContext
		hbaseConfig.set(TableInputFormat.INPUT_TABLE,
			tableName)
		val scan = new Scan(Bytes.toBytes(start),
			Bytes.toBytes(end))
		scan.addFamily(Bytes.toBytes(cf))
		scan.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column))
		val scanStr = TableMapReduceUtil.convertScanToString(scan)
		hbaseConfig.set(TableInputFormat.SCAN,scanStr)
		val hbaseRDD:RDD[(ImmutableBytesWritable,Result)]
		= sc.newAPIHadoopRDD(hbaseConfig,
			classOf[TableInputFormat],
			classOf[ImmutableBytesWritable],
			classOf[Result])

		val rs = hbaseRDD.map(_._2)
			.map(r=>{
				(r.getValue(
					Bytes.toBytes(cf),
					Bytes.toBytes(column)
				))
			})
			.collect()
		rs
	}


	//rowKey散列
	def rowKeyHash(key:String):String={
		var md5:MessageDigest = null
		try {
			md5 = MessageDigest.getInstance("MD5")
		}catch {
			case e:Exception=>{
				e.printStackTrace()
			}
		}
		//rowKey的组成：时间戳+uid
		val str = System.currentTimeMillis() + ":" + key
		val encode = md5.digest(str.getBytes())
		encode.map("%02x".format(_)).mkString
	}


}

object HBaseUtil{
	def apply(spark:SparkSession): HBaseUtil = new HBaseUtil(spark){

	}
}

