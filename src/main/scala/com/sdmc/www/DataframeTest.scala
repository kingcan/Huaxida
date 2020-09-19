package com.sdmc.www

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, rank}
object DataframeTest {
	def main(args: Array[String]): Unit = {

		val conf = new SparkConf()

		//conf.set("hive.exec.mode.local.auto","true")
		val spark = SparkSession.builder()
			.config(conf)
			.appName("OffLineRank")
			//.master("yarn-cluster")
			.master("local")
			.enableHiveSupport()
			.getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
		val hBaseUtil3 = HBaseUtil3()
		val hbaseUtil = HBaseUtil(spark)
		//当前history_rs_recall中有-1号会员导致bug
		val table = "history_rs_recall"
		val cf = "recall"
		val column = "als"
		val table2 = "hbase_user_feature"
		val cf2 = "profile"
		val table3 = "hbase_item_feature"
		val recall = hbaseUtil.getData2(table,cf,column)
		recall.show()
		val recall2 = recall.withColumn("itemid", functions.explode(functions.col("itemlist"))).select(col("uid"),
			col("itemid"))
		recall2.show()

		val alluser = hbaseUtil.getUserFeatureData(cf2)
		alluser.show()
		val tmp3 =recall2.join(alluser,Seq("uid"))
		tmp3.show()
		val allitem =hbaseUtil.getItemFeatureData(cf2)
		val tmp4 =tmp3.join(allitem,Seq("itemid")).sort("uid")
		tmp4.show()
//		val ALSdata = hbaseUtil.getData2(table,cf,column)
//		ALSdata.show()
//		val tmp2 = ALSdata.join(alluser,Seq("uid"))
//		tmp2.show()
		//val recall2 = recall.withColumn("itemid", functions.explode(functions.col("itemlist"))).select("uid","itemid")
		//val service_result = recall.withColumn("itemlist2", explode($"itemlist"))
      //val  userTest = hBaseUtil3.getValuesByRowKey(table2,cf2,"224255")
//		import spark.implicits._
//					val actions = spark.createDataFrame (Seq (
//					(365817, 7617, 19),
//					(366905, 3551, 22),
//					(223939, 12047, 53),
//					(366905, 9079, 41),
//					(268431, 10389, 50),
//					(223939, 6691, 67)
//					) ) toDF("uid","itemid","click")
//		    actions.show()
//		val rs2 = actions.withColumn("rank", rank().over(Window.partitionBy("uid").orderBy("click"))).select("*")
//
////		val rs2 = actions.rdd.map(row =>{
////			val uid = row.getInt(0)
////
////			val  userMap = hBaseUtil3.getValuesByRowKey(table2,cf2,uid.toString)
////			println(userMap.get("ageRegistered"))
////		(uid)}
////		).toDF()
//
// rs2.show()
   // recall2.show()

	}
}
