package com.sdmc.www

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ModelUtil(spark:SparkSession) extends Serializable {
	//当次召回推荐表
	val rsRecall = "history_rs_recall"
	val rsRecall2 = "result_rs_recall"
	//cf
	val cf = "recall"
	def getFeature:DataFrame={
		/**
			*
			* 用户特征
			* user_feature
			*
			* 用户行为分值表
			* user_action
			*/

		val actions = spark.sql(
			"select a.member_id as uid,b.id as itemid,judgeSize(a.play_duration,240) as click from " +
				"testonly.tmp_user_action as a join  testonly.bo_video_content as b on a.parent_asset_id=b.asset_id" +
				" where a.member_id!=-1 "
			//"select uid,pv as clicked from user_action、"///这是从用户行为记录标准获取数据
		)
		import spark.implicits._
		val hbaseUtil = HBaseUtil(spark)
		val table = "hbase_user_feature"
		val cf = "profile"
		//val column1 = "area_id"/////这里是一行一行的读取用户和物品的特征矩阵
		val table2 = "hbase_item_feature"
		//val cf = "profile"
		//val column2 = "duration"/////这里是一行一行的读取用户和物品的特征矩阵
		val allUserFeature =hbaseUtil.getUserFeatureData(cf)
		val allItemFeature = hbaseUtil.getItemFeatureData(cf)
		val tmp1 = actions.join(allUserFeature,Seq("uid"))
		val rs = tmp1.join(allItemFeature,Seq("itemid")).toDF("itemid","uid","label","user_feature1","user_feature2","user_feature3","user_feature4","user_feature5",
			"item_feature1","item_feature2","item_feature3","item_feature4","item_feature5","item_feature6",
			"item_feature7","item_feature8","item_feature9","item_feature10","item_feature11","item_feature12",
			"item_feature13")
     rs.show()

//		val rs = actions.rdd.map(row=>{
//			val uid = row.getInt(0)
//			val itemid = row.getInt(1)
//			val click = row.getInt(2)
//			val start = uid.toString
//			val end = uid.toString
//			val start2 = itemid.toString
//			val end2 = itemid.toString
//			val userFeatureTmp = hBaseUtil3.getValuesByRowKey(table, cf, start)
//			//println(userFeatureTmp.get("ageRegistered"))
//			val itemFeatureTmp = hBaseUtil3.getValuesByRowKey(table2, cf, start2)
//			//      val userFeatureTmp = hbaseUtil.scanData2(table,cf,column1,start,end)////第一版为了节省时间我们没做
//			//      val itemFeatureTmp = hbaseUtil.scanData2(table2,cf,column2,start2,end2)////这句话肯定要改成用户物品矩阵
//			//      val userFeature2 = Bytes.toString(userFeatureTmp(0))
//			//      val itemFeature2 = Bytes.toString(itemFeatureTmp(0))
//			//ageRegistered=92, sex=0, area_id=26, type=1, age=-1
//			//{star=3.5, area_tag_id=11, type=1, duration=5481, category_id=75, online_year_tag_id=109, grade=6,
//			// actor_id=4046, tags_id=3135, hit_count=0, writer_id=4051, director_id=4051, episode_total=1}
//
//			val durationRegistered =userFeatureTmp.get("ageRegistered").toInt
//			val sex =userFeatureTmp.get("sex").toInt
//			val area_id =userFeatureTmp.get("area_id").toInt
//			val type2 =userFeatureTmp.get("type").toInt
//			val age =userFeatureTmp.get("age").toInt
//			val star =itemFeatureTmp.get("star").toFloat
//			val area_tag_id =itemFeatureTmp.get("area_tag_id").toInt
//			val type3 =itemFeatureTmp.get("type").toInt
//			val duration =itemFeatureTmp.get("duration").toInt
//			val category_id =itemFeatureTmp.get("category_id").toInt
//			val online_year_tag_id =itemFeatureTmp.get("online_year_tag_id").toInt
//			val grade =itemFeatureTmp.get("grade").toInt
//			val actor_id =itemFeatureTmp.get("actor_id").toInt
//			val tags_id =itemFeatureTmp.get("tags_id").toInt
//			val hit_count =itemFeatureTmp.get("hit_count").toInt
//			val writer_id =itemFeatureTmp.get("writer_id").toInt
//			val director_id =itemFeatureTmp.get("director_id").toInt
//			val episode_total =itemFeatureTmp.get("episode_total").toInt
//			(click,durationRegistered,sex, area_id, type2, age,
//				star, area_tag_id, type3, duration, category_id, online_year_tag_id, grade,
//				actor_id, tags_id, hit_count, writer_id, director_id, episode_total)
//			//(click,userFeature)
//		}).toDF("label","user_feature1","user_feature2","user_feature3","user_feature4","user_feature5",
//			"item_feature1","item_feature2","item_feature3","item_feature4","item_feature5","item_feature6",
//			"item_feature7","item_feature8","item_feature9","item_feature10","item_feature11","item_feature12",
//			"item_feature13")
		rs
	}
	//把召回策略生成的推荐候选集存储到HBase
	def saveRecall(recommend:DataFrame,cell:String):Unit={

		/**
			* 目标生成格式
			*  uid     itemid
			*  3       [12,34,24,89,21]
			*  5       [19,78,67,21,12]
			*/
		val recommList = recommend.groupBy(col("uid"))
			.agg(collect_list("itemid"))
			.withColumnRenamed("collect_list(itemid)",
				"itemid")
			.select(col("uid"),
				col("itemid"))

		val HBase = new HBaseUtil(spark)
		HBase.putData(rsRecall,recommList,cf,cell)
	}
	//把排序策略生成的推荐候选集存储到HBase
	def saveRecall2(recommend:DataFrame,cell:String):Unit={

		/**
			* 目标生成格式
			*  uid     itemid
			*  3       [12,34,24,89,21]
			*  5       [19,78,67,21,12]
			*/
		val recommList = recommend.groupBy(col("uid"))
			.agg(collect_list("itemid"))
			.withColumnRenamed("collect_list(itemid)",
				"itemid")
			.select(col("uid"),
				col("itemid"))

		val HBase = new HBaseUtil(spark)
		HBase.putData3(rsRecall2,recommList,cf,cell)
	}
}

object ModelUtil {
	def apply(spark:SparkSession): ModelUtil
	= new ModelUtil(spark)
}

