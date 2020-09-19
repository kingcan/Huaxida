package com.sdmc.www

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class LRModel extends Serializable {

	def getModel(training:DataFrame,
							 spark:SparkSession):LogisticRegressionModel={

		val vector = new VectorAssembler()
			.setInputCols(Array("user_feature1","user_feature2","user_feature3","user_feature4","user_feature5",
				"item_feature1","item_feature2","item_feature3","item_feature4","item_feature5","item_feature6",
				"item_feature7","item_feature8","item_feature9","item_feature10","item_feature11","item_feature12",
				"item_feature13"))
			.setOutputCol("features")

		val trainFeature = vector.transform(training)

		val lr =  new LogisticRegression()
			.setLabelCol("label")
			.setFeaturesCol("features")
			.setMaxIter(1)

		val lrModel = lr.fit(trainFeature)

		//评估模型
		//        val evaluator = new BinaryClassificationEvaluator()
		//        evaluator.setMetricName("areaUnderROC")
		//        val auc= evaluator.evaluate(predictions)

		//存储lr模型到HDFS
		//lrModel.write.overwrite().save("/models/lr.obj")
		//lrModel.save("/models/lr.obj")

		lrModel

	}

	def getRank(lr:LogisticRegressionModel,
							spark:SparkSession):DataFrame={
		import spark.implicits._
		//val hBaseUtil3 = HBaseUtil3()////普通连接
		val hbaseUtil = HBaseUtil(spark)
		val table = "history_rs_recall"
		val cf = "recall"
		val column = "als"
		val cf2 = "profile"
		val recall = hbaseUtil.getData2(table,cf,column)
		println("-------打印一下读取的召回表------------")
		recall.show()
		val recall2 = recall.withColumn("itemid", functions.explode(functions.col("itemlist"))).select(col("uid"),
			col("itemid"))
		//到这里召回表中结果被拆分成了uid-itemid的dataframe了
		val allUserFeature =hbaseUtil.getUserFeatureData(cf2)
		val allItemFeature = hbaseUtil.getItemFeatureData(cf2)
		val tmp1 = recall2.join(allUserFeature,Seq("uid"))
		val tmp2 = tmp1.join(allItemFeature,Seq("itemid")).toDF("itemid","uid","user_feature1","user_feature2","user_feature3","user_feature4","user_feature5",
						"item_feature1","item_feature2","item_feature3","item_feature4","item_feature5","item_feature6",
						"item_feature7","item_feature8","item_feature9","item_feature10","item_feature11","item_feature12",
						"item_feature13")
		println("-------打印一下联合的带特征的召回表------------")
		tmp2.show()
//		val rs = recall2.rdd.map(row=>{
//			val uid = row.getInt(0)
//			val itemid = row.getInt(1)
//			val start = uid.toString
//			val start2 = itemid.toString
//			val userFeatureTmp = hBaseUtil3.getValuesByRowKey(table2, cf2, start)
//			val itemFeatureTmp = hBaseUtil3.getValuesByRowKey(table3, cf2, start2)
//			val durationRegistered = userFeatureTmp.get("ageRegistered").toInt
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
//			(uid,itemid,durationRegistered,sex, area_id, type2, age,
//				star, area_tag_id, type3, duration, category_id, online_year_tag_id, grade,
//				actor_id, tags_id, hit_count, writer_id, director_id, episode_total)
//			//(click,userFeature)
//		}).toDF("uid","itemid","user_feature1","user_feature2","user_feature3","user_feature4","user_feature5",
//			"item_feature1","item_feature2","item_feature3","item_feature4","item_feature5","item_feature6",
//			"item_feature7","item_feature8","item_feature9","item_feature10","item_feature11","item_feature12",
//			"item_feature13")

		val vector = new VectorAssembler()
			.setInputCols(Array("user_feature1","user_feature2","user_feature3","user_feature4","user_feature5",
				"item_feature1","item_feature2","item_feature3","item_feature4","item_feature5","item_feature6",
				"item_feature7","item_feature8","item_feature9","item_feature10","item_feature11","item_feature12",
				"item_feature13"))
			.setOutputCol("features")
		val testFeature = vector.transform(tmp2)
		val _recall = lr.transform(testFeature)
		_recall
	}


}

object LRModel{
	def apply(): LRModel = new LRModel()
}
