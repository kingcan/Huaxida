package com.sdmc.www
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
object Rank {

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
    val modelUtil = ModelUtil(spark)
    val training = modelUtil.getFeature
    //当前获取特征时数据正确，label也正确
    val lr = LRModel()
    //getModel方法中融合了特征，
    val lrModel = lr.getModel(training,spark)
    val i2 = lr.getRank(lrModel,spark)
    println("-------打印i2(逻辑回归后的结果)------------")
    i2.show()
    import spark.implicits._
    //val i3 = i2.select("uid","itemid","features","rawPrediction","probability","prediction")
    val i3 = i2.select("uid","itemid","probability","prediction").rdd.map(row => {
      val uid =row.getAs[String]("uid")
      val itemid =row.getAs[Integer]("itemid")
      val probability = row.getAs[DenseVector]("probability")
      val noclick = probability(0)
      val prediction =row.getAs[Double]("prediction")
      (uid,itemid,noclick,prediction)
    }).toDF("uid","itemid","noclick","prediction")
    println("-------打印------------")
    i3.show()
//    val i4 = i3.withColumn("splitcol",split($"probability", ",")).select(
//      col("*"),
//        col("splitcol").getItem(0).as("noclick")
//        //col("splitcol").getItem(1).as("yesclick")
//      ).select("*")
//    println("-------打印i4------------")
//    i4.show()
    //val i5 = i4.withColumn("noclick",col("noclick").select("*")
//    val i4 =i3.rdd.map(x => (x.getAs[Int]("uid"),(x.getAs[Int]("itemid"), x.getAs[Double]("noclick"))))
//      .groupByKey()
//    val rdd3 = i4.map( x => {
//      val rdd2 = x._2.toBuffer
//      val rdd2_2 = rdd2.sortBy(_._2)
//      if (rdd2_2.length > 3) rdd2_2.remove(0, rdd2_2.length - 3)
//      (x._1, rdd2_2.toIterable)
//    })
//    val rdd4 = rdd3.flatMap(x => {
//      val y = x._2
//      for (w <- y) yield (x._1, w._1, w._2)
//    }).toDF("uid", "itemid", "noclickprobability")
    val i4 = i3.withColumn("rank", rank().over(Window.partitionBy("uid").orderBy("noclick"))).select("*")
    println("----------打印排完序后的dataframe------------")
    i4.show()
    //val rdd2 = i4.map(x => (x.getAs("uid"),(x.getAs("itemid"), x.getAs("noclick")))).groupByKey()
    //val i5 = i4.select("uid","itemid")
   // i2.withColumn()withColumn("itemid", functions.explode(functions.col("itemlist")))
    //val i3 = i2.orderBy($"probability".toString().endsWith(",").toString.toDouble)
    modelUtil.saveRecall2(i4,"after_lr")
    println("任务完成了")
//    val lrModel = lr.fit(training)
//    val i2 = lrModel.transform(test)
//    i2.show(false)

//    val evaluator = new BinaryClassificationEvaluator()
//    evaluator.setMetricName("areaUnderROC")
//    val auc= evaluator.evaluate(i2)

//    val evaluator2 = new MulticlassClassificationEvaluator()
//      .setLabelCol("label")
//      .setPredictionCol("prediction")
//      .setMetricName("weightedPrecision")

//    val predictionAccuracy = evaluator2.evaluate(i2)
//    println("Testing Error = " + (predictionAccuracy))
//    println(auc)





    spark.stop()

    System.out.println(Base64.base64Decode("cm93LTA="))
    System.out.println(Base64.base64Decode("Y2Y6Y3Ew"))

  }
  case class ClickUser(uid:Int,itemid:Int,probability:Array[Double],prediction:Float){

  }

}
