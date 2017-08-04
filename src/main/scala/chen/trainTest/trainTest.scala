package chen.trainTest

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{SQLTransformer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/**
  * Created by chen on 12/26/16.
  */
object trainTest {
  val treeNumber = 20
  val tblNameTrain = "dns.features_constructed_50"
  val featureCols = Array("gpm", "ipc", "name_len", "name_lvl",
    "is_arpa", "is_ns", "score")

  /** Trains a random forest model and evaluates it.
    *
    * @param tblName a string of features table name.
    * @param featureCols an array of feature columns in feature table.
    * @param spark an instance of [[org.apache.spark.sql.SparkSession]].
    */
  def trainTest(tblName: String, featureCols: Array[String], spark: SparkSession): Unit = {
    val query = "select * from " + tblName
    val featuresRaw = spark.sql(query)
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val features = assembler.transform(featuresRaw)
    val Array(trainingData, testData) = features.randomSplit(Array(0.7, 0.3))
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(treeNumber)
    val pipeline = new Pipeline()
      .setStages(Array(rf))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)
    predictions.createOrReplaceTempView("prediction_result_tmp")
    predictions.show(200, false)
    spark.sql("drop table if exists dns.prediction_result")
    spark.sql("create table dns.prediction_result as select min, id, sld, name, gpm, name_len, name_lvl, is_ns, score, is_arpa, label, prediction from prediction_result_tmp")
    val wrongs = predictions.select("*")
        .where("not label = prediction")
    spark.sql("drop table if exists dns.prediction_wrong")
    wrongs.createOrReplaceTempView("wrongs_tmp")
    spark.sql("create table dns.prediction_wrong as select min, id, sld, name, gpm, name_len, name_lvl, is_ns, score, is_arpa, label, prediction from wrongs_tmp")
    wrongs.show(2000)
    // Evaluates predictions.
    predictions.cache()
    val truePos = predictions.select("name", "prediction", "label", "features")
      .where("label = 1.0 and prediction = 1.0").count()
    val trueNeg = predictions.select("name", "prediction", "label", "features")
      .where("label = 0.0 and prediction = 0.0").count()
    val falsePos = predictions.select("name", "prediction", "label", "features")
      .where("label = 0.0 and prediction = 1.0").count()
    val falseNeg = predictions.select("name", "prediction", "label", "features")
      .where("label = 1.0 and prediction = 0.0").count()
    println(truePos + " " + falseNeg)
    println(falsePos + " " + trueNeg)
    println("FNR = " + falseNeg * 1.0 / (falseNeg + truePos))
    println("FPR = " + falsePos * 1.0 / (trueNeg + falsePos))
    // val Model = model.stages(0).asInstanceOf[DecisionTreeClassificationModel]
    val Model = model.stages(0).asInstanceOf[RandomForestClassificationModel]
    // Prints forest.
    // println("Learned classification tree model:\n" + Model.toDebugString)
    model.write.overwrite().save("log312hk_sample_decision_tree.model")
  }

  def test(tblName: String, spark: SparkSession)={
    val featuresTestRows = spark.sql("select * from " + tblName)
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val testData = assembler.transform(featuresTestRows)
    val model = PipelineModel.load("log312hk_sample_decision_tree.model");
    val predictions = model.transform(testData)
    predictions.createOrReplaceTempView("prediction_result_tmp")
    predictions.show(200, false)
    spark.sql("drop table if exists dns.prediction_result")
    spark.sql("create table dns.prediction_result as select min, id, sld, name, gpm, name_len, name_lvl, is_ns, score, is_arpa, label, prediction from prediction_result_tmp")
    val wrongs = predictions.select("*")
        .where("not label = prediction")
    spark.sql("drop table if exists dns.prediction_wrong")
    wrongs.createOrReplaceTempView("wrongs_tmp")
    spark.sql("create table dns.prediction_wrong as select min, id, sld, name, gpm, name_len, name_lvl, is_ns, score, is_arpa, label, prediction from wrongs_tmp")
    wrongs.show(2000, false)
    // Evaluates predictions.
    predictions.cache()
    val truePos = predictions.select("name", "prediction", "label", "features")
      .where("label = 1.0 and prediction = 1.0").count()
    val trueNeg = predictions.select("name", "prediction", "label", "features")
      .where("label = 0.0 and prediction = 0.0").count()
    val falsePos = predictions.select("name", "prediction", "label", "features")
      .where("label = 0.0 and prediction = 1.0").count()
    val falseNeg = predictions.select("name", "prediction", "label", "features")
      .where("label = 1.0 and prediction = 0.0").count()
    println(truePos + " " + falseNeg)
    println(falsePos + " " + trueNeg)
    println("FN = " + falseNeg * 1.0 / (falseNeg + truePos))
    println("FP = " + falsePos * 1.0 / (trueNeg + falsePos))
  }

/*
  Adopt BP network as classifier.
 */
  def trainTestBP(tblName: String, featureCols: Array[String], spark: SparkSession):Unit = {
    val featuresRaw = spark.sql("select * from " + tblName)
    val Array(trainingData, testData) = featuresRaw.randomSplit(Array(0.7, 0.3))

    val sqlTrans = new SQLTransformer().setStatement(
      "select gpm, nsp, ipsp, name_len, name_lvl, is_arpa, is_ns, " +
        "nvl(log(score), 0) as score, label FROM __THIS__"
    )
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val layers = Array[Int](featureCols.length, featureCols.length * 2 + 3, 2)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(3000)
    val pipeline = new Pipeline().setStages(
      Array(sqlTrans, assembler, trainer)
    )
    val model = pipeline.fit(trainingData)
    val result = model.transform(testData)
    val predictionAndLabels = result.select("prediction", "label", "features")
    result.createOrReplaceTempView("prediction_result_tmp")
    spark.sql("drop table if exists dns.prediction_result")
    spark.sql("create table dns.prediction_result as select min, id, sld, name, gpm, name_len, name_lvl, is_ns, score, is_arpa, label, prediction from prediction_result_tmp")
    val wrongs = result.select("*")
        .where("not label = prediction")
    spark.sql("drop table if exists dns.prediction_wrong")
    wrongs.createOrReplaceTempView("wrongs_tmp")
    spark.sql("create table dns.prediction_wrong as select min, id, sld, name, gpm, name_len, name_lvl, is_ns, score, is_arpa, label, prediction from wrongs_tmp")
    wrongs.show(2000, false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels))
  }

  def main(args: Array[String]) {
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("ImportNames")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/hive?createDatabaseIfNotExist=true")
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      .config("javax.jdo.option.ConnectionUserName", "hive")
      .config("javax.jdo.option.ConnectionPassword", "1993")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    trainTest(tblNameTrain, featureCols, spark)
//    trainTestBP(tblNameTrain, featureCols, spark)
//    test("dns.features_constructed_30", spark)
    spark.stop()
  }
}
