package chen.trainTest

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * Created by chen on 1/10/17.
  */
object performanceTest {
  val treeNumber = 20
  val tblNameTrain = "dns.features_sample_2000000"
  val featureCols = Array("gpm", "nsp", "ipsp", "name_len", "name_lvl",
    "is_arpa", "is_ns", "score")

  def performanceTest(spark: SparkSession): Unit ={
    val data = spark.sql("select * from " + tblNameTrain + " limit 71000")
    // load model and predict
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val features = assembler.transform(data)
    val model = PipelineModel.load("log312hk_sample_decision_tree.model")
    val predictions = model.transform(features)
    predictions.select("name", "prediction", "features")
      .show(10, false)
  }

  def main(args: Array[String]) {
//    Logger.getLogger(classOf[RackResolver]).getLevel
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("ImportNames")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true")
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      .config("javax.jdo.option.ConnectionUserName", "root")
      .config("javax.jdo.option.ConnectionPassword", "1993")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    performanceTest(spark)
    spark.stop()
  }
}
