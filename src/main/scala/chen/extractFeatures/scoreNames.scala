package extractFeatures

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by chen on 12/13/16.
  */
object scoreNames {
  def trainNgramModel(spark: SparkSession): DataFrame = {
    import spark.implicits._
    // Load training data(common names) from dns.names.
    val namesDF = spark.sql("select * from dns.names limit 100000")
      .rdd
      .map(row => row.getString(0).toLowerCase.split(""))
      .toDF("names")
    // Create biGram model from names.
    val nGram = new NGram()
      .setN(2)
      .setInputCol("names")
      .setOutputCol("ngrams")
    // Train model and count biGrams.
    val nGramFrame = nGram.transform(namesDF)
      .map(_.getAs[Stream[String]]("ngrams")
        .toList.mkString("/"))
      .flatMap(line => line.split("/"))
      .map(word => (word, 1))
      .rdd
      .reduceByKey(_ + _)
    // Get all biGrams count.
    val pairsCount = nGramFrame.count()
    // Calculate "biGram: p"
    val nGramModel = nGramFrame
      .map(pairs_count => (pairs_count._1, pairs_count._2 * 1.0 / pairsCount))
      .toDF("pair", "possibility")
    // Store "biGram: p" to hive.
    nGramModel.createOrReplaceTempView("ngrams_model_tmp")
    spark.sql("select * from ngrams_model_tmp limit 5").show(false)
    spark.sql("drop table if exists ngrams_model")
    spark.sql("create table if not exists dns.ngrams_model as select * from ngrams_model_tmp")
    nGramModel
  }

  /*
  load biGram model from $tableName
   */
  def loadBiGramModel(spark: SparkSession):DataFrame = {
    println("Loading biGram model ...")
    val biGramTblName = "dns.ngrams_model"
    val query = "select * from " + biGramTblName
    val model = spark.sql(query)
    model
  }

  def score(spark: SparkSession, model: DataFrame, tbl_name: String) = {
    import spark.implicits._
    // Reading names (testing data) from dns.log213hk
    val namesDF = spark.sql("select name from dns." + tbl_name)
      .rdd
      .map(row => row.getAs("name").toString.toLowerCase.split(""))
      .toDF("names")
    // create nGram model (biGram)
    val nGram = new NGram()
      .setN(2)
      .setInputCol("names")
      .setOutputCol("ngrams")
    // create a broadcast dict of "bi-gram: p"
    val dict = model.map(row => row.getString(0) -> row.getDouble(1)).collect().toMap
    val bCastDict = spark.sparkContext.broadcast(dict)
    // generate bi-grams
    val biGrams =
      nGram
        .transform(namesDF)
        .cache()
    val aa = biGrams
      .map(row => (row.getAs[Stream[String]]("names").toList.mkString(""),
                   row.getAs[Stream[String]]("ngrams").toList.mkString("/")))
      .map(row => (row._2.split("/").map(r => row._1 + "/" + r)))
      .flatMap(row => row.map(line => line.split("/")))
    // score a name by reduce.
    val score = aa.rdd.map(row => if (row.length != 2)
      (row(0), 0.0)
    else
      (row(0),
      if (bCastDict.value.contains(row(1))) {
        math.log(bCastDict.value(row(1)).toDouble)
      } else
        math.log(0.00000000001)
      ))
      .reduceByKey(_ * _).toDF("name", "score")
      .createOrReplaceTempView("bigram_tmp")
    spark.sql("drop table if exists dns.bigram_" + tbl_name)
    spark.sql("create table dns.bigram_" + tbl_name +" as select * from bigram_tmp")
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
//     val nGramModel = trainNgramModel(spark)
    val nGramModel = loadBiGramModel(spark)
    // given a table with name column
    score(spark, nGramModel, "constructed_data_50")
    spark.stop()
  }
}
