package chen.trainTest

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Created by chen on 12/29/16.
  */
object performanceTestKafka {
  val cnSLDs = ("ac\nah\nbj\ncom\ncq\nedu\nfj\ngd\ngov\ngs\ngx\ngz\n" +
    "ha\nhb\nhe\nhi\nhk\nhl\nhn\njl\njs\njx\nln\nmil\nmo\nnet\nnm\n" +
    "nx\norg\nqh\nsc\nsd\nsh\nsn\nsx\ntj\ntw\nxj\nxz\nyn\nzj\n")
    .split("\n")

  def convertTimeMSec(time: String): Double = {
    val timeList = time.split(":")
    timeList(0)
      .toDouble * scala.math.pow(60, 3) +
      timeList(1).toDouble * scala.math.pow(60, 2) +
      timeList(2).split("\\.")(0).toDouble * 60 +
      timeList(2).split("\\.")(1).toDouble
  }

  def convertTimeSec(time: String): Double = {
    val timeList = time.split(":")
    timeList(0)
      .toDouble * scala.math.pow(60, 2) +
      timeList(1).toDouble * scala.math.pow(60, 1) +
      timeList(2).split("\\.")(0).toDouble
  }

  def convertTimeMin(time: String): Double = {
    val timeList = time.split(":")
    timeList(0)
      .toDouble * scala.math.pow(60, 1) +
      timeList(1).toDouble
  }

  def catFirstSecondItem(subNames: Array[String]): Array[String] = {
    Array(subNames(0) + "." + subNames(1)) ++ subNames.drop(2)
  }

  def getNlSubDomain(name: String, level: Int): String = {
    /*
    Get n level domain of a name.
     */
    val subNamesArray = name.toLowerCase.split("\\.").reverse
    if (subNamesArray.length == 1) {
      return if (level == 0) {
        subNamesArray(0)
      }
      else {
        ""
      }
    }
    if(subNamesArray.length == 0){
      return ""
    }
    val nameLength =
      if (cnSLDs.contains(subNamesArray(1))) {
        subNamesArray.length - 1
      } else {
        subNamesArray.length
      }
    val subNames =
      if (cnSLDs.contains(subNamesArray(1))) {
        catFirstSecondItem(subNamesArray)
      } else {
        subNamesArray
      }
    if (nameLength > level) {
      subNames(level)
    }
    else {
      ""
    }
  }

  def handleRDD(spark: SparkSession, queries: RDD[String]) ={
    import spark.implicits._
    // Collects queries during one minute to a DataFrame.
    System.out.println(queries.count())

    val queriesDF = queries.map(query => query.substring(56, query.lastIndexOf("\"")))
      // .foreach(println)
      .map(line => line.split(" "))
      .filter(line => line.length == 13)
// 12-Mar-2015 15:04:05.175 queries: info: client 202.159.32.93#39185 (npybsvubfpvfvt.com.cn): query: npybsvubfpvfvt.com.cn IN A - (203.119.27.1)

      .map(
        line =>
          (line(0), // date
            line(1), // time
            convertTimeMSec(line(1)), // msec double
            convertTimeSec(line(1)), // sec double
            convertTimeMin(line(1)), // min double
            line(5).split("#")(0), // src_ip
            line(5).split("#")(1).toInt, // src_port
            line(6).substring(1, line(4).length - 2).toLowerCase, // infer_name
            line(8).toLowerCase(), // name
            getNlSubDomain(line(8), 0), //tld
            getNlSubDomain(line(8), 1), //sld
            getNlSubDomain(line(8), 2), //3ld
            getNlSubDomain(line(8), 3), //4ld
            getNlSubDomain(line(8), 4), //5ld
            line(9), // network type
            line(10), // query type
            line(11), // param
            line(12).substring(1, line(12).length - 1).toLowerCase, // ns_ip
            line(1)+line(8)))
      .toDF(
      "date",
      "time",
      "msec",
      "sec",
      "min",
      "src_ip",
      "src_port",
      "infer_name",
      "name",
      "tld",
      "sld",
      "3ld",
      "4ld",
      "5ld",
      "ntype",
      "qtype",
      "param",
      "ns_ip",
        "id")
    // Creates a temp view for queries in one minute.
    queriesDF.createOrReplaceTempView("queries_tmp")
    spark.sql("drop table if exists queries")
    spark.sql("create table queries as select * from queries_tmp")
    // Extract features from queries with sql.
    val featureRows = spark.sql(
      """
        |select main.id, main.sld, main.name, main.src_ip, gpm.gpm, nsp.nsp, ipsp.ipsp, len_lvl.name_len,
        |is_ns, len_lvl.name_lvl,
        |nvl(score, 0) as score,
        |-- in-addr
        |case when main.sld = "in-addr"
        |     then 0.0
        |     else 1.0
        |end as is_arpa,
        |
        |-- label
        |case when find_in_set(main.sld, "lundaddc,hsexpress,zszhanyi,job573,teny") = 0
        |     then 0.0
        |     else 1.0
        |end as label
        |
        |from queries main
        |
        |left outer join
        |-- name space during a period of time of a sld
        |(select sld,  ln(count(name)) as nsp from queries group by sld) nsp
        |on main.sld = nsp.sld
        |
        |-- qpm during a period of time of a sld
        |left outer join
        |(select sld,  ln(count(id)) as gpm from queries group by sld) gpm
        |on main.sld = gpm.sld
        |
        |-- src ip space during a period of time of a sld
        |left outer join
        |(select sld,  ln(count(src_ip)) as ipsp from queries group by sld) ipsp
        |on main.sld = ipsp.sld
        |
        |-- name length, level
        |left outer join
        |(select id, name,  length(name) as name_len, size(split(name, "\\.")) as name_lvl
        |from queries) len_lvl
        |on main.id = len_lvl.id
        |
        |-- is ns?
        |left outer join
        |(select id, name,
        |case when find_in_set(3ld, "ns1,ns2,ns3,ns4,ns5,ns6,ns7,ns8,ns9,ns10") = 0
        |     then 1.0
        |else 0.0
        |end as is_ns
        |from queries) is_ns
        |on main.id = is_ns.id
        |
        |-- bigram score
        |left outer join
        |(select name, score from dns.bigram_log312hk_one_million) bigram
        |on main.name = bigram.name
      """.stripMargin)
    // Loads trained model and makes labels.
    val assembler = new VectorAssembler()
      .setInputCols(Array("gpm", "nsp", "ipsp", "name_len", "name_lvl", "is_arpa", "is_ns", "score"))
      .setOutputCol("features")
    val features = assembler.transform(featureRows)
    val model = PipelineModel.load("log312hk_sample_decision_tree.model")
    val predictions = model.transform(features)
    // Shows samples.
    predictions.select("name","src_ip", "prediction", "features")
      .show(100, false)
  }

  def main(args: Array[String]) {
    // Turns off logging.
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("ImportNames")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true")
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      .config("javax.jdo.option.ConnectionUserName", "root")
      .config("javax.jdo.option.ConnectionPassword", "1993")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(60))
    // Defines the parameters for kafka server.
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.map(record => record.value).foreachRDD(
      r => handleRDD(spark, r)
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
