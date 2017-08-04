package chen.preprocessing

import org.apache.spark.sql.SparkSession

/**
  * Created by chen on 11/18/16.
  */

object ImportLogs {
  val dataPath = "/media/ybc/S/MachineLearning/data/dns/"
  val log312HKPath = dataPath + "query_log.0_hongkong_3_2015031217"
  val log317CNNICPath = dataPath + "query_log.0_cnnic_6_2015031717"
  val log512GZTelPath = dataPath + "query_log.0_gztel_3_2015051217"
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
      if (cnSLDs.exists(x => x == subNamesArray(1))) {
        subNamesArray.length - 1
      } else {
        subNamesArray.length
      }
    val subNames =
      if (cnSLDs.exists(x => x == subNamesArray(1))) {
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

  def importData(spark: SparkSession, dataPath: String, tblName: String): Unit = {
    import spark.implicits._
    val dataRows =
      spark.sparkContext.textFile(dataPath)
        .map(_.split(" "))
        .map(
          line =>
            (line(0), // date
              line(1), // time
              convertTimeMSec(line(1)), // msec double
              convertTimeSec(line(1)), // sec double
              convertTimeMin(line(1)), // min double
              line(5).split("#")(0), // src_ip
              line(5).split("#")(1).toInt, // src_port
              line(6).substring(1, line(6).length - 2).toLowerCase, // infer_name
              line(8).toLowerCase(), // name
              getNlSubDomain(line(8), 0), //tld
              getNlSubDomain(line(8), 1), //sld
              getNlSubDomain(line(8), 2), //3ld
              getNlSubDomain(line(8), 3), //4ld
              getNlSubDomain(line(8), 4), //5ld
              line(9), // network type
              line(10), // query type
              line(11), // param
              line(12).substring(1, line(12).length - 1).toLowerCase)) // ns_ip
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
        "ns_ip")
    dataRows.createOrReplaceTempView(s"${tblName}_tmp")
    spark.sql(s"select * from ${tblName}_tmp limit 10").show()
    spark.sql("use dns")
    spark.sql(s"drop table if exists ${tblName}")
    spark.sql(s"create table ${tblName} as select * from ${tblName}_tmp")
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("import test data")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/hive?createDatabaseIfNotExist=true")
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      .config("javax.jdo.option.ConnectionUserName", "hive")
      .config("javax.jdo.option.ConnectionPassword", "1993")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
//     importData(spark, log312HKPath, "log312hk")
//     importData(spark, log317CNNICPath, "log317cnnic")
//     importData(spark, log512GZTelPath, "log512gztel")
     spark.sql("use dns")
     spark.sql("show tables").show()
  }
}
