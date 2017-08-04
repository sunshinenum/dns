package chen.preprocessing

import org.apache.spark.sql.SparkSession

/**
  * Created by chen on 12/13/16.
  * Import names to database dns for bi-gram training.
  */
object ImportNames {
  val tblName = "names"
  val pathOfNames = s"/media/ybc/S/MachineLearning/data/dns/${tblName}"
  def importNames(spark: SparkSession): Unit ={
    import spark.implicits._
    val names = spark.sparkContext.textFile(pathOfNames)
                .toDF("name")
    names.createOrReplaceTempView(s"${tblName}_tmp")
    spark.sql("use dns")
    spark.sql(s"drop table if exists ${tblName}")
    spark.sql(s"create table names as select * from ${tblName}_tmp")
  }

  def main(args: Array[String]) {
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
    importNames(spark)
  }
}
