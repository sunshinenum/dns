package chen.extractFeatures

import org.apache.spark.sql.SparkSession

/**
  * Created by chen on 12/25/16.
  */
object extractFeatures {
  def extractFeatures(spark: SparkSession): Unit ={
/*  Features Definition
    - name bi-gram score
    - sub names space at all level during a period of time
    - qpm
    - ip address space at all level during a period of time
    - name level
    - name length */
  }

  def main(args: Array[String]) {
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
    extractFeatures(spark)
    spark.stop()
  }
}
