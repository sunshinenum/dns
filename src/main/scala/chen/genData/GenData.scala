package chen.genData

import java.io.FileWriter
import java.util.Date
import main.Tools.RandomString


import scala.util.Random
import com.google.common.net.InetAddresses

import scala.io.Source

/**
  * Created by chen on 3/29/17.
  */
object GenData {
  // Array(AMPrate, RandomRate)
  val attackRate = Array(0.5, 0.5)
  val namesAttacked = Array("atk1", "atk2", "atk3")
  val targetIps = Array("114.213.250.84", "114.213.250.85", "114.213.250.86")
  val cnSLDs = ("ac\nah\nbj\ncom\ncq\nedu\nfj\ngd\ngov\ngs\ngx\ngz\n" +
    "ha\nhb\nhe\nhi\nhk\nhl\nhn\njl\njs\njx\nln\nmil\nmo\nnet\nnm\n" +
    "nx\norg\nqh\nsc\nsd\nsh\nsn\nsx\ntj\ntw\nxj\nxz\nyn\nzj\n")
    .split("\n")

  /*
  * Generates Water Torture DDoS query.
   */
  def genRandomQuery: String = {
    val id = new Date().getTime + Random.nextInt(1000)
    val min = new Date().getTime / 1000 / 60
    val sec = new Date().getTime / 1000
    val rndName = RandomString.randomName
    val tld = "cn"
    val sld = namesAttacked(Random.nextInt(3))
    val name = rndName + sld + "." + tld
    val ip = InetAddresses.fromInteger(Random.nextInt).getHostAddress
    val thirdld = if(rndName.split("\\.").length >= 3){
      rndName.reverse.split("\\.")(2).reverse
    }else{""}
    val query = s"$id,$min,$sec,$name,$thirdld,$sld,$tld,$ip,1.0"
    query
  }

  def genAMPQuery(commonNames: Array[String]): String = {
    val id = new Date().getTime + Random.nextInt(1000)
    val min = new Date().getTime / 1000 / 60
    val sec = new Date().getTime / 1000
    val rndName = commonNames(Random.nextInt(commonNames.length))
    var tld = "cn"
    var sld = rndName
    if (rndName.split("\\.").length <= 2) {
    } else {
      sld = rndName.reverse.split("\\.")(1).reverse
      if (cnSLDs.contains(rndName.reverse.split("\\.")(1).reverse)) {
        if (rndName.split("\\.").length >= 3) {
          sld = rndName.reverse.split("\\.")(2).reverse
          tld = sld + "." + tld
        }
      }
    }
    val name = rndName
    val ip = targetIps(Random.nextInt(3))
    val thirdld = if(rndName.split("\\.").length >= 3){
      rndName.reverse.split("\\.")(2).reverse
    }else{""}
    val query = s"$id,$min,$sec,$name,$thirdld,$sld,$tld,$ip,1.0"
    query
  }

  def genCommonQuery(commonNames: Array[String]): String = {
    val id = new Date().getTime + Random.nextInt(1000)
    val min = new Date().getTime / 1000 / 60
    val sec = new Date().getTime / 1000
    val rndName = commonNames(Random.nextInt(commonNames.length))
    var tld = "cn"
    var sld = rndName
    if (rndName.split("\\.").length <= 2) {
    } else {
      sld = rndName.reverse.split("\\.")(1).reverse
      if (cnSLDs.contains(rndName.reverse.split("\\.")(1).reverse)) {
        if (rndName.split("\\.").length >= 3) {
          sld = rndName.reverse.split("\\.")(2).reverse
          tld = sld + "." + tld
        }
      }
    }
    val name = rndName
    val ip = InetAddresses.fromInteger(Random.nextInt).getHostAddress
    val thirdld = if(rndName.split("\\.").length >= 3){
      rndName.reverse.split("\\.")(2).reverse
    }else{""}
    val label = if (sld == "liufc" || sld == "lxdtcjx"){
      "1.0"
    }else{
      "0.0"
    }
    val query = s"$id,$min,$sec,$name,$thirdld,$sld,$tld,$ip,$label"
    query
  }

  def getCommonNames: Array[String] = {
    val commonNamePath = "/media/ybc/S/MachineLearning/data/dns/common_names"
    Source.fromFile(commonNamePath).getLines().toArray
  }

  /*
   * Generates ${timeDelay} minutes queries to ${path}.
   */
  def genQueries(path: String, timeDelay: Int): Unit = {
    var fOut = new FileWriter(path)
    val commonNames = getCommonNames
    var minStop = new Date().getTime / 1000 / 60 + timeDelay
    while (new Date().getTime / 1000 / 60 < minStop) {
      if (Random.nextInt(100) < 100){
        Thread.sleep(1)
      }
      if (Random.nextInt(100) < attackRate(0) * 100) {
        fOut.write(genAMPQuery(commonNames)+"\n")
      }
      if (Random.nextInt(100) < (100 - attackRate(0)*100 + attackRate(1)*100)){
        fOut.write(genCommonQuery(commonNames)+"\n")
      }
      if (Random.nextInt(100) < attackRate(1) * 100) {
        fOut.write(genRandomQuery+"\n")
      }
      fOut.flush()
    }
    fOut.close()
  }

  def main(args: Array[String]) {
    genQueries(s"/media/ybc/S/MachineLearning/data/dns/queriesGenerated", 60)
  }
}
