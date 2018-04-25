import java.io.{File, PrintWriter}
import java.util.concurrent.Executors

import caseClass.{ClickFraudLog, ConvFraudLog, ClickFraudIPMsg}
import joptsimple.OptionParser
import org.apache.spark.sql.{Encoders, SparkSession}
import org.json4s._
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object ClickConvFraudLogAnalysis  extends AbstractAnalysis{
  val logger = LoggerFactory.getLogger(this.getClass)
  val parser = new OptionParser
  val clickRadius = parser.accepts("click-radius", "查找作弊点击日志时间半径: click-radius")
    .withRequiredArg
    .describedAs("--click-radius 10")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(10)
  val endDate = parser.accepts("end-date", "the date ended")
    .withRequiredArg
    .describedAs("2017-11-12")
    .ofType(classOf[java.lang.String])
    .defaultsTo("")

  implicit def intToString(x:BigInt) = x.toInt
  implicit val formats = Serialization.formats(NoTypeHints)
  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newWorkStealingPool(20)
    def execute(runnable: Runnable) {
      threadPool.submit(runnable)
    }
    def reportFailure(t: Throwable) {}
  }
//  def syncMap(map: mutable.Map[String,mutable.Map[String,Long]],clkTimeMap:mutable.Map[String,mutable.Set[String]],ClkCountMap:mutable.Map[String,Long]) :Unit = {
//    this.synchronized {
//
//    }
//  }
  def main(args: Array[String]): Unit = {

    val options = parser.parse(args : _*)
    val startTime = System.currentTimeMillis()
    val sparkSession = SparkSession
      .builder()
      .appName("Tornado ClickFraud Log")
      .getOrCreate()
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", PropertyBag.getProperty("aws.access_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", PropertyBag.getProperty("aws.secret_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", PropertyBag.getProperty("aws.s3_endpoint", "s3-ap-southeast-1.amazonaws.com"))
    val convSchema = Encoders.product[ConvFraudLog].schema
    val clickSchema = Encoders.product[ClickFraudLog].schema
    sparkSession.udf.register("getFormatDate",getFormatDate _)
    sparkSession.udf.register("getMod",getMod(_:Int,_:Int))
    sparkSession.udf.register("hasNotChar",hasNotChar(_:String,_:String))

    val convFillMap: Map[String,Any] =Map(
      "clk_timestamp"->0,
      "aff_id"->0,
      "ip" -> "",
      "ua" -> "",
      "gaid" -> "",
      "idfa" -> "",
      "invalid_type" -> "",
      "click_id" -> ""
    )
    val clickFillMap: Map[String,Any] =Map(
      "clk_timestamp"->0,
      "aff_id"->0,
      "ip" -> "",
      "ua" -> "",
      "gaid" -> "",
      "idfa" -> ""
    )

    val ipWriter = new PrintWriter(new File("/mnt/xvdb/tmp/conv_analysis_ip"  + ".csv" ))
    val ipClkTimeWriter = new PrintWriter(new File("/mnt/xvdb/tmp/conv_analysis_ip_clk_time"  + ".csv" ))
    val ipClkCountWriter = new PrintWriter(new File("/mnt/xvdb/tmp/conv_analysis_ip_clk_count"  + ".csv" ))
//    val adidWriter = new PrintWriter(new File("/mnt/xvdb/tmp/conv_analysis_adid"  + ".csv" ))
//    val adidClkTimeWriter = new PrintWriter(new File("/mnt/xvdb/tmp/conv_analysis_adid_clk_time"  + ".csv" ))
//    val affWriter = new PrintWriter(new File("/mnt/xvdb/tmp/conv_analysis_aff"  + ".csv" ))
//    val affClkTimeWriter = new PrintWriter(new File("/mnt/xvdb/tmp/conv_analysis_aff_clk_time"  + ".csv" ))

    val convDataFrame = sparkSession.read.schema(convSchema).json(PropertyBag.getProperty("convfraud_data_from","") +"*").na.fill(convFillMap)
    convDataFrame.createOrReplaceTempView("convfraud_init")
    val convQuery = sparkSession.sql(PropertyBag.getProperty("convfraud_init_query",""))
    convQuery.createOrReplaceTempView("convfraud_summary")

    val ipMap = mutable.Map[String,mutable.Map[String,Long]]()
    val ipClkTimeMap = mutable.Map[String,mutable.Set[String]]()
    val ipClkCountMap = mutable.Map[String,Long]()
//    val adidMap = mutable.Map[String,mutable.Map[String,Long]]()
//    val adidClkTimeMap = mutable.Map[String,mutable.Set[String]]()
//    val affMap = mutable.Map[String,mutable.Map[String,Long]]()
//    val affClkTimeMap = mutable.Map[String,mutable.Set[String]]()

    val ipSummaryQuery = sparkSession.sql(PropertyBag.getProperty("convfraud_ip_query",""))
    ipSummaryQuery.show(false)
    ipSummaryQuery.collect().foreach(ipRow =>  {
      val validMap = ipMap.getOrElse(ipRow.getString(0), mutable.Map[String,Long]())
      var validCount = validMap.getOrElse(ipRow.getString(1),0L)
      if(validCount != 0l && ipRow.getString(1) != ""){
        val clkTimeSet = ipClkTimeMap.getOrElse(ipRow.getString(0),mutable.Set[String]())
        if(!clkTimeSet.contains(ipRow.getInt(3).toString)){
          clkTimeSet.add(ipRow.getInt(3).toString)
          ipClkTimeMap.put(ipRow.getString(0),clkTimeSet)
          if(ipRow.getString(0) != "" && !ipClkCountMap.contains(ipRow.getString(0) + "," + ipRow.getInt(3))){
            try{
              val clickDataFrame = sparkSession.read.schema(clickSchema).json(PropertyBag.getProperty("clickfraud_data_from","").format(getFormatDateToHour(ipRow.getInt(3) - 3600l),getFormatDateToHour(ipRow.getInt(3).toLong),getFormatDateToHour(ipRow.getInt(3) + 3600l)) +"*").na.fill(clickFillMap)
              clickDataFrame.createOrReplaceTempView("clickfraud_init")
              val clkSummaryQuery = sparkSession.sql(PropertyBag.getProperty("clickfraud_init_query","").format(ipRow.getString(0),ipRow.getInt(3) - 60 * options.valueOf(clickRadius).intValue(),ipRow.getInt(3) + 60 * options.valueOf(clickRadius).intValue()))
              clkSummaryQuery.createOrReplaceTempView("clickfraud_summary")
              val clkCountQuery = sparkSession.sql(PropertyBag.getProperty("clickfraud_cnt_query",""))
              clkCountQuery.collect().foreach(clkCountRow => {
                ipClkCountMap.put(ipRow.getString(0) + "," + ipRow.getInt(3),clkCountRow.getLong(0))
                ipClkCountWriter.write(ipRow.getString(0) + "," + ipRow.getInt(3) + "," + clkCountRow.getLong(0) + "\n")
                ipClkCountWriter.flush()
                //              ip,invalid_type,count(*) as ip_count,clk_timestamp
                //              val clickFraudIPMsg = ClickFraudIPMsg(ipRow.getString(0),10,ipRow.getInt(3),clkCountRow.getLong(0),)
              })
            }catch{
              case e: Exception => logger.warn("查询点击日志错误 ", e)
            }

          }
        }
      }
      validCount = validCount + ipRow.getLong(2)
      validMap.put(ipRow.getString(1),validCount)
      ipMap.put(ipRow.getString(0),validMap)
    })
//    val adidSummaryQuery = sparkSession.sql(PropertyBag.getProperty("convfraud_adid_query",""))
//    adidSummaryQuery.show(false)
//    adidSummaryQuery.collect().foreach(row => {
//      val validMap = adidMap.getOrElse(row.getString(0) +","+ row.getString(1), mutable.Map[String,Long]())
//      var validCount = validMap.getOrElse(row.getString(2),0L)
//      if(validCount != 0l && row.getString(2) != ""){
//        val clkTimeSet = adidClkTimeMap.getOrElse(row.getString(0) +","+ row.getString(1),mutable.Set[String]())
//        clkTimeSet.add(row.getInt(5).toString)
//        adidClkTimeMap.put(row.getString(0) +","+ row.getString(1),clkTimeSet)
//      }
//      validCount = validCount + row.getLong(3)
//      validMap.put(row.getString(2),validCount)
//      adidMap.put(row.getString(0) +","+ row.getString(1),validMap)
//    })
//    val affSummaryQuery = sparkSession.sql(PropertyBag.getProperty("convfraud_aff_query",""))
//    affSummaryQuery.show(false)
//    affSummaryQuery.collect().foreach(row => {
//      val validMap = affMap.getOrElse(row.getInt(0) + "," + row.getString(1), mutable.Map[String,Long]())
//      var validCount = validMap.getOrElse(row.getString(2).toString,0L)
//      if(validCount != 0l && row.getString(2) != ""){
//        val clkTimeSet = affClkTimeMap.getOrElse(row.getInt(0) + "," + row.getString(1),mutable.Set[String]())
//        clkTimeSet.add(row.getInt(5).toString)
//        affClkTimeMap.put(row.getInt(0) + "," + row.getString(1),clkTimeSet)
//      }
//      validCount = validCount + row.getLong(3)
//      validMap.put(row.getString(2),validCount)
//      affMap.put(row.getInt(0) + "," + row.getString(1),validMap)
//    })
    ipMap.foreach{ case (ip:String,map:mutable.Map[String,Long]) =>
      var sum = 0l
      var percent = 0f
      var valid = 0l
      var invalidType_1 = 0l
      var invalidType_2 = 0l
      var invalidType_3 = 0l
      var invalidType_4 = 0l
      map.foreach{ case (validType:String,count:Long) =>
        sum = sum + count
      }
      if(sum != map.getOrElse("",0l)){
        ipWriter.write(ip + "," + sum + "," + map.getOrElse("",0l) + "," + map.getOrElse("E001",0l) + "," + map.getOrElse("E002",0l) + "," + map.getOrElse("E003",0l) + "," + map.getOrElse("E004",0l) + "," + "%1.4f".format((map.getOrElse("",0l) * 1.0f)/sum) + "\n")
        ipWriter.flush()
      }
    }
    ipClkTimeMap.foreach{
      case (ip:String, clkTime :mutable.Set[String]) =>
        ipClkTimeWriter.write(ip + "," + clkTime.mkString(",") + "\n")
    }
//    ipClkCountMap.foreach({
//      case (ip:String, cnt:Long) =>
//
//    })
//    adidMap.foreach{ case (adid:String,map:mutable.Map[String,Long]) =>
//      var sum = 0l
//      var percent = 0f
//      var valid = 0l
//      var invalidType_1 = 0l
//      var invalidType_2 = 0l
//      var invalidType_3 = 0l
//      var invalidType_4 = 0l
//      map.foreach{ case (validType:String,count:Long) =>
//        sum = sum + count
//      }
//      if(sum != map.getOrElse("",0l)){
//        adidWriter.write(adid + "," + sum + "," + map.getOrElse("",0l) + "," + map.getOrElse("E001",0l) + "," + map.getOrElse("E002",0l) + "," + map.getOrElse("E003",0l) + "," + map.getOrElse("E004",0l) + "," + "%1.4f".format((map.getOrElse("",0l) * 1.0f)/sum) + "\n")
//        adidWriter.flush()
//      }
//    }
//
//    adidClkTimeMap.foreach{
//      case (adid:String, clkTime :mutable.Set[String]) =>
//        adidClkTimeWriter.write(adid + "," + clkTime.mkString(",") + "\n")
//    }
//
//    affMap.foreach{ case (aff:String,map:mutable.Map[String,Long]) =>
//      var sum = 0l
//      var percent = 0f
//      var valid = 0l
//      var invalidType_1 = 0l
//      var invalidType_2 = 0l
//      var invalidType_3 = 0l
//      var invalidType_4 = 0l
//      map.foreach{ case (validType:String,count:Long) =>
//        sum = sum + count
//      }
//      if(sum != map.getOrElse("",0l)){
//        affWriter.write(aff + "," + sum + "," + map.getOrElse("",0l) + "," + map.getOrElse("E001",0l) + "," + map.getOrElse("E002",0l) + "," + map.getOrElse("E003",0l) + "," + map.getOrElse("E004",0l) + "," + "%1.4f".format((map.getOrElse("",0l) * 1.0f)/sum) + "\n")
//        affWriter.flush()
//      }
//
//    }
//
//    affClkTimeMap.foreach{
//      case (aff:String, clkTime :mutable.Set[String]) =>
//        affClkTimeWriter.write(aff + "," + clkTime.mkString(",") + "\n")
//    }
    ipWriter.close()
    ipClkTimeWriter.close()
    ipClkCountWriter.close()
//    adidWriter.close()
//    adidClkTimeWriter.close()
//    affWriter.close()
//    affClkTimeWriter.close()
    println("recovery data date: ----","; tasks costs ",System.currentTimeMillis() - startTime, " mills")
  }
}
