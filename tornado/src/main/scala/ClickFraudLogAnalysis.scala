import java.io.{File, PrintWriter}
import java.util.concurrent.Executors

import caseClass.{ClickFraudLog,EventFraudLog}
import joptsimple.OptionParser
import org.apache.spark.sql.{Encoders, SparkSession}
import org.json4s._
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.ExecutionContext

object ClickFraudLogAnalysis  extends AbstractAnalysis{
  val logger = LoggerFactory.getLogger(this.getClass)
  val parser = new OptionParser
  val startDate = parser.accepts("start-date", "the date to start")
    .withRequiredArg
    .describedAs("2017-11-11")
    .ofType(classOf[java.lang.String])
    .defaultsTo("")
  val endDate = parser.accepts("end-date", "the date ended")
    .withRequiredArg
    .describedAs("2017-11-12")
    .ofType(classOf[java.lang.String])
    .defaultsTo("")

  implicit def intToString(x:BigInt) = x.toInt
  implicit val formats = Serialization.formats(NoTypeHints)
  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newWorkStealingPool(200)
    def execute(runnable: Runnable) {
      threadPool.submit(runnable)
    }
    def reportFailure(t: Throwable) {}
  }
  def main(args: Array[String]): Unit = {

    val options = parser.parse(args : _*)

    val sparkSession = SparkSession
      .builder()
      .appName("Tornado ClickFraud Log")
      .getOrCreate()
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", PropertyBag.getProperty("aws.access_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", PropertyBag.getProperty("aws.secret_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", PropertyBag.getProperty("aws.s3_endpoint", "s3-ap-southeast-1.amazonaws.com"))

    sparkSession.udf.register("getFormatDate",getFormatDate _)
    sparkSession.udf.register("getMod",getMod(_:Int,_:Int))
    sparkSession.udf.register("getFormatDateToHour",getFormatDateToHour(_:Long))
    sparkSession.udf.register("hasNotChar",hasNotChar(_:String,_:String))

    val clickFillMap: Map[String,Any] =Map(
      "aff_id"->0,
      "aff_pub" -> "",
      "ip" -> "",
      "gaid" -> "",
      "idfa" -> ""
    )
    val eventFillMap: Map[String,Any] =Map(
      "event_value" -> 0,
      "aff_id"->0,
      "aff_pub" -> "",
      "ip" -> "",
      "gaid" -> "",
      "idfa" -> ""
    )

    val clickIpWriter20 = new PrintWriter(new File("/mnt/xvdb/tmp/click_analysis_ip_20"  + ".csv" ))
    val clickIpWriter30 = new PrintWriter(new File("/mnt/xvdb/tmp/click_analysis_ip_30"  + ".csv" ))
    val clickIpWriter40 = new PrintWriter(new File("/mnt/xvdb/tmp/click_analysis_ip_40"  + ".csv" ))
    val clickIpWriter50 = new PrintWriter(new File("/mnt/xvdb/tmp/click_analysis_ip_50"  + ".csv" ))
    val clickAdidWriter10 = new PrintWriter(new File("/mnt/xvdb/tmp/click_analysis_adid_10"  + ".csv" ))
    val clickAdidWriter20 = new PrintWriter(new File("/mnt/xvdb/tmp/click_analysis_adid_20"  + ".csv" ))
    val clickAdidWriter30 = new PrintWriter(new File("/mnt/xvdb/tmp/click_analysis_adid_30"  + ".csv" ))
    val clickAdidWriter40 = new PrintWriter(new File("/mnt/xvdb/tmp/click_analysis_adid_40"  + ".csv" ))
    val clickAffWriter = new PrintWriter(new File("/mnt/xvdb/tmp/click_analysis_aff"  + ".csv" ))

    val eventIpWriter = new PrintWriter(new File("/mnt/xvdb/tmp/event_analysis_ip"  + ".csv" ))
    val eventAdidWriter = new PrintWriter(new File("/mnt/xvdb/tmp/event_analysis_adid"  + ".csv" ))
    val eventAffWriter = new PrintWriter(new File("/mnt/xvdb/tmp/event_analysis_aff"  + ".csv" ))

    val click_fraud_log = new PrintWriter(new File("/mnt/xvdb/tmp/click_fraud_task"  + ".log" ))

    val endDateRange = getDateRange(options.valueOf(endDate))
    var startDateRange = getDateRange(options.valueOf(startDate))

    val hourList = List[String]("00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23")

    for(i <- -startDateRange to -endDateRange){
      hourList.foreach(hour => {
        val startTime = System.currentTimeMillis()

        val clickDataFrame = sparkSession.read.schema(Encoders.product[ClickFraudLog].schema).json(PropertyBag.getProperty("click_fraud_data_from","")+ getAddedDate(i).replace("-","") + hour +"*").na.fill(clickFillMap)
        clickDataFrame.createOrReplaceTempView("click_fraud_init")
        val clickQuery = sparkSession.sql(PropertyBag.getProperty("click_fraud_init_query",""))
        clickQuery.createOrReplaceTempView("click_fraud_summary")

        val clickIpSummaryQuery = sparkSession.sql(PropertyBag.getProperty("click_fraud_ip_query",""))
//        ipSummaryQuery.show(false)
        clickIpSummaryQuery.collect().foreach(row => {
          if(row.getLong(1) > 5){
            clickIpWriter20.write(getAddedDate(i).replace("-","") + hour + "," + row.getString(0) + "," + row.getLong(1) + "\n")
            clickIpWriter20.flush()
          }
        })
        val clickAdidSummaryQuery = sparkSession.sql(PropertyBag.getProperty("click_fraud_adid_query",""))
//        adidSummaryQuery.show(false)
        clickAdidSummaryQuery.collect().foreach(row => {
          if(row.getLong(2) > 5){
            clickAdidWriter10.write(getAddedDate(i).replace("-","") + hour + "," + row.getString(0) + "," + row.getString(1) + "," + row.getLong(2) + "\n")
            clickAdidWriter10.flush()
          }
        })
//        val clickAffSummaryQuery = sparkSession.sql(PropertyBag.getProperty("click_fraud_aff_query",""))
////        affSummaryQuery.show(false)
//        clickAffSummaryQuery.collect().foreach(row => {
//          if(row.getLong(2) > 5){
//            clickAffWriter.write(getAddedDate(i).replace("-","") + hour + "," + row.getInt(0) + "," + row.getString(1) + "," + row.getLong(2) + "\n")
//            clickAffWriter.flush()
//          }
//        })

        /*
        * event fraud summary
        * */

        val eventDataFrame = sparkSession.read.schema(Encoders.product[EventFraudLog].schema).json(PropertyBag.getProperty("event_fraud_data_from","")+ getAddedDate(i).replace("-","") + hour +"*").na.fill(eventFillMap)
        eventDataFrame.createOrReplaceTempView("event_fraud_init")
        val eventQuery = sparkSession.sql(PropertyBag.getProperty("event_fraud_init_query",""))
        eventQuery.createOrReplaceTempView("event_fraud_summary")

        val eventIpSummaryQuery = sparkSession.sql(PropertyBag.getProperty("event_fraud_ip_query",""))
        //        ipSummaryQuery.show(false)
        eventIpSummaryQuery.collect().foreach(row => {
//          if(row.getLong(1) > 5){
            eventIpWriter.write(getAddedDate(i).replace("-","") + hour + "," + row.getString(0) + "," + row.getInt(1)  + "," + row.getLong(2) + "\n")
            eventIpWriter.flush()
//          }
        })

        val eventAdidSummaryQuery = sparkSession.sql(PropertyBag.getProperty("event_fraud_adid_query",""))
        //        adidSummaryQuery.show(false)
        eventAdidSummaryQuery.collect().foreach(row => {
//          if(row.getLong(2) > 5){
            eventAdidWriter.write(getAddedDate(i).replace("-","") + hour + "," + row.getString(0) + "," + row.getString(1) + "," + row.getInt(2) + "," + row.getLong(3) + "\n")
            eventAdidWriter.flush()
//          }
        })

        val eventAffSummaryQuery = sparkSession.sql(PropertyBag.getProperty("event_fraud_aff_query",""))
        //        affSummaryQuery.show(false)
        eventAffSummaryQuery.collect().foreach(row => {
//          if(row.getLong(2) > 5){
            eventAffWriter.write(getAddedDate(i).replace("-","") + hour + "," + row.getInt(0) + "," + row.getString(1) + "," + row.getInt(2) + "," + row.getLong(3) + "\n")
            eventAffWriter.flush()
//          }
        })

        click_fraud_log.write("recovery data date: %s".format(getAddedDate(i).replace("-","") + hour) + "; tasks costs " + (System.currentTimeMillis() - startTime)/(1000*60) +  " min(s)\n" )
        click_fraud_log.flush()
      })
    }


//      val mapper = new ObjectMapper()
    clickIpWriter20.close()
    clickAdidWriter10.close()
    clickAffWriter.close()
    eventIpWriter.close()
    eventAdidWriter.close()
    eventAffWriter.close()
    click_fraud_log.close()

  }
}
