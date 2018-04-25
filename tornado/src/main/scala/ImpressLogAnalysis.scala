import java.util.concurrent.Executors

import caseClass.ImpressionData
import com.amazonaws.SDKGlobalConfiguration
import joptsimple.OptionParser
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object ImpressLogAnalysis  extends AbstractAnalysis{
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
  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newWorkStealingPool(200)
    def execute(runnable: Runnable) {
      threadPool.submit(runnable)
    }
    def reportFailure(t: Throwable) {}
  }
  def main(args: Array[String]): Unit = {
    //    System.setProperty("hadoop.home.dir", "C:\\Users\\moca\\办公软件\\hadoop")
    val options = parser.parse(args : _*)
    require(options.valueOf(endDate) != "" || options.valueOf(startDate) != "","起始截至时间不能为空: --start-date 2017-11-11 --end-date 2017-12-12")
    System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")
    val startTime = System.currentTimeMillis()
    val sparkSession = SparkSession
      .builder()
      .appName("Impression Log")
      .getOrCreate()
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", PropertyBag.getProperty("aws.access_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", PropertyBag.getProperty("aws.secret_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", PropertyBag.getProperty("aws.s3_endpoint", "s3-ap-southeast-1.amazonaws.com"))
    val schema = Encoders.product[ImpressionData].schema
    sparkSession.udf.register("getFormatDate",getFormatDate _)
    sparkSession.udf.register("uvGenerator",uvGenerator(_:String,_:String,_:String,_:String,_:String))
    sparkSession.udf.register("getLast",get_last _)
    val endDateRange = getDateRange(options.valueOf(endDate))
    var startDateRange = getDateRange(options.valueOf(startDate))

    for(i <- -startDateRange to -endDateRange){
      val fillMap: Map[String,Any] =Map(
        "timezone" -> 0.0f,
        "dated_adx" -> "1970-01-01",
        "dated_gmt" -> "1970-01-01",
        "publisher_id" -> 0,
        "inventory_id" -> 0,
        "plcmt_id" ->0,
        "campaign_id" -> 0,
        "delivery_id" -> 0,
        "ad_id" -> 0,
          "country_code" -> "",
          "province_geoname_id" -> "0",
          "city_geoname_id" -> "0",
          "device_type" -> "",
          "make" -> "",
          "model" -> "",
          "os" -> "",
          "carrier"-> "",
          "connection_type" -> "",
          "age_group" -> "",
          "gender" -> "",
          "ip" -> "",
          "gaid" -> "",
          "idfa" -> "",
          "ua" -> ""
      )

      val jsonDataFrame = sparkSession.read.schema(schema).json(PropertyBag.getProperty("impress_data_from","").format(getAddedDate(i -1).replace("-",""),getAddedDate(i).replace("-",""),getAddedDate(i+1).replace("-","")) +"*")
      jsonDataFrame.createOrReplaceTempView("impress_init")
      jsonDataFrame.show(false)
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

      val initQuery = sparkSession.sql(PropertyBag.getProperty("impress_init_query","")).na.fill(fillMap)
      initQuery.show(false)
      initQuery.createOrReplaceTempView("impress_summary")
      val adxClickDailySummaryQuery = sparkSession.sql(PropertyBag.getProperty("impress_adx_daily_summary_query","").format(getAddedDate(i)))
//      adxClickDailySummaryQuery.createOrReplaceTempView("adx_click_daily_summary")
      val adxClickDailySummaryQueryFutures = adxClickDailySummaryQuery.collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("impress_adx_daily_summary_insert", ""))
          try{
            insert.setFloat(1, row.getFloat(0))
            insert.setString(2, row.getString(1))
            insert.setInt(3, row.getInt(2))
            insert.setInt(4, row.getInt(3))
            insert.setInt(5, row.getInt(4))
            insert.setInt(6, row.getInt(5))
            insert.setInt(7, row.getInt(6))
            insert.setInt(8, row.getInt(7))
            insert.setString(9, row.getString(8))
            insert.setInt(10, row.getString(9).toInt)
            insert.setInt(11, row.getString(10).toInt)
            insert.setString(12, row.getString(11))
            insert.setString(13, row.getString(12))
            insert.setString(14, row.getString(13))
            insert.setString(15, row.getString(14))
            insert.setString(16, row.getString(15))
            insert.setString(17, row.getString(16))
            insert.setString(18, row.getString(17))
            insert.setString(19, row.getString(18))
            insert.setString(20,getMd5(
                    "%1.1f".format(row.getFloat(0)) +"||" +
                    row.getString(1) + "||" +
                    row.getInt(2)+ "||" +
                    row.getInt(3)+ "||" +
                    row.getInt(4)+ "||" +
                    row.getInt(5)+ "||" +
                    row.getInt(6)+ "||" +
                    row.getInt(7)+ "||" +
                    row.getString(8)+ "||" +
                    row.getString(9)+ "||" +
                    row.getString(10)+ "||" +
                    row.getString(11)+ "||" +
                    row.getString(12)+ "||" +
                    row.getString(13)+ "||" +
                    row.getString(14)+ "||" +
                    row.getString(15)+ "||" +
                    row.getString(16)+ "||" +
                    row.getString(17)+ "||" +
                    row.getString(18)+ "||"
            ))
            insert.setLong(21, row.getLong(19))
            insert.setLong(22, row.getLong(20))
            insert.setLong(23, row.getLong(19))
            insert.setLong(24, row.getLong(20))
            insert.execute()
            connection.commit()
          } catch {
            case except:Exception => logger.error("error in insert adx_clk_daily_summary " + except )
          } finally {
            insert.close()
            insert = null
            connection.close()
            connection = null
          }
        })
      Await.result(Future.sequence(adxClickDailySummaryQueryFutures.toList),Duration.Inf)

      val gmtClickDailySummaryQuery = sparkSession.sql(PropertyBag.getProperty("impress_gmt_daily_summary_query","").format(getAddedDate(i)))
//      gmtClickDailySummaryQuery.createOrReplaceTempView("gmt_click_daily_summary")
      val gmtClickDailySummaryQueryFutures = gmtClickDailySummaryQuery.collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("impress_gmt_daily_summary_insert", ""))
          try{
            insert.setString(1, row.getString(0))
            insert.setInt(2, row.getInt(1))
            insert.setInt(3, row.getInt(2))
            insert.setInt(4, row.getInt(3))
            insert.setInt(5, row.getInt(4))
            insert.setInt(6, row.getInt(5))
            insert.setInt(7, row.getInt(6))
            insert.setString(8, row.getString(7))
            insert.setInt(9, row.getString(8).toInt)
            insert.setInt(10, row.getString(9).toInt)
            insert.setString(11, row.getString(10))
            insert.setString(12, row.getString(11))
            insert.setString(13, row.getString(12))
            insert.setString(14, row.getString(13))
            insert.setString(15, row.getString(14))
            insert.setString(16, row.getString(15))
            insert.setString(17, row.getString(16))
            insert.setString(18, row.getString(17))
            insert.setString(19,getMd5(
                row.getString(0) + "||" +
                row.getInt(1)+ "||" +
                row.getInt(2)+ "||" +
                row.getInt(3)+ "||" +
                row.getInt(4)+ "||" +
                row.getInt(5)+ "||" +
                row.getInt(6)+ "||" +
                row.getString(7)+ "||" +
                row.getString(8)+ "||" +
                row.getString(9)+ "||" +
                row.getString(10)+ "||" +
                row.getString(11)+ "||" +
                row.getString(12)+ "||" +
                row.getString(13)+ "||" +
                row.getString(14)+ "||" +
                row.getString(15)+ "||" +
                row.getString(16)+ "||" +
                row.getString(17)+ "||"
            ))
            insert.setLong(20, row.getLong(18))
            insert.setLong(21, row.getLong(19))
            insert.setLong(22, row.getLong(18))
            insert.setLong(23, row.getLong(19))
            insert.execute()
            connection.commit()
          } catch {
            case except:Exception => logger.error("error in insert gmt_clk_daily_summary " + except )
          } finally {
            insert.close()
            insert = null
            connection.close()
            connection = null
          }
        })
      Await.result(Future.sequence(gmtClickDailySummaryQueryFutures.toList),Duration.Inf)
      println("recovery data date: ", getAddedDate(i),"; tasks costs ",System.currentTimeMillis() - startTime, " mills")
    }
  }
}

