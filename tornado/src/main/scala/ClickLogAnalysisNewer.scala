import java.util.concurrent.Executors

import caseClass.ClickDataExt
import com.amazonaws.SDKGlobalConfiguration
import joptsimple.OptionParser
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object ClickLogAnalysisNewer  extends AbstractAnalysis{
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
      .appName("Tornado Conversion Log")
      .getOrCreate()
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", PropertyBag.getProperty("aws.access_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", PropertyBag.getProperty("aws.secret_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", PropertyBag.getProperty("aws.s3_endpoint", "s3-ap-southeast-1.amazonaws.com"))
    val schema = Encoders.product[ClickDataExt].schema
    sparkSession.udf.register("getFormatDate",getFormatDate _)
    sparkSession.udf.register("uvGenerator",uvGenerator(_:String,_:String,_:String,_:String,_:String))

    val endDateRange = getDateRange(options.valueOf(endDate))
    var startDateRange = getDateRange(options.valueOf(startDate))

    for(i <- -startDateRange to -endDateRange){
      val fillMap: Map[String,Any] =Map(
        "timezone" -> 0.0f,
        "clk_timestamp" -> 0,
        "campaign_id" -> 0,
        "group_id" -> 0,
        "aff_id" -> 0,
        "aff_pub" -> "",
        "country_code" -> "",
        "province_geoname_id" -> 0,
        "city_geoname_id" -> 0,
        "os" -> "",
        "device_type"-> "",
        "device_make" -> "",
        "device_model" -> "",
        "algorithm" -> 0,
        "ip" -> "",
        "ua" -> "",
        "gaid" -> "",
        "idfa" -> "",
        "adid" -> ""
      )
      val jsonDataFrame = sparkSession.read.schema(schema).json(PropertyBag.getProperty("click_data_from","").format(getAddedDate(i -1).replace("-",""),getAddedDate(i).replace("-",""),getAddedDate(i+1).replace("-","")) +"*").na.fill(fillMap)
      jsonDataFrame.createOrReplaceTempView("click_init")

      val initQuery = sparkSession.sql(PropertyBag.getProperty("click_init_query",""))
      initQuery.createOrReplaceTempView("click_summary")
      val adxClickDailySummaryQuery = sparkSession.sql(PropertyBag.getProperty("adx_click_daily_summary_query","").format(getAddedDate(i)))
      adxClickDailySummaryQuery.createOrReplaceTempView("adx_click_daily_summary")
      val adxClickDailySummaryQueryFutures = adxClickDailySummaryQuery.collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("adx_click_daily_summary_insert", ""))
          try{
            insert.setFloat(1, row.getFloat(0))
            insert.setString(2, row.getString(1))
            insert.setInt(3, row.getInt(2))
            insert.setInt(4, row.getInt(3))
            insert.setInt(5, row.getInt(4))
            insert.setString(6, row.getString(5))
            insert.setString(7, row.getString(6))
            insert.setInt(8,row.getInt(7))
            insert.setInt(9,row.getInt(8))
            insert.setString(10, row.getString(9))
            insert.setString(11, row.getString(10))
            insert.setString(12, row.getString(11))
            insert.setString(13, row.getString(12))
            insert.setInt(14,row.getInt(13))
            insert.setLong(15, row.getLong(14))
            insert.setLong(16, row.getLong(15))
            insert.setString(17,getMd5("%1.1f".format(row.getFloat(0)) +
              "||" + row.getString(1) +
              "||" + row.getInt(2) +
              "||" + row.getInt(3) +
              "||" + row.getInt(4) +
              "||" + row.getString(5) +
              "||" + row.getString(6) +
              "||" + row.getInt(7) +
              "||" + row.getInt(8) +
              "||" + row.getString(9) +
              "||" + row.getString(10) +
              "||" + row.getString(11) +
              "||" + row.getString(12) +
              "||" + row.getInt(13) + "||")
            )
            insert.setLong(18, row.getLong(14))
            insert.setLong(19, row.getLong(15))
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

      val gmtClickDailySummaryQuery = sparkSession.sql(PropertyBag.getProperty("gmt_click_daily_summary_query","").format(getAddedDate(i)))
      gmtClickDailySummaryQuery.createOrReplaceTempView("gmt_click_daily_summary")
      val gmtClickDailySummaryQueryFutures = gmtClickDailySummaryQuery.collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("gmt_click_daily_summary_insert", ""))
          try{
            insert.setString(1, row.getString(0))
            insert.setInt(2, row.getInt(1))
            insert.setInt(3, row.getInt(2))
            insert.setInt(4, row.getInt(3))
            insert.setString(5, row.getString(4))
            insert.setString(6, row.getString(5))
            insert.setInt(7,row.getInt(6))
            insert.setInt(8,row.getInt(7))
            insert.setString(9, row.getString(8))
            insert.setString(10, row.getString(9))
            insert.setString(11, row.getString(10))
            insert.setString(12, row.getString(11))
            insert.setInt(13,row.getInt(12))
            insert.setLong(14, row.getLong(13))
            insert.setLong(15, row.getLong(14))
            insert.setString(16,getMd5(row.getString(0) +
              "||" + row.getInt(1) +
              "||" + row.getInt(2) +
              "||" + row.getInt(3) +
              "||" + row.getString(4) +
              "||" + row.getString(5) +
              "||" + row.getInt(6) +
              "||" + row.getInt(7) +
              "||" + row.getString(8) +
              "||" + row.getString(9) +
              "||" + row.getString(10) +
              "||" + row.getString(11) +
              "||" + row.getInt(12) + "||")
            )
            insert.setLong(17, row.getLong(13))
            insert.setLong(18, row.getLong(14))
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

      val adxDailySummaryQueryFutures = sparkSession.sql(PropertyBag.getProperty("click_adx_daily_summary_query","")).collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("click_adx_daily_summary_insert", ""))
          try {
            insert.setFloat(1, row.getFloat(0)) //timezone
            insert.setString(2, row.getString(1)) //dated
            insert.setInt(3, row.getInt(2)) //campaign_id
            insert.setInt(4, row.getInt(3)) //group_id
            insert.setInt(5, row.getInt(4)) //aff_id
            insert.setString(6, row.getString(5)) //aff_pub
            insert.setInt(7,row.getInt(6)) //algorithm
            insert.setLong(8, row.getLong(7))
            insert.setLong(9, row.getLong(8))
            insert.setString(10,getMd5("%1.1f".format(row.getFloat(0))+"||"+row.getString(1)+"||"+row.getInt(2)+"||"+row.getInt(3)+"||"+row.getInt(4)+"||"+row.getString(5)+"||"+row.getInt(6)+"||"))
            insert.setLong(11, row.getLong(7))
            insert.setLong(12, row.getLong(8))
            insert.execute()
            connection.commit()
          } catch {
            case except:Exception => logger.error("error in insert adx_daily_summary ",except)
          } finally {
            insert.close()
            insert = null
            connection.close()
            connection = null
          }
        })
      Await.result(Future.sequence(adxDailySummaryQueryFutures.toList),Duration.Inf)

      val gmtDailySummaryQueryFutures = sparkSession.sql(PropertyBag.getProperty("click_gmt_daily_summary_query","")).collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("click_gmt_daily_summary_insert", ""))
          try {
            insert.setString(1, row.getString(0)) //dated
            insert.setInt(2, row.getInt(1)) //campaign_id
            insert.setInt(3, row.getInt(2)) //group_id
            insert.setInt(4, row.getInt(3)) //aff_id
            insert.setString(5, row.getString(4)) //aff_pub
            insert.setInt(6,row.getInt(5)) //algorithm
            insert.setLong(7, row.getLong(6))
            insert.setLong(8, row.getLong(7))
            insert.setString(9,getMd5(row.getString(0)+"||"+row.getInt(1)+"||"+row.getInt(2)+"||"+row.getInt(3)+"||"+row.getString(4)+"||"+row.getInt(5) +"||"))
            insert.setLong(10, row.getLong(6))
            insert.setLong(11, row.getLong(7))
            insert.execute()
            connection.commit()
          } catch {
            case except:Exception => logger.error("error in insert gmt_daily_summary ",except)
          } finally {
            insert.close()
            insert = null
            connection.close()
            connection = null
          }
        })
      Await.result(Future.sequence(gmtDailySummaryQueryFutures.toList),Duration.Inf)
      println("recovery data date: ", getAddedDate(i),"; tasks costs ",System.currentTimeMillis() - startTime, " mills")
    }
  }
}
