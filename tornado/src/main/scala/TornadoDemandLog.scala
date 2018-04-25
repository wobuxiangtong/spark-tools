import java.util.concurrent.Executors
import caseClass.ClickData
import com.amazonaws.SDKGlobalConfiguration
import joptsimple.OptionParser
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object TornadoDemandLog  extends AbstractAnalysis{
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
      .appName("Tornado Demand Log")
      .getOrCreate()
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", PropertyBag.getProperty("aws.access_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", PropertyBag.getProperty("aws.secret_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", PropertyBag.getProperty("aws.s3_endpoint", "s3-ap-southeast-1.amazonaws.com"))
    val schema = Encoders.product[ClickData].schema
    sparkSession.udf.register("getFormatDate",getFormatDate _)
    sparkSession.udf.register("domainSelect",domainSelect(_:String,_:String))
    sparkSession.udf.register("uvSelect",uvSelect(_:String,_:String,_:String,_:String,_:String,_:String,_:String,_:String,_:String))

    val endDateRange = getDateRange(options.valueOf(endDate))
    var startDateRange = getDateRange(options.valueOf(startDate))

    for(i <- -startDateRange to -endDateRange){
      val adxFillMap: Map[String,Any] =Map(
        "dated_adx" -> "1970-01-01",
        "dated_gmt" -> "1970-01-01",
        "timezone" -> 0.0,
        "demand" -> "",
        "campaign_id" -> "",
        "creative_id" -> "",
        "supply" -> "",
        "publisher_domain" -> "",
        "inventory_domain" -> "",
        "app_bundle" -> "",
        "tagid" -> "",
        "country_code"-> "",
        "province_geonameid" -> 0,
        "city_geonameid" -> 0,
        "device_make" -> "",
        "device_model" -> "",
        "os" -> "",
        "carrier" -> "",
        "connection_type" -> 0,
        "age_group" -> "",
        "gender" -> "",
        "demand_currency" -> "",
        "supply_currency" -> "",
        "demand_cost" -> 0.0,
        "supply_revenue"-> 0.0,
        "uv" -> ""
      )
//      val gmtFillMap = adxFillMap - "timezone" - "dated_adx" + ("dated_gmt" -> "1970-01-01")
      val jsonDataFrame = sparkSession.read.schema(schema).json(PropertyBag.getProperty("demand_data_from","").format(getAddedDate(i -1).replace("-",""),getAddedDate(i).replace("-",""),getAddedDate(i+1).replace("-","")) +"*")
      jsonDataFrame.createOrReplaceTempView("demand_init")

      val initQuery = sparkSession.sql(PropertyBag.getProperty("demand_init_query","")).na.fill(adxFillMap)
      initQuery.createOrReplaceTempView("demand_summary")
      val adxClickDailySummaryQuery = sparkSession.sql(PropertyBag.getProperty("adx_demand_daily_summary_query","").format(getAddedDate(i)))
      adxClickDailySummaryQuery.foreachPartition(x => {
        val adxClickDailySummaryQueryFutures = x.map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa1" + row.getString(1))
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("adx_demand_daily_summary_insert", ""))
          try{
            println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa1" + row.getString(1))
            insert.setDouble(1, row.getDouble(0))//timezone
            insert.setString(2, row.getString(1))//dated
            insert.setString(3, row.getString(2))//demand
            insert.setString(4, row.getString(3))//campaign_id
            insert.setString(5, row.getString(4))//creative_id
            insert.setString(6, row.getString(5))//supply
            insert.setString(7, row.getString(6))//publisher_domain
            insert.setString(8, row.getString(7))//inventory_domain
            insert.setString(9, row.getString(8))//app_bundle
            insert.setString(10, row.getString(9))//tagid
            insert.setString(11, row.getString(10))//country_code
            insert.setInt(12, row.getInt(11))//province_geonameid
            insert.setInt(13, row.getInt(12))//city_geonameid
            insert.setString(14, row.getString(13))//device_make
            insert.setString(15, row.getString(14))//device_model
            insert.setString(16, row.getString(15))//os
            insert.setString(17, row.getString(16))//carrier
            insert.setInt(18, row.getInt(17))//connection_type
            insert.setString(19, row.getString(18))//age_group
            insert.setString(20, row.getString(19))//gender
            insert.setString(21, row.getString(20))//currency
            insert.setLong(22, row.getLong(21))//clks
            insert.setLong(23, row.getLong(22))//uniq_clks
            insert.setDouble(24, row.getDouble(23))//cost
            insert.setString(25, getMd5("%1.1f".format(row.getDouble(0)) +
              "||" + row.getString(1) +
              "||" + row.getString(2) +
              "||" + row.getString(3) +
              "||" + row.getString(4) +
              "||" + row.getString(5) +
              "||" + row.getString(6) +
              "||" + row.getString(7) +
              "||" + row.getString(8) +
              "||" + row.getString(9) +
              "||" + row.getString(10) +
              "||" + row.getInt(11) +
              "||" + row.getInt(12) +
              "||" + row.getString(13) +
              "||" + row.getString(14) +
              "||" + row.getString(15) +
              "||" + row.getString(16) +
              "||" + row.getInt(17) +
              "||" + row.getString(18) +
              "||" + row.getString(19) +
              "||" + row.getString(20) +
              "||"))
            insert.setLong(26, row.getLong(21))//clks
            insert.setLong(27, row.getLong(22))//uniq_clks
            insert.setDouble(28, row.getDouble(23))//cost
            println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + row.getString(1))
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
      })


//      val gmtClickDailySummaryQuery = sparkSession.sql(PropertyBag.getProperty("gmt_demand_daily_summary_query","").format(getAddedDate(i)))
//      gmtClickDailySummaryQuery.foreachPartition(x => {
//        val gmtClickDailySummaryQueryFutures =x.map(row => Future{
//          var connection = ConnectionPoolExt.getConnection.orNull
//          if(connection.isClosed){
//            connection = ConnectionPoolExt.getConnection.orNull
//          }
//          connection.setAutoCommit(false)
//          var insert = connection.prepareStatement(PropertyBag.getProperty("gmt_demand_daily_summary_insert", ""))
//          try{
//            insert.setString(1, row.getString(0))//dated
//            insert.setString(2, row.getString(1))//demand
//            insert.setString(3, row.getString(2))//campaign_id
//            insert.setString(4, row.getString(3))//creative_id
//            insert.setString(5, row.getString(4))//supply
//            insert.setString(6, row.getString(5))//publisher_domain
//            insert.setString(7, row.getString(6))//inventory_domain
//            insert.setString(8, row.getString(7))//app_bundle
//            insert.setString(9, row.getString(8))//tagid
//            insert.setString(10, row.getString(9))//country_code
//            insert.setInt(11, row.getInt(10))//province_geonameid
//            insert.setInt(12, row.getInt(11))//city_geonameid
//            insert.setString(13, row.getString(12))//device_make
//            insert.setString(14, row.getString(13))//device_model
//            insert.setString(15, row.getString(14))//os
//            insert.setString(16, row.getString(15))//carrier
//            insert.setInt(17, row.getInt(16))//connection_type
//            insert.setString(18, row.getString(17))//age_group
//            insert.setString(19, row.getString(18))//gender
//            insert.setString(20, row.getString(19))//currency
//            insert.setLong(21, row.getLong(20))//clks
//            insert.setLong(22, row.getLong(21))//uniq_clks
//            insert.setDouble(23, row.getDouble(22))//cost
//            insert.setString(24, getMd5(row.getString(0) +
//              "||" + row.getString(1) +
//              "||" + row.getString(2) +
//              "||" + row.getString(3) +
//              "||" + row.getString(4) +
//              "||" + row.getString(5) +
//              "||" + row.getString(6) +
//              "||" + row.getString(7) +
//              "||" + row.getString(8) +
//              "||" + row.getString(9) +
//              "||" + row.getInt(10) +
//              "||" + row.getInt(11) +
//              "||" + row.getString(12) +
//              "||" + row.getString(13) +
//              "||" + row.getString(14) +
//              "||" + row.getString(15) +
//              "||" + row.getInt(16) +
//              "||" + row.getString(17) +
//              "||" + row.getString(18) +
//              "||" + row.getString(19) +
//              "||"))
//            insert.setLong(25, row.getLong(20))//clks
//            insert.setLong(26, row.getLong(21))//uniq_clks
//            insert.setDouble(27, row.getDouble(22))//cost
//            insert.execute()
//            connection.commit()
//          } catch {
//            case except:Exception => logger.error("error in insert gmt_clk_daily_summary " + except )
//          } finally {
//            insert.close()
//            insert = null
//            connection.close()
//            connection = null
//          }
//        })
//        Await.result(Future.sequence(gmtClickDailySummaryQueryFutures.toList),Duration.Inf)
//      })
//
//
//      val gmtSupplyDailySummaryQuery = sparkSession.sql(PropertyBag.getProperty("gmt_supply_daily_summary_query","").format(getAddedDate(i)))
//      gmtSupplyDailySummaryQuery.foreachPartition(x => {
//        val gmtSupplyDailySummaryQueryFutures = x.map(row => Future{
//          var connection = ConnectionPoolExt.getConnection.orNull
//          if(connection.isClosed){
//            connection = ConnectionPoolExt.getConnection.orNull
//          }
//          connection.setAutoCommit(false)
//          var insert = connection.prepareStatement(PropertyBag.getProperty("gmt_supply_daily_summary_insert", ""))
//          try{
//            insert.setString(1, row.getString(0))//dated
//            insert.setString(2, row.getString(1))//supply
//            insert.setString(3, row.getString(2))//publisher_domain
//            insert.setString(4, row.getString(3))//inventory_domain
//            insert.setString(5, row.getString(4))//app_bundle
//            insert.setString(6, row.getString(5))//tagid
//            insert.setString(7, row.getString(6))//currency
//            insert.setLong(8, row.getLong(7))//clks
//            insert.setDouble(9, row.getDouble(8))//cost
//            insert.setString(10, getMd5(row.getString(0) +
//              "||" + row.getString(1) +
//              "||" + row.getString(2) +
//              "||" + row.getString(3) +
//              "||" + row.getString(4) +
//              "||" + row.getString(5) +
//              "||" + row.getString(6) +
//              "||"))
//            insert.setLong(11, row.getLong(7))//clks
//            insert.setDouble(12, row.getDouble(8))//cost
//            insert.execute()
//            connection.commit()
//          } catch {
//            case except:Exception => logger.error("error in insert gmt_supply " + except )
//          } finally {
//            insert.close()
//            insert = null
//            connection.close()
//            connection = null
//          }
//        })
//        Await.result(Future.sequence(gmtSupplyDailySummaryQueryFutures.toList),Duration.Inf)
//      })

      println("recovery data date: ", getAddedDate(i),"; tasks costs ",System.currentTimeMillis() - startTime, " mills")
    }
  }
}
