import java.util.concurrent.Executors

import caseClass.EventStremingNewer
import com.amazonaws.SDKGlobalConfiguration
import joptsimple.OptionParser
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object EventLogAnalysisNewer  extends AbstractAnalysis{
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
      .appName("Tornado Event Log")
      .getOrCreate()
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", PropertyBag.getProperty("aws.access_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", PropertyBag.getProperty("aws.secret_key", ""))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", PropertyBag.getProperty("aws.s3_endpoint", "s3-ap-southeast-1.amazonaws.com"))
    val schema = Encoders.product[EventStremingNewer].schema
    sparkSession.udf.register("getFormatDate",getFormatDate _)
    sparkSession.udf.register("retentDays",retentDays(_:Int,_:Int))

    val endDateRange = getDateRange(options.valueOf(endDate))
    var startDateRange = getDateRange(options.valueOf(startDate))

    for(i <- -startDateRange to -endDateRange){
      val fillMap: Map[String,Any] =Map(
      "timezone" -> 0.0f,
      "campaign_id"->0,
      "group_id"->0,
      "aff_id"->0,
      "aff_pub"->"",
      "event_value"->"",
      "install_timestamp"->0,
      "event_timestamp"->0,
      "order_amount"->0.0f,
      "order_currency"->"",
      "algorithm"->0,
      "kpi"->0,
      "click_id"->""
      )
      val jsonDataFrame = sparkSession.read.schema(schema).json(PropertyBag.getProperty("event_data_from","").format(getAddedDate(i-1).replace("-",""),getAddedDate(i).replace("-",""),getAddedDate(i+1).replace("-","")) +"*").na.fill(fillMap)
      jsonDataFrame.createOrReplaceTempView("event_init")
      val eventQuery = sparkSession.sql(PropertyBag.getProperty("event_init_query",""))
      eventQuery.createOrReplaceTempView("event_summary")

      val adxEventDailySummaryPVUVQueryFutures = sparkSession.sql(PropertyBag.getProperty("adx_event_daily_summary_pvuv_query","").format(getAddedDate(i))).collect()
        .map(row => Future{
          var adxEventDailySummaryPVUVConnection = ConnectionPoolExt.getConnection.orNull
          if(adxEventDailySummaryPVUVConnection.isClosed){
            adxEventDailySummaryPVUVConnection = ConnectionPoolExt.getConnection.orNull
          }
          adxEventDailySummaryPVUVConnection.setAutoCommit(false)
          var adxEventDailySummaryPVUVConnectionInsert = adxEventDailySummaryPVUVConnection.prepareStatement(PropertyBag.getProperty("adx_event_daily_summary_pvuv_insert", ""))
          try{
            adxEventDailySummaryPVUVConnectionInsert.setFloat(1,row.getFloat(2))
            adxEventDailySummaryPVUVConnectionInsert.setString(2,row.getString(4))
            adxEventDailySummaryPVUVConnectionInsert.setInt(3,row.getInt(5))
            adxEventDailySummaryPVUVConnectionInsert.setInt(4,row.getInt(6))
            adxEventDailySummaryPVUVConnectionInsert.setInt(5,row.getInt(7))
            adxEventDailySummaryPVUVConnectionInsert.setString(6,row.getString(8))
            adxEventDailySummaryPVUVConnectionInsert.setInt(7,row.getInt(9))
            adxEventDailySummaryPVUVConnectionInsert.setString(8,row.getString(10))
            adxEventDailySummaryPVUVConnectionInsert.setInt(9,row.getInt(11))

            adxEventDailySummaryPVUVConnectionInsert.setLong(10,row.getLong(12))
            adxEventDailySummaryPVUVConnectionInsert.setLong(11,row.getLong(13))
            adxEventDailySummaryPVUVConnectionInsert.setDouble(12,row.getDouble(14))

            adxEventDailySummaryPVUVConnectionInsert.setString(13,getMd5("%1.1f".format(row.getFloat(2)) +
              "||" + row.getString(4) +
              "||" + row.getInt(5) +
              "||" + row.getInt(6) +
              "||" + row.getInt(7) +
              "||" + row.getString(8) +
              "||" + row.getInt(9) +
              "||" + row.getString(10) +
              "||" + row.getInt(11) + "||"))

            adxEventDailySummaryPVUVConnectionInsert.setLong(14,row.getLong(12))
            adxEventDailySummaryPVUVConnectionInsert.setLong(15,row.getLong(13))
            adxEventDailySummaryPVUVConnectionInsert.setDouble(16,row.getDouble(14))
            adxEventDailySummaryPVUVConnectionInsert.execute()
            adxEventDailySummaryPVUVConnection.commit()
          } catch {
            case e:Exception => logger.error("error in insert event adx pvuv " + e )
          } finally {
            adxEventDailySummaryPVUVConnectionInsert.close()
            adxEventDailySummaryPVUVConnection.close()
            adxEventDailySummaryPVUVConnectionInsert = null
            adxEventDailySummaryPVUVConnection = null
          }
        })
      Await.result(Future.sequence(adxEventDailySummaryPVUVQueryFutures.toList),Duration.Inf)

      val adxDailySummaryPVUVQueryFutures = sparkSession.sql(PropertyBag.getProperty("adx_event_daily_summary_pvuv_query","").format(getAddedDate(i))).collect()
        .map(row => Future{
          var adxDailySummaryPVUVConnection = ConnectionPoolExt.getConnection.orNull
          if(adxDailySummaryPVUVConnection.isClosed){
            adxDailySummaryPVUVConnection = ConnectionPoolExt.getConnection.orNull
          }
          adxDailySummaryPVUVConnection.setAutoCommit(false)
          var adxDailySummaryPVUVConnectionInsert = adxDailySummaryPVUVConnection.prepareStatement(PropertyBag.getProperty("event_adx_daily_summary_insert", ""))

          try{
            var retent_1 = 0l
            var retent_2 = 0l
            var retent_7 = 0l
            var retent_15 = 0l
            var retent_30 = 0l
            var kpi = 0l
            //            println("------------------->",row.schema)
            if (row.getInt(0) < 1){
              retent_1 = row.getLong(13)
            }
            if(row.getInt(0) < 2){
              retent_2 = row.getLong(13)
            }
            if(row.getInt(0) < 7){
              retent_7 = row.getLong(13)
            }
            if(row.getInt(0) < 15){
              retent_15 = row.getLong(13)
            }
            if(row.getInt(0) < 30){
              retent_30 = row.getLong(13)
            }

            if(row.getInt(1) == 1){
              kpi = row.getLong(12)
            }else if(row.getInt(1) == 2){
              kpi =row.getLong(13)
            }else if(row.getInt(1) == 91){
              kpi = retent_1
            }else if(row.getInt(1) == 92){
              kpi = retent_2
            }else if(row.getInt(1) == 97){
              kpi = retent_7
            }else if(row.getInt(1) == 915){
              kpi = retent_15
            }else if(row.getInt(1) == 930){
              kpi = retent_30
            }
            //adx_event_daily_summary
            adxDailySummaryPVUVConnectionInsert.setFloat(1,row.getFloat(2))
            adxDailySummaryPVUVConnectionInsert.setInt(3,row.getInt(5))
            adxDailySummaryPVUVConnectionInsert.setInt(4,row.getInt(6))
            adxDailySummaryPVUVConnectionInsert.setInt(5,row.getInt(7))
            adxDailySummaryPVUVConnectionInsert.setString(6,row.getString(8))
            adxDailySummaryPVUVConnectionInsert.setInt(7,row.getInt(11))
            adxDailySummaryPVUVConnectionInsert.setLong(8,kpi)
            adxDailySummaryPVUVConnectionInsert.setLong(10,kpi)

            if(kpi != 0l && (row.getInt(1) < 10)){
              adxDailySummaryPVUVConnectionInsert.setString(2,row.getString(4))
              adxDailySummaryPVUVConnectionInsert.setString(9,getMd5("%1.1f".format(row.getFloat(2)) +
                "||" + row.getString(4) +
                "||" + row.getInt(5) +
                "||" + row.getInt(6) +
                "||" + row.getInt(7) +
                "||" + row.getString(8) +
                "||" + row.getInt(11) + "||"))
              adxDailySummaryPVUVConnectionInsert.execute()
              adxDailySummaryPVUVConnection.commit()
            }else if(kpi !=0l && (row.getInt(1) > 10)){
              adxDailySummaryPVUVConnectionInsert.setString(2,row.getString(3))
              adxDailySummaryPVUVConnectionInsert.setString(9,getMd5("%1.1f".format(row.getFloat(2)) +
                "||" + row.getString(3) +
                "||" + row.getInt(5) +
                "||" + row.getInt(6) +
                "||" + row.getInt(7) +
                "||" + row.getString(8) +
                "||" + row.getInt(11) + "||"))
              adxDailySummaryPVUVConnectionInsert.execute()
              adxDailySummaryPVUVConnection.commit()
            }

          } catch {
            case e:Exception => logger.error("error in insert event adx pvuv " + e )
          } finally {
            adxDailySummaryPVUVConnectionInsert.close()
            adxDailySummaryPVUVConnection.close()
            adxDailySummaryPVUVConnectionInsert = null
            adxDailySummaryPVUVConnection = null
          }
        })
            Await.result(Future.sequence(adxDailySummaryPVUVQueryFutures.toList),Duration.Inf)

      val adxEventDailySummaryRetentQueryFutures = sparkSession.sql(PropertyBag.getProperty("adx_event_daily_summary_pvuv_query","").format(getAddedDate(i))).collect()
        .map(row => Future{
          var adxEventDailySummaryRetentConnection = ConnectionPoolExt.getConnection.orNull
          if(adxEventDailySummaryRetentConnection.isClosed){
            adxEventDailySummaryRetentConnection = ConnectionPoolExt.getConnection.orNull
          }
          adxEventDailySummaryRetentConnection.setAutoCommit(false)
          var adxEventDailySummaryRetentInsert = adxEventDailySummaryRetentConnection.prepareStatement(PropertyBag.getProperty("adx_event_daily_summary_retent_insert", ""))
          try{
            var retent_1 = 0l
            var retent_2 = 0l
            var retent_7 = 0l
            var retent_15 = 0l
            var retent_30 = 0l
            if (row.getInt(0) < 1){
              retent_1 = row.getLong(13)
            }
            if(row.getInt(0) < 2){
              retent_2 = row.getLong(13)
            }
            if(row.getInt(0) < 7){
              retent_7 = row.getLong(13)
            }
            if(row.getInt(0) < 15){
              retent_15 = row.getLong(13)
            }
            if(row.getInt(0) < 30){
              retent_30 = row.getLong(13)
            }
            //adx_event_daily_summary_retent
            if(retent_30 !=0l){
              adxEventDailySummaryRetentInsert.setFloat(1,row.getFloat(2))
              adxEventDailySummaryRetentInsert.setString(2,row.getString(3))
              adxEventDailySummaryRetentInsert.setInt(3,row.getInt(5))
              adxEventDailySummaryRetentInsert.setInt(4,row.getInt(6))
              adxEventDailySummaryRetentInsert.setInt(5,row.getInt(7))
              adxEventDailySummaryRetentInsert.setString(6,row.getString(8))
              adxEventDailySummaryRetentInsert.setInt(7,row.getInt(9))
              adxEventDailySummaryRetentInsert.setString(8,row.getString(10))
              adxEventDailySummaryRetentInsert.setInt(9,row.getInt(11))
              adxEventDailySummaryRetentInsert.setLong(10,retent_1)
              adxEventDailySummaryRetentInsert.setLong(11,retent_2)
              adxEventDailySummaryRetentInsert.setLong(12,retent_7)
              adxEventDailySummaryRetentInsert.setLong(13,retent_15)
              adxEventDailySummaryRetentInsert.setLong(14,retent_30)

              adxEventDailySummaryRetentInsert.setString(15,getMd5("%1.1f".format(row.getFloat(2)) +
                "||" + row.getString(3) +
                "||" + row.getInt(5) +
                "||" + row.getInt(6) +
                "||" + row.getInt(7) +
                "||" + row.getString(8) +
                "||" + row.getInt(9) +
                "||" + row.getString(10) +
                "||" + row.getInt(11) + "||"))
              adxEventDailySummaryRetentInsert.setLong(16,retent_1)
              adxEventDailySummaryRetentInsert.setLong(17,retent_2)
              adxEventDailySummaryRetentInsert.setLong(18,retent_7)
              adxEventDailySummaryRetentInsert.setLong(19,retent_15)
              adxEventDailySummaryRetentInsert.setLong(20,retent_30)
              adxEventDailySummaryRetentInsert.execute()
              adxEventDailySummaryRetentConnection.commit()
            }
          } catch {
            case e:Exception => logger.error("error in insert event adx pvuv " + e )
          } finally {

            adxEventDailySummaryRetentInsert.close()
            adxEventDailySummaryRetentConnection.close()
            adxEventDailySummaryRetentInsert = null
            adxEventDailySummaryRetentConnection = null
          }
        })
      Await.result(Future.sequence(adxEventDailySummaryRetentQueryFutures.toList),Duration.Inf)

      val gmtEventDailySummaryPVUVQueryFutures = sparkSession.sql(PropertyBag.getProperty("gmt_event_daily_summary_pvuv_query","").format(getAddedDate(i))).collect()
        .map(row => Future{
          var gmtEventDailySummaryPVUVConnection = ConnectionPoolExt.getConnection.orNull
          if(gmtEventDailySummaryPVUVConnection.isClosed){
            gmtEventDailySummaryPVUVConnection = ConnectionPoolExt.getConnection.orNull
          }
          gmtEventDailySummaryPVUVConnection.setAutoCommit(false)
          var gmtEventDailySummaryPVUVConnectionInsert = gmtEventDailySummaryPVUVConnection.prepareStatement(PropertyBag.getProperty("gmt_event_daily_summary_pvuv_insert", ""))
          try{
            gmtEventDailySummaryPVUVConnectionInsert.setString(1,row.getString(3))
            gmtEventDailySummaryPVUVConnectionInsert.setInt(2,row.getInt(4))
            gmtEventDailySummaryPVUVConnectionInsert.setInt(3,row.getInt(5))
            gmtEventDailySummaryPVUVConnectionInsert.setInt(4,row.getInt(6))
            gmtEventDailySummaryPVUVConnectionInsert.setString(5,row.getString(7))
            gmtEventDailySummaryPVUVConnectionInsert.setInt(6,row.getInt(8))
            gmtEventDailySummaryPVUVConnectionInsert.setString(7,row.getString(9))
            gmtEventDailySummaryPVUVConnectionInsert.setInt(8,row.getInt(10))

            gmtEventDailySummaryPVUVConnectionInsert.setLong(9,row.getLong(11))
            gmtEventDailySummaryPVUVConnectionInsert.setLong(10,row.getLong(12))
            gmtEventDailySummaryPVUVConnectionInsert.setDouble(11,row.getDouble(13))

            gmtEventDailySummaryPVUVConnectionInsert.setString(12,getMd5(row.getString(3) +
              "||" + row.getInt(4) +
              "||" + row.getInt(5) +
              "||" + row.getInt(6) +
              "||" + row.getString(7) +
              "||" + row.getInt(8) +
              "||" + row.getString(9) +
              "||" + row.getInt(10) + "||"))

            gmtEventDailySummaryPVUVConnectionInsert.setLong(13,row.getLong(11))
            gmtEventDailySummaryPVUVConnectionInsert.setLong(14,row.getLong(12))
            gmtEventDailySummaryPVUVConnectionInsert.setDouble(15,row.getDouble(13))

            gmtEventDailySummaryPVUVConnectionInsert.execute()
            gmtEventDailySummaryPVUVConnection.commit()

          } catch {
            case e:Exception => logger.error("error in insert event gmt pvuv " + e )
          } finally {
            gmtEventDailySummaryPVUVConnectionInsert.close()
            gmtEventDailySummaryPVUVConnection.close()
            gmtEventDailySummaryPVUVConnectionInsert = null
            gmtEventDailySummaryPVUVConnection = null
          }
        })
      Await.result(Future.sequence(gmtEventDailySummaryPVUVQueryFutures.toList),Duration.Inf)

      val gmtDailySummaryPVUVQueryFutures = sparkSession.sql(PropertyBag.getProperty("gmt_event_daily_summary_pvuv_query","").format(getAddedDate(i))).collect()
        .map(row => Future{
          var gmtDailySummaryPVUVConnection = ConnectionPoolExt.getConnection.orNull
          if(gmtDailySummaryPVUVConnection.isClosed){
            gmtDailySummaryPVUVConnection = ConnectionPoolExt.getConnection.orNull
          }
          gmtDailySummaryPVUVConnection.setAutoCommit(false)
          var gmtDailySummaryPVUVConnectionInsert = gmtDailySummaryPVUVConnection.prepareStatement(PropertyBag.getProperty("event_gmt_daily_summary_insert", ""))
          try{
            var retent_1 = 0l
            var retent_2 = 0l
            var retent_7 = 0l
            var retent_15 = 0l
            var retent_30 = 0l
            var kpi = 0l
            if (row.getInt(0) < 1){
              retent_1 = row.getLong(12)
            }
            if(row.getInt(0) < 2){
              retent_2 = row.getLong(12)
            }
            if(row.getInt(0) < 7){
              retent_7 = row.getLong(12)
            }
            if(row.getInt(0) < 15){
              retent_15 = row.getLong(12)
            }
            if(row.getInt(0) < 30){
              retent_30 = row.getLong(12)
            }

            if(row.getInt(1) == 1){
              kpi = row.getLong(11)
            }else if(row.getInt(1) == 2){
              kpi =row.getLong(12)
            }else if(row.getInt(1) == 91){
              kpi = retent_1
            }else if(row.getInt(1) == 92){
              kpi = retent_2
            }else if(row.getInt(1) == 97){
              kpi = retent_7
            }else if(row.getInt(1) == 915){
              kpi = retent_15
            }else if(row.getInt(1) == 930){
              kpi = retent_30
            }
            //gmt_event_daily_summary
            gmtDailySummaryPVUVConnectionInsert.setInt(2,row.getInt(4))
            gmtDailySummaryPVUVConnectionInsert.setInt(3,row.getInt(5))
            gmtDailySummaryPVUVConnectionInsert.setInt(4,row.getInt(6))
            gmtDailySummaryPVUVConnectionInsert.setString(5,row.getString(7))
            gmtDailySummaryPVUVConnectionInsert.setInt(6,row.getInt(10))
            gmtDailySummaryPVUVConnectionInsert.setLong(7,kpi)
            gmtDailySummaryPVUVConnectionInsert.setLong(9,kpi)
            if(kpi != 0l && (row.getInt(1) < 10)){
              gmtDailySummaryPVUVConnectionInsert.setString(1,row.getString(3))
              gmtDailySummaryPVUVConnectionInsert.setString(8,getMd5(row.getString(3) +
                "||" + row.getInt(4) +
                "||" + row.getInt(5) +
                "||" + row.getInt(6) +
                "||" + row.getString(7) +
                "||" + row.getInt(10) + "||"))
              gmtDailySummaryPVUVConnectionInsert.execute()
              gmtDailySummaryPVUVConnection.commit()
            }else if(kpi !=0l && (row.getInt(1) > 10)){
              gmtDailySummaryPVUVConnectionInsert.setString(1,row.getString(2))
              gmtDailySummaryPVUVConnectionInsert.setString(8,getMd5(row.getString(2) +
                "||" + row.getInt(4) +
                "||" + row.getInt(5) +
                "||" + row.getInt(6) +
                "||" + row.getString(7) +
                "||" + row.getInt(10) + "||"))
              gmtDailySummaryPVUVConnectionInsert.execute()
              gmtDailySummaryPVUVConnection.commit()
            }
            //gmt_event_daily_summary_retent
          } catch {
            case e:Exception => logger.error("error in insert event gmt pvuv " + e )
          } finally {
            gmtDailySummaryPVUVConnectionInsert.close()
            gmtDailySummaryPVUVConnection.close()
            gmtDailySummaryPVUVConnectionInsert = null
            gmtDailySummaryPVUVConnection = null
          }
        })
      Await.result(Future.sequence(gmtDailySummaryPVUVQueryFutures.toList),Duration.Inf)

      val gmtEventDailySummaryRetentQueryFutures = sparkSession.sql(PropertyBag.getProperty("gmt_event_daily_summary_pvuv_query","").format(getAddedDate(i))).collect()
        .map(row => Future{
          var gmtEventDailySummaryRetentConnection = ConnectionPoolExt.getConnection.orNull
          if(gmtEventDailySummaryRetentConnection.isClosed){
            gmtEventDailySummaryRetentConnection = ConnectionPoolExt.getConnection.orNull
          }
          gmtEventDailySummaryRetentConnection.setAutoCommit(false)
          var gmtEventDailySummaryRetentInsert = gmtEventDailySummaryRetentConnection.prepareStatement(PropertyBag.getProperty("gmt_event_daily_summary_retent_insert", ""))
          try{
            var retent_1 = 0l
            var retent_2 = 0l
            var retent_7 = 0l
            var retent_15 = 0l
            var retent_30 = 0l
            if (row.getInt(0) < 1){
              retent_1 = row.getLong(12)
            }
            if(row.getInt(0) < 2){
              retent_2 = row.getLong(12)
            }
            if(row.getInt(0) < 7){
              retent_7 = row.getLong(12)
            }
            if(row.getInt(0) < 15){
              retent_15 = row.getLong(12)
            }
            if(row.getInt(0) < 30){
              retent_30 = row.getLong(12)
            }
            //gmt_event_daily_summary_retent
            if(retent_30 !=0l){
              gmtEventDailySummaryRetentInsert.setString(1,row.getString(2))
              gmtEventDailySummaryRetentInsert.setInt(2,row.getInt(4))
              gmtEventDailySummaryRetentInsert.setInt(3,row.getInt(5))
              gmtEventDailySummaryRetentInsert.setInt(4,row.getInt(6))
              gmtEventDailySummaryRetentInsert.setString(5,row.getString(7))
              gmtEventDailySummaryRetentInsert.setInt(6,row.getInt(8))
              gmtEventDailySummaryRetentInsert.setString(7,row.getString(9))
              gmtEventDailySummaryRetentInsert.setInt(8,row.getInt(10))
              gmtEventDailySummaryRetentInsert.setLong(9,retent_1)
              gmtEventDailySummaryRetentInsert.setLong(10,retent_2)
              gmtEventDailySummaryRetentInsert.setLong(11,retent_7)
              gmtEventDailySummaryRetentInsert.setLong(12,retent_15)
              gmtEventDailySummaryRetentInsert.setLong(13,retent_30)

              gmtEventDailySummaryRetentInsert.setString(14,getMd5(row.getString(2) +
                "||" + row.getInt(4) +
                "||" + row.getInt(5) +
                "||" + row.getInt(6) +
                "||" + row.getString(7) +
                "||" + row.getInt(8) +
                "||" + row.getString(9) +
                "||" + row.getInt(10) + "||"))
              gmtEventDailySummaryRetentInsert.setLong(15,retent_1)
              gmtEventDailySummaryRetentInsert.setLong(16,retent_2)
              gmtEventDailySummaryRetentInsert.setLong(17,retent_7)
              gmtEventDailySummaryRetentInsert.setLong(18,retent_15)
              gmtEventDailySummaryRetentInsert.setLong(19,retent_30)
              gmtEventDailySummaryRetentInsert.execute()
              gmtEventDailySummaryRetentConnection.commit()
            }
          } catch {
            case e:Exception => logger.error("error in insert event gmt pvuv " + e )
          } finally {
            gmtEventDailySummaryRetentInsert.close()
            gmtEventDailySummaryRetentConnection.close()
            gmtEventDailySummaryRetentInsert = null
            gmtEventDailySummaryRetentConnection = null
          }
        })
      Await.result(Future.sequence(gmtEventDailySummaryRetentQueryFutures.toList),Duration.Inf)


      println("recovery data date: ", getAddedDate(i),"; tasks costs ",System.currentTimeMillis() - startTime, " mills")
    }
  }
}
