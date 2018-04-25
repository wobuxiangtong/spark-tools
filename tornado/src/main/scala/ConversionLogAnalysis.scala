import java.util.concurrent.Executors

import caseClass.ConversionLog
import com.amazonaws.SDKGlobalConfiguration
import joptsimple.OptionParser
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object ConversionLogAnalysis  extends AbstractAnalysis{
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
    val schema = Encoders.product[ConversionLog].schema
    sparkSession.udf.register("getFormatDate",getFormatDate _)
    sparkSession.udf.register("validGroupCount",new ValidGroupCount)
    sparkSession.udf.register("invalidGroupCount",new InvalidGroupCount)
    sparkSession.udf.register("notifyGroupCount",new NotifyGroupCount)
    sparkSession.udf.register("profitSum",new ProfitSum)
    sparkSession.udf.register("tacSum",new TacSum)

    val endDateRange = getDateRange(options.valueOf(endDate))
    var startDateRange = getDateRange(options.valueOf(startDate))

    for(i <- -startDateRange to -endDateRange){
      val fillMap:Map[String,Any] = Map(
        "click_id"->"",
        "timezone"-> 0.0f,
        "conv_timestamp"->0,
        "campaign_id" -> 0,
        "group_id" -> 0,
        "aff_id" -> 0,
        "aff_pub" -> "",
        "income" -> 0.0f,
        "income_currency" -> "",
        "payout" -> 0.0f,
        "payout_currency" -> "",
        "algorithm" -> 0,
        "invalid_type" -> "",
        "forwarded" -> 0,
        "net_profit" -> 0.0,
        "tac" -> 0.0)
      val jsonDataFrame = sparkSession.read.schema(schema).json(PropertyBag.getProperty("conv_data_from","").format(getAddedDate(i -1).replace("-",""),getAddedDate(i).replace("-",""),getAddedDate(i+1).replace("-","")) +"*").na.fill(fillMap)
      jsonDataFrame.createOrReplaceTempView("conv_init")

      val initQuery = sparkSession.sql(PropertyBag.getProperty("conv_init_query",""))
      initQuery.createOrReplaceTempView("conv_summary")

      val adxConvDailySummaryQuery = sparkSession.sql(PropertyBag.getProperty("adx_conv_daily_summary_query","").format(getAddedDate(i)))
      adxConvDailySummaryQuery.createOrReplaceTempView("adx_conv_daily_summary")
      val adxConvDailySummaryQueryFutures = adxConvDailySummaryQuery.collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("adx_conv_daily_summary_insert", ""))
          try{
            insert.setFloat(1, row.getFloat(0))
            insert.setString(2, row.getString(1))
            insert.setInt(3, row.getInt(2))
            insert.setInt(4, row.getInt(3))
            insert.setInt(5, row.getInt(4))
            insert.setString(6, row.getString(5))
            insert.setFloat(7, row.getFloat(6))
            insert.setString(8, row.getString(7))
            insert.setFloat(9, row.getFloat(8))
            insert.setString(10, row.getString(9))
            insert.setInt(11,row.getInt(10))
            insert.setLong(12, row.getLong(11))
            insert.setInt(13, row.getInt(12))
            insert.setInt(14, row.getInt(13))
            //            println(row.getFloat(0)+"||"+row.getString(1)+"||"+row.getInt(2)+"||"+row.getInt(3)+"||"+row.getInt(4)+"||"+row.getString(5)+"||"+row.getFloat(6)+"||"+row.getString(7)+"||"+row.getFloat(8)+"||"+row.getString(9)+"||"+row.getInt(10)+"||")
            insert.setString(15,getMd5("%1.1f".format(row.getFloat(0))+"||"+row.getString(1)+"||"+row.getInt(2)+"||"+row.getInt(3)+"||"+row.getInt(4)+"||"+row.getString(5)+"||"+"%1.2f".format(row.getFloat(6))+"||"+row.getString(7)+"||"+"%1.2f".format(row.getFloat(8))+"||"+row.getString(9)+"||"+row.getInt(10)+"||"))
            insert.setLong(16, row.getLong(11))
            insert.setInt(17, row.getInt(12))
            insert.setInt(18, row.getInt(13))
            insert.execute()
            connection.commit()
          } catch {
            case except:Exception => logger.error("error in insert adx_conv_daily_summary " + except )
          } finally {
            insert.close()
            insert = null
            connection.close()
            connection = null
          }
        })
      Await.result(Future.sequence(adxConvDailySummaryQueryFutures.toList),Duration.Inf)

      val gmtConvDailySummaryQuery = sparkSession.sql(PropertyBag.getProperty("gmt_conv_daily_summary_query","").format(getAddedDate(i)))
      gmtConvDailySummaryQuery.createOrReplaceTempView("gmt_conv_daily_summary")
      val gmtConvDailySummaryQueryFutures = gmtConvDailySummaryQuery.collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("gmt_conv_daily_summary_insert", ""))
          try {
            insert.setString(1, row.getString(0))
            insert.setInt(2, row.getInt(1))
            insert.setInt(3, row.getInt(2))
            insert.setInt(4, row.getInt(3))
            insert.setString(5, row.getString(4))
            insert.setFloat(6, row.getFloat(5))
            insert.setString(7, row.getString(6))
            insert.setFloat(8, row.getFloat(7))
            insert.setString(9, row.getString(8))
            insert.setInt(10,row.getInt(9))
            insert.setLong(11, row.getLong(10))
            insert.setInt(12, row.getInt(11))
            insert.setInt(13, row.getInt(12))
            insert.setString(14,getMd5(row.getString(0)+"||"+row.getInt(1)+"||"+row.getInt(2)+"||"+row.getInt(3)+"||"+row.getString(4)+"||"+"%1.2f".format(row.getFloat(5))+"||"+row.getString(6)+"||"+"%1.2f".format(row.getFloat(7))+"||"+row.getString(8)+"||"+row.getInt(9)+"||"))
            insert.setLong(15, row.getLong(10))
            insert.setInt(16, row.getInt(11))
            insert.setInt(17, row.getInt(12))
            insert.execute()
            connection.commit()
          } catch {
            case except:Exception => logger.error("error in insert gmt_conv_daily_summary ",except)
          } finally {
            insert.close()
            insert = null
            connection.close()
            connection = null
          }
        })
      Await.result(Future.sequence(gmtConvDailySummaryQueryFutures.toList),Duration.Inf)

      val adxInvalidConvDailySummaryQueryFutures = sparkSession.sql(PropertyBag.getProperty("adx_invalid_conv_daily_summary_query","").format(getAddedDate(i))).collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("adx_invalid_conv_daily_summary_insert", ""))
          try {
            insert.setFloat(1, row.getFloat(0))
            insert.setString(2, row.getString(1))
            insert.setInt(3, row.getInt(2))
            insert.setInt(4, row.getInt(3))
            insert.setInt(5, row.getInt(4))
            insert.setString(6, row.getString(5))
            insert.setString(7, row.getString(6))
            insert.setInt(8, row.getInt(7))
            insert.setLong(9, row.getLong(8))
            insert.setString(10,getMd5("%1.1f".format(row.getFloat(0))+"||"+row.getString(1)+"||"+row.getInt(2)+"||"+row.getInt(3)+"||"+row.getInt(4)+"||"+row.getString(5)+"||"+row.getString(6)+"||" + row.getInt(7)+ "||"))
            insert.setLong(11, row.getLong(8))
            insert.execute()
            connection.commit()
          } catch {
            case except:Exception => logger.error("error in insert adx_invalid_conv_daily_summary ",except)
          } finally {
            insert.close()
            insert = null
            connection.close()
            connection = null
          }
        })
      Await.result(Future.sequence(adxInvalidConvDailySummaryQueryFutures.toList),Duration.Inf)

      val convAdxDailySummaryQueryFutures = sparkSession.sql(PropertyBag.getProperty("conv_adx_daily_summary_query","")).collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var insert = connection.prepareStatement(PropertyBag.getProperty("conv_adx_daily_summary_insert", ""))
          try {
            insert.setFloat(1, row.getFloat(0)) //timezone
            insert.setString(2, row.getString(1)) //dated
            insert.setInt(3, row.getInt(2)) //campaign_id
            insert.setInt(4, row.getInt(3)) //group_id
            insert.setInt(5, row.getInt(4)) //aff_id
            insert.setString(6, row.getString(5)) //aff_pub
            insert.setInt(7,row.getInt(6)) //algorithm
            insert.setLong(8, row.getLong(7)) //gross_conv_cnt
            insert.setLong(9, row.getLong(8)) //valid_conv_cnt
            insert.setLong(10, row.getLong(9)) //notify_conv_cnt
            insert.setDouble(11, row.getDouble(10)) //net_profit
            insert.setDouble(12, row.getDouble(11)) //tac
            insert.setString(13,getMd5("%1.1f".format(row.getFloat(0))+"||"+row.getString(1)+"||"+row.getInt(2)+"||"+row.getInt(3)+"||"+row.getInt(4)+"||"+row.getString(5)+"||"+row.getInt(6)+"||"))
            insert.setLong(14, row.getLong(7))
            insert.setLong(15, row.getLong(8))
            insert.setLong(16, row.getLong(9))
            insert.setDouble(17, row.getDouble(10))
            insert.setDouble(18, row.getDouble(11))
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
      Await.result(Future.sequence(convAdxDailySummaryQueryFutures.toList),Duration.Inf)

      val convGmtDailySummaryQueryFutures = sparkSession.sql(PropertyBag.getProperty("conv_gmt_daily_summary_query","")).collect()
        .map(row => Future{
          var connection = ConnectionPoolExt.getConnection.orNull
          if(connection.isClosed){
            connection = ConnectionPoolExt.getConnection.orNull
          }
          connection.setAutoCommit(false)
          var convGmtDailySummaryInsert = connection.prepareStatement(PropertyBag.getProperty("conv_gmt_daily_summary_insert", ""))
          try {
            convGmtDailySummaryInsert.setString(1, row.getString(0)) //dated
            convGmtDailySummaryInsert.setInt(2, row.getInt(1)) //campaign_id
            convGmtDailySummaryInsert.setInt(3, row.getInt(2)) //group_id
            convGmtDailySummaryInsert.setInt(4, row.getInt(3)) //aff_id
            convGmtDailySummaryInsert.setString(5, row.getString(4)) //aff_pub
            convGmtDailySummaryInsert.setInt(6,row.getInt(5)) //algorithm
            convGmtDailySummaryInsert.setLong(7, row.getLong(6)) //gross_conv_cnt
            convGmtDailySummaryInsert.setLong(8, row.getLong(7)) //valid_conv_cnt
            convGmtDailySummaryInsert.setLong(9, row.getLong(8)) //notify_conv_cnt
            convGmtDailySummaryInsert.setDouble(10, row.getDouble(9)) //net_profit
            convGmtDailySummaryInsert.setDouble(11, row.getDouble(10)) //tac
            convGmtDailySummaryInsert.setString(12,getMd5(row.getString(0)+"||"+row.getInt(1)+"||"+row.getInt(2)+"||"+row.getInt(3)+"||"+row.getString(4)+"||"+row.getInt(5) +"||"))
            convGmtDailySummaryInsert.setLong(13, row.getLong(6))
            convGmtDailySummaryInsert.setLong(14, row.getLong(7))
            convGmtDailySummaryInsert.setLong(15, row.getLong(8))
            convGmtDailySummaryInsert.setDouble(16,row.getDouble(9))
            convGmtDailySummaryInsert.setDouble(17,row.getDouble(10))
            convGmtDailySummaryInsert.execute()
            connection.commit()
          } catch {
            case except:Exception => logger.error("error in insert gmt_daily_summary ",except)
          } finally {
            convGmtDailySummaryInsert.close()
            convGmtDailySummaryInsert = null
            connection.close()
            connection = null
          }
        })
      Await.result(Future.sequence(convGmtDailySummaryQueryFutures.toList),Duration.Inf)

      println("recovery data date: ", getAddedDate(i),"; tasks costs ",System.currentTimeMillis() - startTime, " mills")
    }
  }
}
