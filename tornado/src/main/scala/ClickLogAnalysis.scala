import java.sql.PreparedStatement

import caseClass.ClickDataExt
import com.amazonaws.SDKGlobalConfiguration
import joptsimple.OptionParser
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.{immutable, mutable}
/**
  * Created by aaron on 2017/8/14.
  */
object ClickLogAnalysis extends AbstractAnalysis{

  val logger = LoggerFactory.getLogger(this.getClass)
  val parser = new OptionParser
  val logDate = parser.accepts("log-date", "kafka log date to receive")
    .withRequiredArg
    .describedAs("2017-11-11,2017-11-12,2017-11-13")
    .ofType(classOf[java.lang.String])
    .defaultsTo("")

  val tableSchema = Encoders.product[ClickDataExt].schema
  val tempAdTableName = PropertyBag.getProperty("tempClickAdTableNameExt", "tempClickTableExt")
  val adx_uniqueClk_SparkSql =  PropertyBag.getProperty("adx_uniqueClk_SparkSql", "select timezone, getFormatDate(clk_timestamp + timezone*3600) as dated, campaign_id, group_id, aff_id,aff_pub, country_code,province_geoname_id, city_geoname_id, os, device_type, device_make, device_model,algorithm, ip,ua,gaid,idfa,adid, count(*) as gross_cnt from tableName  group by timezone, getFormatDate(clk_timestamp + timezone*3600), campaign_id, group_id, aff_id,aff_pub, country_code,province_geoname_id, city_geoname_id, os, device_type, device_make, device_model,algorithm, ip,ua,gaid,idfa,adid")
  val gmt_uniqueClk_SparkSql = PropertyBag.getProperty("gmt_uniqueClk_SparkSql", "select getFormatDate(clk_timestamp) as dated, campaign_id, group_id, aff_id,aff_pub, country_code, province_geoname_id, city_geoname_id, os, device_type, device_make, device_model,algorithm,ip,ua,gaid,idfa,adid, count(*) as gross_cnt from tableName  group by getFormatDate(clk_timestamp), campaign_id, group_id, aff_id,aff_pub, country_code, province_geoname_id, city_geoname_id, os, device_type, device_make, device_model,algorithm,ip,ua,gaid,idfa,adid")
  val adx_insert_PvUvSql =PropertyBag.getProperty("adx_insert_PvUvSql","INSERT INTO adx_clk_daily_summary (timezone, dated,unionk, campaign_id, group_id, aff_id, aff_pub, country_code, province_geoname_id, city_geoname_id, os, device_type, device_make, device_model,algorithm , gross_cnt, uniq_cnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE gross_cnt =gross_cnt+?, uniq_cnt =uniq_cnt+?")
  val gmt_insert_PvUvSql =PropertyBag.getProperty("gmt_insert_PvUvSql","INSERT INTO gmt_clk_daily_summary (dated, unionk,campaign_id, group_id, aff_id, aff_pub, country_code, province_geoname_id, city_geoname_id, os, device_type, device_make, device_model,algorithm , gross_cnt,uniq_cnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE gross_cnt =gross_cnt+?, uniq_cnt =uniq_cnt+?")
  val adx_daliy_summary_insertSql = PropertyBag.getProperty("adx_daliy_summary_insertSql","INSERT INTO adx_daily_summary (timezone,dated,unionk,campaign_id, group_id, aff_id, aff_pub,algorithm,gross_clk_cnt, uniq_clk_cnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE gross_clk_cnt =gross_clk_cnt+?, uniq_clk_cnt =uniq_clk_cnt+?")
  val gmt_daily_summary_insertSql = PropertyBag.getProperty("gmt_daily_summary_insertSql","INSERT INTO gmt_daily_summary (dated,unionk,campaign_id, group_id, aff_id, aff_pub,algorithm,gross_clk_cnt, uniq_clk_cnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE gross_clk_cnt =gross_clk_cnt+?, uniq_clk_cnt =uniq_clk_cnt+?")
  var adxPvUvMap:mutable.Map[String,Array[Long]] = mutable.Map()
  var gmtPvUvMap:mutable.Map[String,Array[Long]] = mutable.Map()
  var adxSummaryMap:mutable.Map[String,Array[Long]] = mutable.Map()
  var gmtSummaryMap:mutable.Map[String,Array[Long]] = mutable.Map()
  val adxClickFilterMap: immutable.Map[String,Any] =immutable.Map(
    "timezone" -> 0.0f,
    "dated" -> "1970-01-01",
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
  val gmtClickFilterMap: immutable.Map[String,Any] =immutable.Map(
    "dated" -> "1970-01-01",
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

  def main(args: Array[String]): Unit = {
    val options = parser.parse(args : _*)
    val conf = new SparkConf().setAppName("ClickLogAnalyser")

    val sc = new SparkContext(conf)

    System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")

    sc.hadoopConfiguration.set("fs.s3a.access.key", PropertyBag.getProperty("aws.access_key", ""))

    sc.hadoopConfiguration.set("fs.s3a.secret.key", PropertyBag.getProperty("aws.secret_key", ""))

    sc.hadoopConfiguration.set("fs.s3a.endpoint", PropertyBag.getProperty("aws.s3_endpoint", "s3-ap-southeast-1.amazonaws.com"))

    var clickDataFrom = ""
    var currentDate = ""
    if (options.valueOf(logDate) != "") {
      currentDate = options.valueOf(logDate).split(",")(1)
      clickDataFrom = PropertyBag.getProperty("click_data_from","").format(options.valueOf(logDate).split(",")(0).replace("-",""),options.valueOf(logDate).split(",")(1).replace("-",""),options.valueOf(logDate).split(",")(2).replace("-","")) +"*"
    } else {
      currentDate = getAddedDate(-1)
      clickDataFrom = PropertyBag.getProperty("click_data_from","").format(getAddedDate(-2).replace("-",""),getAddedDate(-1).replace("-",""),getAddedDate(0).replace("-","")) +"*"
    }


    val clickRdd = sc.textFile(clickDataFrom)

    clickLogPvUV(clickRdd)

    def clickLogPvUV(RDD: RDD[String]): Unit = {
      val sc = RDD.sparkContext.getConf
      val sparkSql = SparkSession.builder.config(sc).getOrCreate()
      if (!RDD.isEmpty()) {
        sparkSql.udf.register("getFormatDate", getFormatDate _)
        try {
          val addf = sparkSql.read.schema(tableSchema).json(RDD)
          addf.createOrReplaceTempView(tempAdTableName)
//          addf.show(false)

          val adxDailyResultDF = sparkSql.sql(adx_uniqueClk_SparkSql.replace("tableName", tempAdTableName).format(currentDate)).repartition(1).na.fill(adxClickFilterMap)
//          adxDailyResultDF.show(false)
          adxDailyResultDF.foreachPartition(partitionOfRecord => {
            if (!partitionOfRecord.isEmpty) {
              doAdxPvUVCollection(partitionOfRecord)
            }
          })

          val gmtDailyResultDF = sparkSql.sql(gmt_uniqueClk_SparkSql.replace("tableName", tempAdTableName).format(currentDate)).repartition(1).na.fill(gmtClickFilterMap)
          gmtDailyResultDF.foreachPartition(partitionOfRecord => {
            if (!partitionOfRecord.isEmpty) {
              doGmtPvUVCollection(partitionOfRecord)
            }
          })
        } catch {
          case exception: Exception =>
            logger.error("Error in clickLogUV: " + ExceptionUtils.getStackTrace(exception))

        } finally {
        }
      }
    }

    @throws(classOf[Exception])
    def doAdxPvUVCollection(dataList: Iterator[Row]): Unit = {
      var key = new StringBuilder
      var array = new Array[Long](2)
      if (dataList != null) {
        try {
          dataList.foreach(data => {
            key = new StringBuilder
            array = new Array[Long](2)
            key.append(data.getFloat(0) + "@@@" + data.getString(1) + "@@@" + data.getInt(2) + "@@@" + data.getInt(3) + "@@@" + data.getInt(4) + "@@@" + data.getString(5) + "@@@" + data.getString(6) + "@@@" + data.getInt(7) + "@@@" + data.getInt(8) + "@@@" + data.getString(9) + "@@@" + data.getString(10) + "@@@" + data.getString(11) + "@@@" + data.getString(12)+ "@@@" + data.getInt(13))

              if (!adxPvUvMap.contains(key.toString())) {
                array(0) +=data.getLong(19)
                array(1) += 1
                adxPvUvMap += (key.toString() -> array)
              } else {
                array = adxPvUvMap(key.toString())
                array(0) +=data.getLong(19)
                array(1) += 1
                adxPvUvMap.update(key.toString(), array)
              }
            key = null
            array = null
          }
          )
        } catch {
          case exception: Exception =>
            logger.error("Error in ClickLogAnalyser doAdxUVCollection: " + exception.getMessage + "\n-----------------------\n" + ExceptionUtils.getStackTrace(exception) + "\n-----------------------------")
            throw exception

        } finally {
          doInsertAdxIncrClickUV(adxPvUvMap)
        }
      }
    }

    def doInsertAdxIncrClickUV(adxPvUvMap: mutable.Map[String, Array[Long]]) = {
      val connection = ConnectionPoolExt.getConnection.orNull
      connection.setAutoCommit(false)
      val adxInsert: PreparedStatement = connection.prepareStatement(adx_insert_PvUvSql)
      var i =0
      var array = new Array[Long](2)
      var adxMd5 = new StringBuilder
      var adxSummaryKey = new StringBuilder
      var adxSummaryArray = new Array[Long](2)

      if (!adxPvUvMap.isEmpty) {
        try {
          adxPvUvMap.foreach(p => {
            val keyArray = p._1.split("@@@")
            if (keyArray.length == 14) {
              adxSummaryKey = new StringBuilder
              adxSummaryArray = new Array[Long](2)
              adxMd5 = new StringBuilder
              adxSummaryKey.append(keyArray.apply(0).toFloat+"@@@"+keyArray.apply(1)+"@@@"+keyArray.apply(2).toInt+"@@@"+keyArray.apply(3).toInt+"@@@"+keyArray.apply(4).toInt+"@@@"+keyArray.apply(5)+"@@@"+keyArray.apply(13).toInt)
              adxMd5.append(keyArray.apply(0).toFloat+"||"+keyArray.apply(1)+"||"+keyArray.apply(2).toInt+"||"+keyArray.apply(3).toInt+"||"+keyArray.apply(4).toInt+"||"+keyArray.apply(5)+"||"+keyArray.apply(6)+"||"+keyArray.apply(7).toInt+"||"+
                keyArray.apply(8).toInt+"||"+keyArray.apply(9)+"||"+keyArray.apply(10)+"||"+keyArray.apply(11)+"||"+keyArray.apply(12)+"||"+keyArray.apply(13).toInt + "||")
              i +=1
              array = new Array[Long](2)
              array = p._2
              adxInsert.setFloat(1, keyArray.apply(0).toFloat)
              adxInsert.setString(2, keyArray.apply(1))
              adxInsert.setString(3,getMd5(adxMd5.toString()))
              adxInsert.setInt(4, keyArray.apply(2).toInt)
              adxInsert.setInt(5, keyArray.apply(3).toInt)
              adxInsert.setInt(6, keyArray.apply(4).toInt)
              adxInsert.setString(7, keyArray.apply(5))
              adxInsert.setString(8, keyArray.apply(6))
              adxInsert.setInt(9, keyArray.apply(7).toInt)
              adxInsert.setInt(10, keyArray.apply(8).toInt)
              adxInsert.setString(11, if (null == keyArray.apply(9)) ""  else keyArray.apply(9))
              adxInsert.setString(12, if (null == keyArray.apply(10)) "" else keyArray.apply(10))
              adxInsert.setString(13, if (null == keyArray.apply(11)) "" else keyArray.apply(11))
              adxInsert.setString(14, if (null == keyArray.apply(12)) "" else keyArray.apply(12))
              adxInsert.setInt(15, keyArray.apply(13).toInt)
              adxInsert.setLong(16, array(0))
              adxInsert.setLong(17, array(1))
              adxInsert.setLong(18, array(0))
              adxInsert.setLong(19, array(1))
              adxInsert.addBatch()

              if (!adxSummaryMap.contains(adxSummaryKey.toString())) {
                adxSummaryArray(0) +=array(0)
                adxSummaryArray(1) += array(1)
                adxSummaryMap += (adxSummaryKey.toString() -> adxSummaryArray)
              } else {
                adxSummaryArray = adxSummaryMap(adxSummaryKey.toString())
                adxSummaryArray(0) +=array(0)
                adxSummaryArray(1) += array(1)
                adxSummaryMap.update(adxSummaryKey.toString(), adxSummaryArray)
              }
              array = null
              adxSummaryKey = null
              adxSummaryArray = null

              if (i%1000 == 0){
                adxInsert.executeBatch()
                connection.commit()
                adxInsert.clearBatch()
                i =0
              }
            }
          }
          )
        } catch {
          case exception: Exception =>
            logger.error("Error in ClickLogAnalyser doInsertAdxIncrClickUV: " + exception.getMessage + "\n-----------------------\n" + ExceptionUtils.getStackTrace(exception) + "\n-----------------------------")
            throw exception

        } finally {
          if (adxInsert != null) {
            adxInsert.executeBatch()
            connection.commit()
            adxInsert.clearBatch()
            adxInsert.close()
          }
          if (connection != null) {
            connection.close()
          }
          if (!adxPvUvMap.isEmpty) {
            adxPvUvMap.clear()
          }
          doInsertAdxSummary(adxSummaryMap)
        }
      }
    }

    def doInsertAdxSummary(adxSummaryMap: mutable.Map[String, Array[Long]]) = {
      val connection = ConnectionPoolExt.getConnection.orNull
      connection.setAutoCommit(false)
      val adxInsert: PreparedStatement = connection.prepareStatement(adx_daliy_summary_insertSql)
      var i =0
      var array = new Array[Long](2)
      var adxMd5 = new StringBuilder
     if(!adxSummaryMap.isEmpty){
       try {
         adxSummaryMap.foreach(p => {
           val keyArray = p._1.split("@@@")
           if (keyArray.length == 7) {
             adxMd5 = new StringBuilder
             adxMd5.append(keyArray.apply(0).toFloat+"||"+keyArray.apply(1)+"||"+keyArray.apply(2).toInt+"||"+keyArray.apply(3).toInt+"||"+keyArray.apply(4).toInt+"||"+keyArray.apply(5)+"||"+keyArray.apply(6).toInt+"||")
             i +=1
             array = new Array[Long](2)
             array = p._2
             adxInsert.setFloat(1, keyArray.apply(0).toFloat)
             adxInsert.setString(2, keyArray.apply(1))
             adxInsert.setString(3,getMd5(adxMd5.toString()))
             adxInsert.setInt(4, keyArray.apply(2).toInt)
             adxInsert.setInt(5, keyArray.apply(3).toInt)
             adxInsert.setInt(6, keyArray.apply(4).toInt)
             adxInsert.setString(7, keyArray.apply(5))
             adxInsert.setInt(8, keyArray.apply(6).toInt)
             adxInsert.setLong(9, array(0))
             adxInsert.setLong(10, array(1))
             adxInsert.setLong(11, array(0))
             adxInsert.setLong(12, array(1))
             adxInsert.addBatch()
             array = null

             if (i%1000 == 0){
               adxInsert.executeBatch()
               connection.commit()
               adxInsert.clearBatch()
               i =0
             }
           }
         }
         )
       } catch {
         case exception: Exception =>
           logger.error("Error in ClickLogAnalyser doInsertAdxSummary: " + exception.getMessage + "\n-----------------------\n" + ExceptionUtils.getStackTrace(exception) + "\n-----------------------------")
           throw exception

       } finally {
         if (adxInsert != null) {
           adxInsert.executeBatch()
           connection.commit()
           adxInsert.clearBatch()
           adxInsert.close()
         }
         if (connection != null) {
           connection.close()
         }
         if (!adxSummaryMap.isEmpty) {
           adxSummaryMap.clear()
         }
       }
      }
    }

    @throws(classOf[Exception])
    def doGmtPvUVCollection(dataList: Iterator[Row]): Unit = {
      var key = new StringBuilder
      var array = new Array[Long](2)

      if (dataList != null) {
        try {
          dataList.foreach(data => {
            key = new StringBuilder
            array = new Array[Long](2)
            key.append(data.getString(0) + "@@@" + data.getInt(1) + "@@@" + data.getInt(2) + "@@@" + data.getInt(3) + "@@@" + data.getString(4) + "@@@" + data.getString(5) + "@@@" + data.getInt(6) + "@@@" + data.getInt(7) + "@@@" + data.getString(8) + "@@@" + data.getString(9) + "@@@" + data.getString(10) + "@@@" + data.getString(11) +"@@@"+data.getInt(12))

            if (!gmtPvUvMap.contains(key.toString())) {
              array(0) += data.getLong(18)
              array(1) += 1
              gmtPvUvMap += (key.toString() -> array)
            } else {
              array = gmtPvUvMap(key.toString())
              array(0) += data.getLong(18)
              array(1) += 1
              gmtPvUvMap.update(key.toString(), array)
            }

            key = null
            array =null
          }
          )
        } catch {
          case exception: Exception =>
            logger.error("Error in ClickLogAnalyser doGmtUVCollection: " + exception.getMessage + "\n-----------------------\n" + ExceptionUtils.getStackTrace(exception) + "\n-----------------------------")
            throw exception

        } finally {
          doInsertgmtIncrClickUV(gmtPvUvMap)
        }
      }
    }

    def doInsertgmtIncrClickUV(gmtPvUvMap: mutable.Map[String, Array[Long]]) = {
      val connection = ConnectionPoolExt.getConnection.orNull
      connection.setAutoCommit(false)
      val gmtInsert: PreparedStatement = connection.prepareStatement(gmt_insert_PvUvSql)
      var i =0
      var array = new Array[Long](2)
      var gmtMd5 = new StringBuilder
      var gmtSummaryKey = new StringBuilder
      var gmtSummaryArray = new Array[Long](2)

      if (!gmtPvUvMap.isEmpty) {
        try {
          gmtPvUvMap.foreach(p => {
            val keyArray = p._1.split("@@@")
            if (keyArray.length == 13) {
              gmtMd5 = new StringBuilder
              gmtSummaryKey = new StringBuilder
              gmtSummaryArray = new Array[Long](2)
              gmtSummaryKey.append(keyArray.apply(0)+"@@@"+keyArray.apply(1).toInt+"@@@"+keyArray.apply(2).toInt+"@@@"+keyArray.apply(3).toInt+"@@@"+keyArray.apply(4)+"@@@"+keyArray.apply(12).toInt)
              gmtMd5.append(keyArray.apply(0)+"||"+keyArray.apply(1).toInt+"||"+keyArray.apply(2).toInt+"||"+keyArray.apply(3).toInt+"||"+keyArray.apply(4)+"||"+keyArray.apply(5)
              +"||"+keyArray.apply(6).toInt+"||"+keyArray.apply(7).toInt+"||"+keyArray.apply(8)+"||"+keyArray.apply(9)+"||"+keyArray.apply(10)+"||"+keyArray.apply(11)+"||"+keyArray.apply(12).toInt+"||")
              i +=1
              array = new Array[Long](2)
              array = p._2
              gmtInsert.setString(1, keyArray.apply(0))
              gmtInsert.setString(2,getMd5(gmtMd5.toString()))
              gmtInsert.setInt(3, keyArray.apply(1).toInt)
              gmtInsert.setInt(4, keyArray.apply(2).toInt)
              gmtInsert.setInt(5, keyArray.apply(3).toInt)
              gmtInsert.setString(6, keyArray.apply(4))
              gmtInsert.setString(7, keyArray.apply(5))
              gmtInsert.setInt(8, keyArray.apply(6).toInt)
              gmtInsert.setInt(9, keyArray.apply(7).toInt)
              gmtInsert.setString(10, if (null == keyArray.apply(8)) ""  else keyArray.apply(8))
              gmtInsert.setString(11, if (null == keyArray.apply(9)) "" else keyArray.apply(9))
              gmtInsert.setString(12, if (null == keyArray.apply(10)) "" else keyArray.apply(10))
              gmtInsert.setString(13, if (null == keyArray.apply(11)) "" else keyArray.apply(11))
              gmtInsert.setInt(14, keyArray.apply(12).toInt)
              gmtInsert.setLong(15, array(0))
              gmtInsert.setLong(16, array(1))
              gmtInsert.setLong(17, array(0))
              gmtInsert.setLong(18, array(1))
              gmtInsert.addBatch()

              if (!gmtSummaryMap.contains(gmtSummaryKey.toString())) {
                gmtSummaryArray(0) +=array(0)
                gmtSummaryArray(1) += array(1)
                gmtSummaryMap += (gmtSummaryKey.toString() -> gmtSummaryArray)
              } else {
                gmtSummaryArray = gmtSummaryMap(gmtSummaryKey.toString())
                gmtSummaryArray(0) +=array(0)
                gmtSummaryArray(1) += array(1)
                gmtSummaryMap.update(gmtSummaryKey.toString(), gmtSummaryArray)
              }
              array =null
              gmtSummaryKey = null
              gmtSummaryArray = null

              if (i%1000 == 0){
                gmtInsert.executeBatch()
                connection.commit()
                gmtInsert.clearBatch()
                i =0
              }
            }
          }
          )
        } catch {
          case exception: Exception =>
            logger.error("Error in ClickLogAnalyser doInsertgmtIncrClickUV: " + exception.getMessage + "\n-----------------------\n" + ExceptionUtils.getStackTrace(exception) + "\n-----------------------------")
            throw exception

        } finally {
          if (gmtInsert != null) {
            gmtInsert.executeBatch()
            connection.commit()
            gmtInsert.clearBatch()
            gmtInsert.close()
          }
          if (connection != null) {
            connection.close()
          }
          if (!gmtPvUvMap.isEmpty) {
            gmtPvUvMap.clear()
          }
          doInsertGmtSummary(gmtSummaryMap)
        }
      }
    }

    def doInsertGmtSummary(gmtSummaryMap: mutable.Map[String, Array[Long]]) = {
      val connection = ConnectionPoolExt.getConnection.orNull
      connection.setAutoCommit(false)
      val gmtInsert: PreparedStatement = connection.prepareStatement(gmt_daily_summary_insertSql)
      var i =0
      var array = new Array[Long](2)
      var gmtMd5 = new StringBuilder
      if(!gmtSummaryMap.isEmpty){
        try {
          gmtSummaryMap.foreach(p => {
            val keyArray = p._1.split("@@@")
            if (keyArray.length == 6) {
              gmtMd5 = new StringBuilder
              gmtMd5.append(keyArray.apply(0)+"||"+keyArray.apply(1).toInt+"||"+keyArray.apply(2).toInt+"||"+keyArray.apply(3).toInt+"||"+keyArray.apply(4)+"||"+keyArray.apply(5).toInt+"||")
              i +=1
              array = new Array[Long](2)
              array = p._2
              gmtInsert.setString(1, keyArray.apply(0))
              gmtInsert.setString(2,getMd5(gmtMd5.toString()))
              gmtInsert.setInt(3, keyArray.apply(1).toInt)
              gmtInsert.setInt(4, keyArray.apply(2).toInt)
              gmtInsert.setInt(5, keyArray.apply(3).toInt)
              gmtInsert.setString(6, keyArray.apply(4))
              gmtInsert.setInt(7, keyArray.apply(5).toInt)
              gmtInsert.setLong(8, array(0))
              gmtInsert.setLong(9, array(1))
              gmtInsert.setLong(10, array(0))
              gmtInsert.setLong(11, array(1))
              gmtInsert.addBatch()
              array =null

              if (i%1000 == 0){
                gmtInsert.executeBatch()
                connection.commit()
                gmtInsert.clearBatch()
                i =0
              }
            }
          }
          )
        } catch {
          case exception: Exception =>
            logger.error("Error in ClickLogAnalyser doInsertGmtSummary: " + exception.getMessage + "\n-----------------------\n" + ExceptionUtils.getStackTrace(exception) + "\n-----------------------------")
            throw exception

        } finally {
          if (gmtInsert != null) {
            gmtInsert.executeBatch()
            connection.commit()
            gmtInsert.clearBatch()
            gmtInsert.close()
          }
          if (connection != null) {
            connection.close()
          }
          if (!gmtSummaryMap.isEmpty) {
            gmtSummaryMap.clear()
          }
        }
      }
    }
    println("recovery data date: ", currentDate)
  }

}
