import com.alibaba.fastjson.JSON
import com.twitter.algebird.{HLL, HyperLogLogMonoid}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


object DemandStreamAnalysis extends AbstractAnalysis{

    private val logger = LoggerFactory.getLogger(this.getClass)
  @transient
  private val producer = ProduceRowMsg.getProduce
  //  val parser = new OptionParser
  //  private val batchDuration = parser.accepts("batch-duration", "spark streaimg batchs duration")
  //    .withRequiredArg
  //    .describedAs("time(second)")
  //    .ofType(classOf[java.lang.Integer])
  //    .defaultsTo(20)
  //  private val checkpointDuration = parser.accepts("checkpoint-duration", "spark streaimg checkpoint duration")
  //    .withRequiredArg
  //    .describedAs("time(second)")
  //    .ofType(classOf[java.lang.Integer])
  //    .defaultsTo(200)
  //  private val timeoutDuration = parser.accepts("timeout-duration", "spark streaimg state timeout duration")
  //    .withRequiredArg
  //    .describedAs("time(hours)")
  //    .ofType(classOf[java.lang.Integer])
  //    .defaultsTo(24)
  def main(args: Array[String]): Unit = {
    val checkPointDir = PropertyBag.getProperty("demand.checkpoint.dir", "")
    val ssc = StreamingContext.getOrCreate(checkPointDir,functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("Tornado Demand Stream")//.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val checkPointDir = PropertyBag.getProperty("demand.checkpoint.dir", "")
    val ssc = new StreamingContext(sc, Seconds(20))
    if(!checkPointDir.isEmpty){
      ssc.checkpoint(checkPointDir)
    }

    val subscribe = {
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> PropertyBag.getProperty("kafka.broker.connect", ""),
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "tornado-demand-A",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val topics = Array("tornado_clks","tornado_imps")
      Subscribe[String, String](topics, kafkaParams)
    }
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      subscribe
    )
    incrPvUv(stream)
    ssc
  }
  def incrPvUv(stream: InputDStream[ConsumerRecord[String, String]]):Unit = {
    stream.flatMap(
      x => {
        try{

          var data = JSON.parseObject(x.value().toString)
          var topic = "imps"

          if(x.topic() == "tornado_clks"){
            topic = "clks"
            data.put("timestamp",data.getOrDefault("clickts","0"))
          }else{
            data.put("timestamp",data.getOrDefault("pixelts","0"))
          }

          if(data.getJSONObject("app") != null){
            data.put("inventory_domain",data.getJSONObject("app").getOrDefault("domain",""))
            data.put("app_bundle",data.getJSONObject("app").getOrDefault("bundle",""))
            if(data.getJSONObject("app").getJSONObject("publisher") != null){
              data.put("publisher_domain",data.getJSONObject("app").getJSONObject("publisher").getOrDefault("domain",""))
            }
          }else if(data.getJSONObject("site") != null){
            data.put("inventory_domain",data.getJSONObject("site").getOrDefault("domain",""))
            if(data.getJSONObject("site").getJSONObject("publisher") != null){
              data.put("publisher_domain",data.getJSONObject("site").getJSONObject("publisher").getOrDefault("domain",""))
            }
          }
          if(data.getJSONObject("geo") != null){
            data.put("country_code",data.getJSONObject("geo").getOrDefault("cc",""))
            data.put("province_geonameid",data.getJSONObject("geo").getOrDefault("pgeonid","0"))
            data.put("city_geonameid",data.getJSONObject("geo").getOrDefault("cgeonid","0"))
          }
          if(data.getJSONObject("user") != null){
            data.put("gender",data.getJSONObject("user").getOrDefault("gender",""))
          }
          if(data.getJSONObject("device") != null){
            data.put("carrier",data.getJSONObject("device").getOrDefault("carrier",""))
            data.put("device_make",data.getJSONObject("device").getOrDefault("make",""))
            data.put("device_model",data.getJSONObject("device").getOrDefault("model",""))
            data.put("connection_type",data.getJSONObject("device").getOrDefault("connectiontype",""))
            data.put("os",data.getJSONObject("device").getOrDefault("os",""))
            if(data.getJSONObject("device").getOrDefault("dpidsha1","") != ""){
              data.put("uv",data.getJSONObject("device").getOrDefault("dpidsha1",""))
            }else if(data.getJSONObject("device").getOrDefault("dpidmd5","") != ""){
              data.put("uv",data.getJSONObject("device").getOrDefault("dpidmd5",""))
            }else if(data.getJSONObject("device").getOrDefault("didsha1","") != ""){
              data.put("uv",data.getJSONObject("device").getOrDefault("didsha1",""))
            }else if(data.getJSONObject("device").getOrDefault("didmd5","") != ""){
              data.put("uv",data.getJSONObject("device").getOrDefault("didmd5",""))
            }else if(data.getJSONObject("device").getOrDefault("macsha1","") != ""){
              data.put("uv",data.getJSONObject("device").getOrDefault("macsha1",""))
            }else if(data.getJSONObject("device").getOrDefault("macmd5","") != ""){
              data.put("uv",data.getJSONObject("device").getOrDefault("macmd5",""))
            }else{
              data.put("uv",""+ data.getJSONObject("device").getOrDefault("ip","")+data.getJSONObject("device").getOrDefault("ua","")+data.getJSONObject("device").getOrDefault("hwv",""))
            }
          }
          var middle = ""
          var tied_uv = ""
          try{
            middle = "%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s".format(data.getOrDefault("demand",""),data.getOrDefault("cid",""),data.getOrDefault("crid",""),data.getOrDefault("supply",""),data.getOrDefault("publisher_domain",""),data.getOrDefault("inventory_domain",""),data.getOrDefault("app_bundle",""),data.getOrDefault("tagid","0"),data.getOrDefault("country_code",""),data.getOrDefault("province_geonameid","0"),data.getOrDefault("city_geonameid","0"),data.getOrDefault("device_make",""),data.getOrDefault("device_model",""),data.getOrDefault("os",""),data.getOrDefault("carrier",""),data.getOrDefault("connection_type",""),data.getOrDefault("uagrp",""),data.getOrDefault("gender",""),data.getOrDefault("dcostcur",""))
            Set(
              topic + "<;>" + "%1.1f".format(data.getOrDefault("tz","0.0").toString.toFloat) + "<;>" + getFormatDate(data.getOrDefault("timestamp","0").toString.toInt + (data.getOrDefault("tz","0.0").toString.toFloat*3600).toLong) + "<;>" +middle -> (1,hyperLogLog(data.getOrDefault("uv","").toString.getBytes()),data.getOrDefault("dcost","0.0").toString.toDouble),
              topic + "<;>" + getFormatDate(data.getOrDefault("timestamp","0").toString.toInt) + "<;>" +middle -> (1,hyperLogLog(data.getOrDefault("uv","").toString.getBytes()),data.getOrDefault("dcost","0.0").toString.toDouble),
              topic + "<;>" + getFormatDate(data.getOrDefault("timestamp","0").toString.toInt) + "<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s".format(data.getOrDefault("supply",""),data.getOrDefault("publisher_domain",""),data.getOrDefault("inventory_domain",""),data.getOrDefault("app_bundle",""),data.getOrDefault("tagid","0"),data.getOrDefault("srevenuecur",""))-> (1,hyperLogLog.zero,data.getOrDefault("srevenue","0.0").toString.toDouble)
            )
          }catch{
            case e:Exception =>
              logger.error("set error " + e)
              Set[(String,(Int,HLL,Double))]()
          }finally {
            data = null
          }
        }catch {
          case e:Exception =>
            logger.error("json data error " + e)
            Set[(String,(Int,HLL,Double))]()

        }
      }).reduceByKey((pvUvFirst,pvUvSecond) => (pvUvFirst._1 + pvUvSecond._1,pvUvFirst._2 + pvUvSecond._2,pvUvFirst._3 + pvUvSecond._3))
      .mapWithState(StateSpec.function(mappingPvUvFunction).timeout(Duration(3600000*24)))
      .checkpoint(Seconds(100)).
      foreachRDD( { rdd =>{
        rdd.foreachPartition( partition =>{
          if(!partition.isEmpty){
            ProduceRowMsg.produceDemand(producer,"tornado_demand_result","tornado-demand",partition)
          }
        })
      }
      })
  }
  val hyperLogLog = new HyperLogLogMonoid(12)
  private val mappingPvUvFunction = (key:String, batchValues: Option[(Int,HLL,Double)], state: State[HLL]) => {
    var pvUvButch = batchValues.getOrElse((0,hyperLogLog.zero,0.0))
    try {
      if (state.isTimingOut()) {
        var uv = pvUvButch._2.estimatedSize.toInt
        if(uv > pvUvButch._1){
          uv = pvUvButch._1
        }
        key + "<;>" + pvUvButch._1 + "<;>" + uv + "<;>" + pvUvButch._3
      } else {
        val stateBefore = state.getOption().getOrElse(hyperLogLog.zero)
        val stateNow = stateBefore + pvUvButch._2
        state.update(stateNow)
        var stateChange = stateNow.estimatedSize.toInt - stateBefore.estimatedSize.toInt
        if (stateChange > pvUvButch._1){
          stateChange = pvUvButch._1
        }
        key + "<;>" + pvUvButch._1 + "<;>" + stateChange  + "<;>" + pvUvButch._3
      }
    } finally{
      pvUvButch = null
    }
  }
}

