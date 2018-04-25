/**
  * Created by Uni on 2018/3/19.
  */
import com.alibaba.fastjson.JSON
import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import joptsimple.{OptionParser, OptionSet}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StateSpec, StreamingContext, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object ImpressClickStreamAnalysis extends AbstractAnalysis{
  private val logger = LoggerFactory.getLogger(this.getClass)
  @transient
  private val producer = ProduceRowMsg.getProduce
  val parser = new OptionParser
  private val batchDuration = parser.accepts("batch-duration", "spark streaming butch duration")
    .withRequiredArg
    .describedAs("time(second)")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(20)
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
    implicit val options = parser.parse(args : _*)
    val checkPointDir = PropertyBag.getProperty("impressClick.checkpoint.dir", "")
    val ssc = StreamingContext.getOrCreate(checkPointDir,functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }

  def functionToCreateContext()(implicit options:OptionSet): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("Tornado ImpressClick Stream")//.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val checkPointDir = PropertyBag.getProperty("impressClick.checkpoint.dir", "")
    val ssc = new StreamingContext(sc, Seconds(options.valueOf(batchDuration).intValue()))
    if(!checkPointDir.isEmpty){
      ssc.checkpoint(checkPointDir)
    }
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertyBag.getProperty("kafka.broker.connect", ""),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "tornado-impressClick",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("dot-click")
    val subscribe = Subscribe[String, String](topics, kafkaParams)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      subscribe
    )
    incrPvUv(stream)
    ssc
  }
  def incrPvUv(stream: InputDStream[ConsumerRecord[String, String]])(implicit options:OptionSet):Unit = {
    stream.flatMap(
      x =>{
        try{
          val data = JSON.parseObject(x.value().toString)
          if (data.getJSONObject("option") != null){
            var provinceGeonameId = 0
            var cityGeonameId = 0
            try{
              provinceGeonameId = data.getJSONObject("option").getJSONArray("region_ids").getString(0).toInt
            }catch{
              case e:Exception =>
                provinceGeonameId = 0
                logger.error("error when parse province_geoname_id with value" + e)
            }
            try{
              cityGeonameId = data.getJSONObject("option").getJSONArray("region_ids").getString(data.getJSONObject("option").getJSONArray("region_ids").size() - 1).toInt
            }catch{
              case e:Exception =>
                cityGeonameId = 0
                logger.error("error when parse city_geoname_id with value" + e)
            }
            val middle = "%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s".format(
              data.getOrDefault("publisher_id","0"),
              data.getOrDefault("inventory_id","0"),
              data.getOrDefault("plcmt_id","0"),
              data.getOrDefault("campaign_id","0"),
              data.getOrDefault("delivery_id","0"),
              data.getOrDefault("ad_id","0"),
              data.getJSONObject("option").getOrDefault("country_code",""),
              provinceGeonameId.toString,
              cityGeonameId.toString,
              data.getJSONObject("option").getOrDefault("devicetype",""),
              data.getJSONObject("option").getOrDefault("make",""),
              data.getJSONObject("option").getOrDefault("model",""),
              data.getJSONObject("option").getOrDefault("os",""),
              data.getJSONObject("option").getOrDefault("carrier",""),
              data.getJSONObject("option").getOrDefault("connectiontype",""),
              data.getJSONObject("option").getOrDefault("agegroup",""),
              data.getJSONObject("option").getOrDefault("gender","")
            )
            //            val tied = data.getJSONObject("option").getOrDefault("ip","").toString + data.getJSONObject("option").getOrDefault("gaid","") + data.getJSONObject("option").getOrDefault("idfa","")
            //            Set[(String,(Int,HLL))](
            //              "%1.1f".format(data.getOrDefault("timezone","0.0").toString.toFloat) + "<;>" + getFormatDate(data.getOrDefault("request_unix","0").toString.toLong + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong) + "<;>" +  middle ->(1,hyperLogLog(tied.getBytes)),
            //              getFormatDate(data.getOrDefault("request_unix","0").toString.toLong) + "<;>" +  middle ->(1,hyperLogLog(tied.getBytes))
            //            )
            Set[(String,Int)](
              "%1.1f".format(data.getOrDefault("timezone","0.0").toString.toFloat) + "<;>" + getFormatDate(data.getOrDefault("request_unix","0").toString.toLong + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong) + "<;>" +  middle ->1,
              getFormatDate(data.getOrDefault("request_unix","0").toString.toLong) + "<;>" +  middle ->1
            )
          }else{
            Set[(String,Int)]()
          }
        }catch{
          case e:Exception =>
            logger.error("json data err" + e)
            Set[(String,Int)]()
        }
      }
    ).reduceByKey((first,second) => (first + second))
      //      .mapWithState(StateSpec.function(mappingPvUvFunction).timeout(Duration(3600000*options.valueOf(timeoutDuration).intValue())))
      //      .checkpoint(Seconds(options.valueOf(checkpointDuration).intValue()))
      .foreachRDD({ rdd => {
      rdd.foreachPartition(partition => {
        if(partition.isEmpty){
          ProduceRowMsg.produceClick(producer,"analysis-result","click",partition)
        }
      })
    }
    })
  }
  //  val hyperLogLog = new HyperLogLogMonoid(12)
  //  private val mappingPvUvFunction = (key:String, batchValues: Option[(Int,HLL)], state: State[HLL]) => {
  //    var pvUvButch = batchValues.getOrElse((0,hyperLogLog.zero))
  //    try {
  //      if (state.isTimingOut()) {
  //        var uv = pvUvButch._2.estimatedSize.toInt
  //        if(uv > pvUvButch._1){
  //          uv = pvUvButch._1
  //        }
  //        key + "<;>" + pvUvButch._1 + "<;>" + uv
  //      } else {
  //        val stateBefore = state.getOption().getOrElse(hyperLogLog.zero)
  //        val stateNow = stateBefore + pvUvButch._2
  //        state.update(stateNow)
  //        var stateChange = stateNow.estimatedSize.toInt - stateBefore.estimatedSize.toInt
  //        if (stateChange > pvUvButch._1){
  //          stateChange = pvUvButch._1
  //        }
  //        key + "<;>" + pvUvButch._1 + "<;>" + stateChange
  //      }
  //    } finally{
  //      pvUvButch = null
  //    }
  //  }
}


