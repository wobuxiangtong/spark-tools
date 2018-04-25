import com.alibaba.fastjson.JSON
import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import joptsimple.{OptionParser, OptionSet}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


object EventStreamAnalysis extends AbstractAnalysis{

  val logger = LoggerFactory.getLogger(this.getClass)
  @transient
  val producer = ProduceRowMsg.getProduce
  val parser = new OptionParser
  val batchDuration = parser.accepts("batch-duration", "spark streaimg batchs duration")
    .withRequiredArg
    .describedAs("time(second)")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(20)
  val checkpointDuration = parser.accepts("checkpoint-duration", "spark streaimg checkpoint duration")
    .withRequiredArg
    .describedAs("time(second)")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(200)
  val timeoutDuration = parser.accepts("timeout-duration", "spark streaimg state timeout duration")
    .withRequiredArg
    .describedAs("time(hours)")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(12)
  def main(args: Array[String]): Unit = {
    implicit val options = parser.parse(args : _*)
    val checkPointDir = PropertyBag.getProperty("event.checkpoint.dir", "")
    val ssc = StreamingContext.getOrCreate(checkPointDir,functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext()(implicit options:OptionSet): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("Tornado Event Stream")//.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val checkPointDir = PropertyBag.getProperty("event.checkpoint.dir", "")
    val ssc = new StreamingContext(sc, Seconds(options.valueOf(batchDuration).intValue()))
    if(!checkPointDir.isEmpty){
      ssc.checkpoint(checkPointDir)
    }
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertyBag.getProperty("kafka.broker.connect", ""),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "tornado-event-AAA",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("tornado_event")
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
      x => {
        try{
          var data = JSON.parseObject(x.value().toString)
          var middle = ""
          var tied_uv = ""
          try{
            data.getOrDefault("event_timestamp","0").toString.toInt  - data.getOrDefault("install_timestamp","0").toString.toInt < 24*3600  match {
              case  n if n  => data.put("in1_retent_flag","1")
              case _   =>
            }
            data.getOrDefault("event_timestamp","0").toString.toInt  - data.getOrDefault("install_timestamp","0").toString.toInt < 2*24*3600  match {
              case  n if n  => data.put("in2_retent_flag","1")
              case _   =>
            }
            data.getOrDefault("event_timestamp","0").toString.toInt  - data.getOrDefault("install_timestamp","0").toString.toInt < 7*24*3600  match {
              case  n if n  => data.put("in7_retent_flag","1")
              case _   =>
            }
            data.getOrDefault("event_timestamp","0").toString.toInt  - data.getOrDefault("install_timestamp","0").toString.toInt < 15*24*3600  match {
              case  n if n  => data.put("in15_retent_flag","1")
              case _   =>
            }
            data.getOrDefault("event_timestamp","0").toString.toInt  - data.getOrDefault("install_timestamp","0").toString.toInt < 30*24*3600  match {
              case  n if n => data.put("in30_retent_flag","1")
              case _   =>
            }
            middle = "%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s".format(data.getOrDefault("campaign_id","0"),data.getOrDefault("group_id","0"),data.getOrDefault("aff_id","0"),data.getOrDefault("aff_pub",""),data.getOrDefault("event_value","0"),data.getOrDefault("order_currency","").toString.slice(0,3),data.getOrDefault("algorithm","0"))
            tied_uv = data.getOrDefault("ip","") + "_" + data.getOrDefault("gaid","") + "_" + data.getOrDefault("idfa","") + "_" + data.getOrDefault("adid","") + "_" + data.getOrDefault("adid","")
            Set(data.getOrDefault("timezone","0.0") + "<;>" + getFormatDate(data.getOrDefault("event_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong) + "<;>" +middle+"<;>"+ "event" -> (hyperLogLog(tied_uv.getBytes()),1,data.getOrDefault("in1_retent_flag","0").toString.toInt,data.getOrDefault("in2_retent_flag","0").toString.toInt,data.getOrDefault("in7_retent_flag","0").toString.toInt,data.getOrDefault("in15_retent_flag","0").toString.toInt,data.getOrDefault("in30_retent_flag","0").toString.toInt,data.getOrDefault("order_amount","0.0").toString.toDouble,data.getOrDefault("kpi","0").toString.toInt),
              getFormatDate(data.getOrDefault("event_timestamp","0").toString.toInt)  + "<;>" +middle+"<;>"+ "event" -> (hyperLogLog(tied_uv.getBytes()),1,data.getOrDefault("in1_retent_flag","0").toString.toInt,data.getOrDefault("in2_retent_flag","0").toString.toInt,data.getOrDefault("in7_retent_flag","0").toString.toInt,data.getOrDefault("in15_retent_flag","0").toString.toInt,data.getOrDefault("in30_retent_flag","0").toString.toInt,data.getOrDefault("order_amount","0.0").toString.toDouble,data.getOrDefault("kpi","0").toString.toInt),
              data.getOrDefault("timezone","0.0").toString + "<;>" + getFormatDate(data.getOrDefault("install_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong)  + "<;>" +middle+"<;>"+ "conv" -> (hyperLogLog(tied_uv.getBytes()),1,data.getOrDefault("in1_retent_flag","0").toString.toInt,data.getOrDefault("in2_retent_flag","0").toString.toInt,data.getOrDefault("in7_retent_flag","0").toString.toInt,data.getOrDefault("in15_retent_flag","0").toString.toInt,data.getOrDefault("in30_retent_flag","0").toString.toInt,data.getOrDefault("order_amount","0.0").toString.toDouble,data.getOrDefault("kpi","0").toString.toInt),
              getFormatDate(data.getOrDefault("install_timestamp","0").toString.toInt)  + "<;>" +middle + "<;>" + "conv" -> (hyperLogLog(tied_uv.getBytes()),1,data.getOrDefault("in1_retent_flag","0").toString.toInt,data.getOrDefault("in2_retent_flag","0").toString.toInt,data.getOrDefault("in7_retent_flag","0").toString.toInt,data.getOrDefault("in15_retent_flag","0").toString.toInt,data.getOrDefault("in30_retent_flag","0").toString.toInt,data.getOrDefault("order_amount","0.0").toString.toDouble,data.getOrDefault("kpi","0").toString.toInt))
            }catch{
            case e:Exception =>
              logger.error("set error " + e)
              Set[(String,(HLL,Int,Int,Int,Int,Int,Int,Double,Int))]()
          }finally {
            data = null
          }
        }catch {
          case e:Exception =>
            logger.error("json data error " + e)
            Set[(String,(HLL,Int,Int,Int,Int,Int,Int,Double,Int))]()

        }
      }).reduceByKey((pvUvFirst,pvUvSecond) => (pvUvFirst._1 + pvUvSecond._1,pvUvFirst._2 + pvUvSecond._2,pvUvFirst._3 + pvUvSecond._3,pvUvFirst._4 + pvUvSecond._4,pvUvFirst._5 + pvUvSecond._5,pvUvFirst._6 + pvUvSecond._6,pvUvFirst._7 + pvUvSecond._7,pvUvFirst._8 + pvUvSecond._8,pvUvSecond._9))
      .mapWithState(StateSpec.function(mappingPvUvFunction).timeout(Duration(3600000*options.valueOf(timeoutDuration).intValue())))
      .checkpoint(Seconds(options.valueOf(checkpointDuration).intValue())).
      foreachRDD( { rdd =>{
//        if(rdd.isEmpty()){
          rdd.foreachPartition( partition =>{
            if(!partition.isEmpty){
              ProduceRowMsg.produceEvent(producer,"tornado_event_result","tornado-event",partition)
            }
          })
        }
//    }
    })
  }
  val hyperLogLog = new HyperLogLogMonoid(12)
  val mappingPvUvFunction = (key:String, batchValues: Option[(HLL,Int,Int,Int,Int,Int,Int,Double,Int)], state: State[HLL]) => {
    var pvUvButch = batchValues.getOrElse((hyperLogLog.zero,0,0,0,0,0,0,0.0,0))
    var kpi = 0
    try {
      def getKpi(kpiFlag:Int,uV:Int): Int = {
        (kpiFlag,uV) match {
          case n if n._1 == 0 => 0
          case n if n._1 == 1 => pvUvButch._2
          case n if n._1 == 2 => uV
          case n if n._1 == 91 => pvUvButch._3
          case n if n._1 == 92 => pvUvButch._4
          case n if n._1 == 97 => pvUvButch._5
          case n if n._1 == 915 => pvUvButch._6
          case n if n._1 == 930 => pvUvButch._7
          case _ => 0
        }
      }
      if (state.isTimingOut()) {
          kpi = getKpi(pvUvButch._9,pvUvButch._1.estimatedSize.toInt)
          if(pvUvButch._9 == 2 && kpi > pvUvButch._2){
            kpi = pvUvButch._2
          }
          var uv = pvUvButch._1.estimatedSize.toInt
          if(uv > pvUvButch._2){
            uv = pvUvButch._2
          }
          key + "<;>" + pvUvButch._2 + "<;>" + uv  + "<;>" + pvUvButch._8 + "<;>" + kpi + "<;>" + pvUvButch._3 + "<;>" + pvUvButch._4 + "<;>" + pvUvButch._5 + "<;>" + pvUvButch._6 + "<;>" + pvUvButch._7
      } else {
          val stateBefore = state.getOption().getOrElse(hyperLogLog.zero)
          val stateNow = stateBefore + pvUvButch._1
          state.update(stateNow)
          var stateChange = stateNow.estimatedSize.toInt - stateBefore.estimatedSize.toInt
          kpi = getKpi(pvUvButch._9,stateChange)
          if(pvUvButch._9 == 2 && kpi > pvUvButch._2){
            kpi = pvUvButch._2
          }
          if (stateChange > pvUvButch._2){
            stateChange = pvUvButch._2
          }
          key + "<;>" + pvUvButch._2 + "<;>" + stateChange  + "<;>" + pvUvButch._8 + "<;>" + kpi + "<;>" + pvUvButch._3 + "<;>" + pvUvButch._4 + "<;>" + pvUvButch._5 + "<;>" + pvUvButch._6 + "<;>" + pvUvButch._7
      }
    } finally{
      pvUvButch = null
    }
  }
}
