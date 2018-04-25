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


object EventStreamAnalysisNewer extends AbstractAnalysis{

  private val logger = LoggerFactory.getLogger(this.getClass)
  @transient
  private val producer = ProduceRowMsg.getProduce
  val parser = new OptionParser
  private val batchDuration = parser.accepts("batch-duration", "spark streaimg batchs duration")
    .withRequiredArg
    .describedAs("time(second)")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(20)
  private val checkpointDuration = parser.accepts("checkpoint-duration", "spark streaimg checkpoint duration")
    .withRequiredArg
    .describedAs("time(second)")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(200)
  private val timeoutDuration = parser.accepts("timeout-duration", "spark streaimg state timeout duration")
    .withRequiredArg
    .describedAs("time(hours)")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(24)
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

    val subscribe = {
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> PropertyBag.getProperty("kafka.broker.connect", ""),
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "tornado-event-AAA",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val topics = Array("tornado_event")
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
  def incrPvUv(stream: InputDStream[ConsumerRecord[String, String]])(implicit options:OptionSet):Unit = {
    stream.flatMap(
      x => {
        try{
          var data = JSON.parseObject(x.value().toString)
          var middle = ""
          var tied_uv = ""
          try{
            val retentDays = (data.getOrDefault("event_timestamp","0").toString.toInt  - data.getOrDefault("install_timestamp","0").toString.toInt)/(3600*24)
            middle = "%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s".format(data.getOrDefault("campaign_id","0"),data.getOrDefault("group_id","0"),data.getOrDefault("aff_id","0"),data.getOrDefault("aff_pub",""),data.getOrDefault("event_value","0"),data.getOrDefault("order_currency","").toString.slice(0,3),data.getOrDefault("algorithm","0"))
            tied_uv = data.getOrDefault("click_id","").toString

            Set(
              retentDays + "<;>" + data.getOrDefault("kpi","0") + "<;>" + "%1.1f".format(data.getOrDefault("timezone","0.0").toString.toFloat) + "<;>" + getFormatDate(data.getOrDefault("install_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong)  + "<;>" + getFormatDate(data.getOrDefault("event_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong) + "<;>" +middle -> (1,hyperLogLog(tied_uv.getBytes()),data.getOrDefault("order_amount","0.0").toString.toDouble),
              retentDays + "<;>" + data.getOrDefault("kpi","0") + "<;>" + getFormatDate(data.getOrDefault("install_timestamp","0").toString.toInt)  + "<;>" + getFormatDate(data.getOrDefault("event_timestamp","0").toString.toInt) + "<;>" +middle -> (1,hyperLogLog(tied_uv.getBytes()),data.getOrDefault("order_amount","0.0").toString.toDouble)
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
      .mapWithState(StateSpec.function(mappingPvUvFunction).timeout(Duration(3600000*options.valueOf(timeoutDuration).intValue())))
      .checkpoint(Seconds(options.valueOf(checkpointDuration).intValue())).
      foreachRDD( { rdd =>{
          rdd.foreachPartition( partition =>{
            if(!partition.isEmpty){
              ProduceRowMsg.produceEventNewer(producer,"tornado_event_result","tornado-event",partition)
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
