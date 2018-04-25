import com.alibaba.fastjson.JSON
import joptsimple.{OptionParser, OptionSet}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object ClickStreamAnalysisWeaker extends  AbstractAnalysis{
  val logger:Logger = LoggerFactory.getLogger(this.getClass)
  val parser = new OptionParser
  private val batchDuration = parser.accepts("batch-duration", "spark streaimg batchs duration")
    .withRequiredArg
    .describedAs("time(second)")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(20)
  @transient
  val producer:KafkaProducer[String,String] = ProduceRowMsg.getProduce
  def main(args: Array[String]): Unit = {
    implicit val options = parser.parse(args : _*)
    val checkPointDir = PropertyBag.getProperty("click.checkpoint.dir", "")
    val ssc = StreamingContext.getOrCreate(checkPointDir,functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext()(implicit options:OptionSet): StreamingContext = {
    val sparkConf = new SparkConf()
      .setAppName("Tornado Click Stream")
    val sc = new SparkContext(sparkConf)
    val checkPointDir = PropertyBag.getProperty("click.checkpoint.dir", "")
    val ssc = new StreamingContext(sc, Seconds(options.valueOf(batchDuration).intValue()))
    if(!checkPointDir.isEmpty){
      ssc.checkpoint(checkPointDir)
    }
    val topics = Array("tornado_click")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertyBag.getProperty("kafka.broker.connect", ""),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "tornado-click-AAA",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
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
          try{
              middle = "<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s<;>%s".format(
              data.getOrDefault("campaign_id","0"),
              data.getOrDefault("group_id","0"),
              data.getOrDefault("aff_id","0"),
              data.getOrDefault("aff_pub",""),
              data.getOrDefault("country_code",""),
              data.getOrDefault("province_geoname_id","0"),
              data.getOrDefault("city_geoname_id","0"),
              data.getOrDefault("os",""),
              data.getOrDefault("device_type",""),
              data.getOrDefault("device_make",""),
              data.getOrDefault("device_model",""),
              data.getOrDefault("algorithm","0"))
            Set("%1.1f".format(data.getOrDefault("timezone","0.0").toString.toFloat)+ "<;>" + getFormatDate(data.getOrDefault("clk_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong)  +middle -> 1,
                getFormatDate(data.getOrDefault("clk_timestamp","0").toString.toInt) + middle -> 1)
          }catch{
            case e:Exception =>
              logger.error("set error " + e)
              Set[(String,Int)]()
          }finally {
            data = null
          }
        }catch {
          case e:Exception =>
            logger.error("json data error " + e)
            Set[(String,Int)]()
        }
      }).reduceByKey((pvFirst,pvSecond) => pvFirst + pvSecond).foreachRDD(  rdd =>{
      rdd.foreachPartition( partition =>{
        if(!partition.isEmpty) {
          ProduceRowMsg.produceClick(producer, "tornado_click_result", "tornado-click", partition)
        }
      })
    }
    )
  }
}