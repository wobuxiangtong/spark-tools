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

object ConversionStreamAnalysisNewer extends  AbstractAnalysis{
  val logger:Logger = LoggerFactory.getLogger(this.getClass)
  val parser = new OptionParser
  val batchDuration = parser.accepts("batch-duration", "spark streaimg batchs duration")
    .withRequiredArg
    .describedAs("time(second)")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(20)
  @transient
  val producer:KafkaProducer[String,String] = ProduceRowMsg.getProduce
  def main(args: Array[String]): Unit = {
    implicit val options = parser.parse(args : _*)
    val checkPointDir = PropertyBag.getProperty("conv.checkpoint.dir", "")
    val ssc = StreamingContext.getOrCreate(checkPointDir,functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext()(implicit options:OptionSet): StreamingContext = {
    val sparkConf = new SparkConf()
      .setAppName("Tornado Conv Stream")
    val sc = new SparkContext(sparkConf)
    val checkPointDir = PropertyBag.getProperty("conv.checkpoint.dir", "")
    val ssc = new StreamingContext(sc, Seconds(options.valueOf(batchDuration).intValue()))
    if(!checkPointDir.isEmpty){
      ssc.checkpoint(checkPointDir)
    }
    val topics = Array("tornado_conversion")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertyBag.getProperty("kafka.broker.connect", ""),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "tornado-conv-AAA",
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
          var conversionMiddle = ""
          var summaryMiddle = ""
          try{
            if(data.getOrDefault("forwarded","0").toString.toInt ==1){
              data.put("notify_flag","1")
            }
              conversionMiddle = "<;>%s<;>%s<;>%s<;>%s<;>%1.2f<;>%s<;>%1.2f<;>%s<;>%s".format(data.getOrDefault("campaign_id","0"), data.getOrDefault("group_id","0"), data.getOrDefault("aff_id","0"), data.getOrDefault("aff_pub",""), data.getOrDefault("income","0.00").toString.toFloat, data.getOrDefault("income_currency",""), data.getOrDefault("payout","0.00").toString.toFloat, data.getOrDefault("payout_currency",""), data.getOrDefault("algorithm","0"))
              summaryMiddle = "<;>%s<;>%s<;>%s<;>%s<;>%s".format(data.getOrDefault("campaign_id","0"), data.getOrDefault("group_id","0"), data.getOrDefault("aff_id","0"), data.getOrDefault("aff_pub",""), data.getOrDefault("algorithm","0"))
            if(data.getOrDefault("invalid_type","")==""){
              data.put("valid_flag","1")
              Set("%1.1f".format(data.getOrDefault("timezone","0.0").toString.toFloat)+ "<;>" + getFormatDate(data.getOrDefault("conv_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong)  + conversionMiddle -> (1,data.getOrDefault("valid_flag","0").toString.toInt,data.getOrDefault("notify_flag","0").toString.toInt,0.0f,0.0f),
                getFormatDate(data.getOrDefault("conv_timestamp","0").toString.toInt) + conversionMiddle -> (1,data.getOrDefault("valid_flag","0").toString.toInt,data.getOrDefault("notify_flag","0").toString.toInt,0.0f,0.0f),
                "%1.1f".format(data.getOrDefault("timezone","0.0").toString.toFloat)+ "<;>" + getFormatDate(data.getOrDefault("conv_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong)  + summaryMiddle -> (1,data.getOrDefault("valid_flag","0").toString.toInt,data.getOrDefault("notify_flag","0").toString.toInt,data.getOrDefault("net_profit","0.0").toString.toFloat,data.getOrDefault("tac","0.0").toString.toFloat),//11
                getFormatDate(data.getOrDefault("conv_timestamp","0").toString.toInt) + summaryMiddle -> (1,data.getOrDefault("valid_flag","0").toString.toInt,data.getOrDefault("notify_flag","0").toString.toInt,data.getOrDefault("net_profit","0.0").toString.toFloat,data.getOrDefault("tac","0.0").toString.toFloat)//10
              )
            }else{
              Set("%1.1f".format(data.getOrDefault("timezone","0.0").toString.toFloat)+ "<;>" + getFormatDate(data.getOrDefault("conv_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong)  + conversionMiddle -> (1,data.getOrDefault("valid_flag","0").toString.toInt,data.getOrDefault("notify_flag","0").toString.toInt,0.0f,0.0f),
                getFormatDate(data.getOrDefault("conv_timestamp","0").toString.toInt) + conversionMiddle -> (1,data.getOrDefault("valid_flag","0").toString.toInt,data.getOrDefault("notify_flag","0").toString.toInt,0.0f,0.0f),
                "%1.1f".format(data.getOrDefault("timezone","0.0").toString.toFloat) + "<;>" + getFormatDate(data.getOrDefault("conv_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong)  + summaryMiddle -> (1,data.getOrDefault("valid_flag","0").toString.toInt,data.getOrDefault("notify_flag","0").toString.toInt,data.getOrDefault("net_profit","0.0").toString.toFloat,data.getOrDefault("tac","0.0").toString.toFloat),//7
                getFormatDate(data.getOrDefault("conv_timestamp","0").toString.toInt) + summaryMiddle -> (1,data.getOrDefault("valid_flag","0").toString.toInt,data.getOrDefault("notify_flag","0").toString.toInt,data.getOrDefault("net_profit","0.0").toString.toFloat,data.getOrDefault("tac","0.0").toString.toFloat),//6
                "%1.1f".format(data.getOrDefault("timezone","0.0").toString.toFloat) + "<;>" + getFormatDate(data.getOrDefault("conv_timestamp","0").toString.toInt + (data.getOrDefault("timezone","0.0").toString.toFloat*3600).toLong)  + "<;>" + data.getOrDefault("campaign_id","0") + "<;>" + data.getOrDefault("group_id","0") + "<;>" + data.getOrDefault("aff_id","0") + "<;>" + data.getOrDefault("aff_pub","") + "<;>" + data.getOrDefault("invalid_type","") + "<;>" +  data.getOrDefault("algorithm","0") -> (1,0,0,0.0f,0.0f)//8
              )
            }
          }catch{
            case e:Exception =>
              logger.error("set error " + e)
              Set[(String,(Int,Int,Int,Float,Float))]()
          }finally {
            data = null
          }
        }catch {
          case e:Exception =>
            logger.error("json data error " + e)
            Set[(String,(Int,Int,Int,Float,Float))]()
        }
      }).reduceByKey((first,second) => (first._1 + second._1,first._2 + second._2,first._3 + second._3,first._4 + second._4,first._5 + second._5)).foreachRDD(  rdd =>{
      rdd.foreachPartition( partition =>{
        if(!partition.isEmpty) {
          ProduceRowMsg.produceConvNewer(producer, "tornado_conversion_result", "tornado-conv", partition)
        }
      })
    }
    )
  }
}