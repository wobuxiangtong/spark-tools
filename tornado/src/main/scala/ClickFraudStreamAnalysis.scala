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

object ClickFraudStreamAnalysis extends  AbstractAnalysis{
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
    val checkPointDir = PropertyBag.getProperty("clickfraud.checkpoint.dir", "")
    val ssc = StreamingContext.getOrCreate(checkPointDir,functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext()(implicit options:OptionSet): StreamingContext = {
    val sparkConf = new SparkConf()
      .setAppName("Tornado ClickFraud Stream")
    val sc = new SparkContext(sparkConf)
    val checkPointDir = PropertyBag.getProperty("clickfraud.checkpoint.dir", "")
    val ssc = new StreamingContext(sc, Seconds(options.valueOf(batchDuration).intValue()))
    if(!checkPointDir.isEmpty){
      ssc.checkpoint(checkPointDir)
    }
    val topics = Array("tornado_clickfraud")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertyBag.getProperty("kafka.broker.connect", ""),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "tornado-clickfraud-AAA",
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
    stream.flatMap(x => {
          /*
          * 10001 :IP ADDR
          * 10002: DEVICE ID(gaid,idfa)
          * */
        val data = JSON.parseObject("10001<;>" + x.value().toString)
        if (data.containsKey("gaid") && (!data.get("gaid").toString.contains("{"))){
          data.put("advertising_id",data.get("gaid").toString)
        }
        if((!data.containsKey("advertising_id")) && data.containsKey("idfa") && (!data.get("idfa").toString.contains("{"))){
          data.put("advertising_id",data.get("idfa").toString)
        }

        Set(
          "10001<;>" + data.getOrDefault("ip","").toString -> 1,
           "10002<;>" + data.getOrDefault("advertising_id","").toString -> 1
        )
      }).reduceByKeyAndWindow((a:Int,b:Int) => a + b,Seconds(60),Seconds(20)).foreachRDD({ rdd =>{
        rdd.foreachPartition( pvUvRecord =>{
          if (!pvUvRecord.isEmpty) {
            for (msg <- pvUvRecord){
              println("IP : %s, Count : %d".format(msg._1,msg._2))
            }
          }
        })
      }
      }
    )
  }
}