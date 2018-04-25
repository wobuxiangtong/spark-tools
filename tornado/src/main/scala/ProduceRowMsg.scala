import java.util
import java.util.Properties
import java.util.concurrent.Executors
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
//import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.concurrent.{ExecutionContext, Future}
/**
  * Created by uni on 2017/08/04.
  */
object ProduceRowMsg{



  val logger = LoggerFactory.getLogger(this.getClass)
  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newWorkStealingPool(20)
    def execute(runnable: Runnable) {
      threadPool.submit(runnable)
    }
    def reportFailure(t: Throwable): Unit = {
      logger.error("future error" + t)
    }
  }

  def getProduce:KafkaProducer[String,String] = {
    val brokersList = PropertyBag.getProperty("kafka.broker.connect", "127.0.0.1:9092")
    val props = new Properties()
    props.put("bootstrap.servers", brokersList)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  def produceClick(producer:KafkaProducer[String,String] ,topic : String, msgType : String, msgList : Iterator[(String,Int)]) : Unit = {
    try{
      for(msg <- msgList){
        if(msg._1 != ""){
          Future{
              logger.warn(msg._1 + msg._2)
              producer.send(new ProducerRecord[String, String](topic,msgType +"<;>"+ msg._1 + "<;>" + msg._2))
          }
        }
      }
    }catch{
      case e : Exception => logger.error("error to producer click msg: ", e)
    }
  }
  def produceConv(producer: KafkaProducer[String, String], topic: String, msgType: String, msg:String): Unit = {
    try {
      Future {
            producer.send(new ProducerRecord[String, String](topic,msg))
          }
    } catch {
      case e: Exception => logger.error("error to producer conv msg: ", ExceptionUtils.getStackTrace(e))
    } finally {
    }
  }
  def produceConvNewer(producer: KafkaProducer[String, String], topic: String, msgType: String, msgList: Iterator[(String, (Int, Int, Int, Float, Float))]) :Unit = {
    try{
      for(msg <- msgList){
        if(msg._1 != ""){
          Future{
              producer.send(new ProducerRecord[String, String](topic, msg._1 + "<;>" + msg._2._1 + "<;>"  + msg._2._2 + "<;>"  + msg._2._3 +  "<;>" + msg._2._4 +  "<;>" + msg._2._5))
          }
        }

      }
    }catch{
      case e:Exception => logger.error("error to producer conv msg: ", e)
    }
  }
  def produceEvent(producer:KafkaProducer[String,String] ,topic : String, msgType : String, msgList : Iterator[String]) : Unit = {
    try{
      for(msg <- msgList){
        if(msg != ""){
          Future{
            val msgSplit = msg.split("<;>",-1)
            if( msgSplit(msgSplit.length-9) == "event"){
              if(msgSplit(0).toInt < 10 && msgSplit(1).toInt != 0){
                producer.send(new ProducerRecord[String, String](topic, msgSplit.slice(2,msgSplit.length-9).mkString("<;>") + "<;>" + msgSplit(1)))
              }
              if(msgSplit(msgSplit.length-8).toInt != 0){
                producer.send(new ProducerRecord[String, String](topic, msgSplit.slice(2,msgSplit.length-9).mkString("<;>") + "<;>" + msgSplit.slice(msgSplit.length-8,msgSplit.length-5).mkString("<;>")))
              }
            }else{
              if(msgSplit(0).toInt > 10 && msgSplit(1).toInt != 0){
                producer.send(new ProducerRecord[String, String](topic, msgSplit.slice(2,msgSplit.length-9).mkString("<;>") + "<;>" + msgSplit(1)))
              }
              if(msgSplit(msgSplit.length-1).toInt != 0){
                producer.send(new ProducerRecord[String, String](topic, msgSplit.slice(2,msgSplit.length-9).mkString("<;>") + "<;>" + msgSplit.slice(msgSplit.length-5,msgSplit.length).mkString("<;>")))
              }
            }
          }
        }
      }
    }catch{
      case e : Exception => logger.error("error to producer event msg: ", e)
    }
  }
  def produceEventNewer(producer:KafkaProducer[String,String] ,topic : String, msgType : String, msgList : Iterator[String]) : Unit = {
    try{
      for(msg <- msgList){
        if(msg != ""){
          Future{
            val msgSplit = msg.split("<;>",-1)
            var retent_1 = 0
            var retent_2 = 0
            var retent_7 = 0
            var retent_15 = 0
            var retent_30 = 0
            var kpi = 0.0f
            if (msgSplit(0).toInt < 1){
              retent_1 = msgSplit(msgSplit.length-2).toInt
            }
            if(msgSplit(0).toInt < 2){
              retent_2 = msgSplit(msgSplit.length-2).toInt
            }
            if(msgSplit(0).toInt < 7){
              retent_7 = msgSplit(msgSplit.length-2).toInt
            }
            if(msgSplit(0).toInt < 15){
              retent_15 = msgSplit(msgSplit.length-2).toInt
            }
            if(msgSplit(0).toInt < 30){
              retent_30 = msgSplit(msgSplit.length-2).toInt
            }
            if(msgSplit(1).toInt != 0){
              if(math.abs(msgSplit(1).toInt) == 1){
                kpi = (math.abs(msgSplit(1).toInt)/msgSplit(1).toInt) * msgSplit(msgSplit.length - 3).toInt
              }else if(math.abs(msgSplit(1).toInt) == 2){
                kpi = (math.abs(msgSplit(1).toInt)/msgSplit(1).toInt) * msgSplit(msgSplit.length - 2).toInt
              }else if(math.abs(msgSplit(1).toInt) == 7){ // kpi = order amount
                kpi = (math.abs(msgSplit(1).toInt)/msgSplit(1).toInt) * msgSplit(msgSplit.length - 1).toFloat
              }else if(math.abs(msgSplit(1).toInt) == 91){
                kpi = (math.abs(msgSplit(1).toInt)/msgSplit(1).toInt) * retent_1
              }else if(math.abs(msgSplit(1).toInt) == 92){
                kpi = (math.abs(msgSplit(1).toInt)/msgSplit(1).toInt) * retent_2
              }else if(math.abs(msgSplit(1).toInt) == 97){
                kpi = (math.abs(msgSplit(1).toInt)/msgSplit(1).toInt) * retent_7
              }else if(math.abs(msgSplit(1).toInt) == 915){
                kpi = (math.abs(msgSplit(1).toInt)/msgSplit(1).toInt) * retent_15
              }else if(math.abs(msgSplit(1).toInt) == 930){
                kpi = (math.abs(msgSplit(1).toInt)/msgSplit(1).toInt) * retent_30
              }else if(math.abs(msgSplit(1).toInt) > 800 && math.abs(msgSplit(1).toInt) < 900 && (math.abs(msgSplit(1).toInt) == 801 + msgSplit(0).toInt)){ //kpi n day retent
                kpi = (math.abs(msgSplit(1).toInt)/msgSplit(1).toInt) * msgSplit(msgSplit.length-2).toInt
              }
            }

            if(msgSplit.length == 15){
              //adx_event_daily_summary_pvuv
              producer.send(new ProducerRecord[String, String](topic, msgSplit(2) + "<;>" + msgSplit.slice(4,msgSplit.length).mkString("<;>"))) //12
              //adx_daily_summary
              if(kpi != 0 && msgSplit(1).toInt < 10){
                producer.send(new ProducerRecord[String, String](topic, msgSplit(2) + "<;>" + msgSplit.slice(4,msgSplit.length - 3).mkString("<;>") + "<;>" + kpi)) //10
              }else if(kpi !=0 && msgSplit(1).toInt > 10){
                producer.send(new ProducerRecord[String, String](topic, msgSplit(2) + "<;>" + msgSplit(3) + "<;>" + msgSplit.slice(5,msgSplit.length - 3).mkString("<;>") + "<;>" + kpi))//10
              }
              //adx_event_daily_summary_retent
              if(retent_30 !=0){
                producer.send(new ProducerRecord[String, String](topic, msgSplit(2) + "<;>" + msgSplit(3) + "<;>" + msgSplit.slice(5,msgSplit.length - 3).mkString("<;>") + "<;>" + retent_1 + "<;>" + retent_2 + "<;>" + retent_7 + "<;>" + retent_15 + "<;>" + retent_30))//14
              }
            }else if(msgSplit.length == 14){
              producer.send(new ProducerRecord[String, String](topic, msgSplit.slice(3,msgSplit.length).mkString("<;>"))) //11
              if(kpi != 0 && msgSplit(1).toInt < 10){
                producer.send(new ProducerRecord[String, String](topic, msgSplit.slice(3,msgSplit.length - 3).mkString("<;>") + "<;>" + kpi)) //9
              }else if(kpi !=0 && msgSplit(1).toInt > 10){
                producer.send(new ProducerRecord[String, String](topic, msgSplit(2) + "<;>" + msgSplit.slice(4,msgSplit.length - 3).mkString("<;>") + "<;>" + kpi))//9
              }
              if(retent_30 !=0){
                producer.send(new ProducerRecord[String, String](topic, msgSplit(2) + "<;>" + msgSplit.slice(4,msgSplit.length - 3).mkString("<;>") + "<;>" + retent_1 + "<;>" + retent_2 + "<;>" + retent_7 + "<;>" + retent_15 + "<;>" + retent_30))//13
              }
            }
          }
        }
      }
    }catch{
      case e : Exception => logger.error("error to producer event msg: ", e)
    }
  }

  def produceImpress(producer:KafkaProducer[String,String] ,topic : String, msgType : String, msgList : Iterator[String]) :Unit ={
    try{
      for(msg <- msgList){
        if(msg != ""){
          Future{
            producer.send(new ProducerRecord[String, String](topic, msg))
          }
        }
      }
    }catch{
      case e : Exception => logger.error("error to producer impress msg: ", e)
    }
  }
  def producePvUvTest(producer: KafkaProducer[String, String], topic: String, msgType: String, pvUvRecords: Iterator[(String, Int)]): Unit  = {
    try{
      for(msg <- pvUvRecords){
        Future{
          producer.send(new ProducerRecord[String, String](topic,msg.toString() ))
        }
      }
    }catch{
      case e : Exception => logger.error("error to producer msg: ", ExceptionUtils.getStackTrace(e))
    }finally {
    }
  }
  def producePvUv(producer:KafkaProducer[String,String] ,topic : String, msgType : String, msgList : Iterator[String]) : Unit = {
    try{
      for(msg <- msgList){
        Future{
          if(msg.split("<;>")(0) == "-0.0"){
            producer.send(new ProducerRecord[String, String](topic, msg.replace("-0.0<;>","")))
          }else{
            producer.send(new ProducerRecord[String, String](topic, msg))
          }
        }
      }
    }catch{
      case e : Exception => logger.error("error to producer msg: ", e)
    }
  }
  def produceDemand(producer: KafkaProducer[String, String], topic: String, msgType: String, msgList: Iterator[String]) :Unit = {
    try{
      for(msg <- msgList){
        if(msg != ""){
          Future{
            producer.send(new ProducerRecord[String, String](topic,msg))
          }
        }
      }
    }catch{
      case e : Exception => logger.error("error to producer click msg: ", e)
    }
  }
  def consumer(topic : String):KafkaConsumer[String,String]={
    val props = new Properties()
    val brokersList = PropertyBag.getProperty("kafka.broker.connect", "127.0.0.1:9092")
    props.put("bootstrap.servers", brokersList)
    props.put("key.deserializer",  "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "moca-click-uni")
    props.put("max.poll.records","1")
    props.put("enable.auto.commit","false")
    props.put("session.timeout.ms","40000")
    props.put("request.timeout.ms","60000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList(topic))
    consumer
  }
  def consumer(topic : String,groupID:String):KafkaConsumer[String,String]={
    val props = new Properties()
    val brokersList = PropertyBag.getProperty("kafka.broker.connect", "127.0.0.1:9092")
    props.put("bootstrap.servers", brokersList)
    props.put("key.deserializer",  "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)
    props.put("max.poll.records","1")
    props.put("enable.auto.commit","true")
    props.put("session.timeout.ms","40000")
    props.put("request.timeout.ms","60000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList(topic))
    consumer
  }

  def main(args: Array[String]): Unit = {

    val product = getProduce
    var count = 0
    while(true){
      for(j<-1 to 1){
        for(i <- 1 to 10){
          count = count +1
          if(count % 10 == 0){
            println("product ", count, " items ")
            Thread.sleep(60000)
          }
          product.send(new ProducerRecord[String, String]("tornado_click", "{'idfa':'%s','campaign_id':%d }".format(j.toString,i)))

        }
      }

    }
  }
}
