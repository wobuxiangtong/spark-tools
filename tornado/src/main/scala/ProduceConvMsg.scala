import java.util.Properties
import java.util.concurrent.Executors
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by aaron on 2017/9/4.
  */
object ProduceConvMsg {

  val logger = LoggerFactory.getLogger(this.getClass)
  val brokersList = PropertyBag.getProperty("kafka.broker.connect", "127.0.0.1:9092")
  /**
    * 1、配置属性
    * metadata.broker.list : kafka集群的broker
    * serializer.class : 如何序列化发送消息
    */
  val props = new Properties()
  lazy  val producer = new KafkaProducer[String, String](props)
  props.put("bootstrap.servers", brokersList)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(100))

  /**
    * 产生并发送消息
    *
    */
  def producePvMsg(topic : String, msgType : String, msgList : Iterator[Row]) : Unit = {

    try{
      implicit val ec = ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(20))
      for(msg <- msgList){
        Future{
          producer.send(new ProducerRecord[String, String](topic, "pv-"+"msgType-"+msg.toString().replace("[", "").replace("]", "")))
        }
      }

    }catch{
      case e : Exception => logger.error("error to producer msg: ", ExceptionUtils.getStackTrace(e))
    }finally {
      //        producer.close
    }
  }

  /**
    * 产生并发送消息
    *
    */
  def produceConvMsg(topic : String, msgType : String, msg : (String)) : Unit = {

    try{
      Future{
        producer.send(new ProducerRecord[String, String](topic, msgType+"<;>"+msg))
      }

    }catch{
      case e : Exception => logger.error("error to produceConvMsg: ", ExceptionUtils.getStackTrace(e))
    }finally {
      //        producer.close
    }
  }
}
