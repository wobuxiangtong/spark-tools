
/**
  * Created by uni on 2017/08/09
  * mysql连接池消费kafka中消息，指定connection消费指定message，避免竞争引发死锁
  */
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.JavaConversions._
//import scala.concurrent.ExecutionContext.Implicits.global
object ConversionDetailConsumer {
  val producer:KafkaProducer[String,String] = ProduceRowMsg.getProduce
  def job(records:ConsumerRecords[String,String]): Unit ={
    records.foreach(record => {
      try{
        val data = JSON.parseObject(record.value().toString)
        val dataStr = data.getOrDefault("click_id","") + "<;>" +
          data.getOrDefault("group_id","0") + "<;>" +
          data.getOrDefault("campaign_id","0")+ "<;>" +
          data.getOrDefault("aff_id","0")+ "<;>" +
          data.getOrDefault("aff_pub","0")+ "<;>" +
          data.getOrDefault("invalid_type","")+ "<;>" +
          data.getOrDefault("income","0.0")+ "<;>" +
          data.getOrDefault("income_currency","")+ "<;>" +
          data.getOrDefault("payout","0.0")+ "<;>" +
          data.getOrDefault("payout_currency","")+ "<;>" +
          data.getOrDefault("forwarded","0")+ "<;>" +
          data.getOrDefault("ip","")+ "<;>" +
          data.getOrDefault("gaid","")+ "<;>" +
          data.getOrDefault("idfa","") + "<;>" +
          data.getOrDefault("carrier","") + "<;>" +
          data.getOrDefault("clk_timestamp","0")+ "<;>" +
          data.getOrDefault("conv_timestamp","0")
        //      println(dataStr)
        producer.send(new ProducerRecord[String, String]("tornado_conversion_detail", dataStr))
      }catch {
        case e:Exception => println("data err" + e)
      }


    })
  }
  def main(args: Array[String]): Unit = {
    var pvUvConsumer = ProduceRowMsg.consumer("tornado_conversion","tornado_conversion_details")
    while(true){
      val records = pvUvConsumer.poll(1000)
      job(records)
    }

  }


}
