
/**
  * Created by uni on 2017/08/09
  * mysql连接池消费kafka中消息，指定connection消费指定message，避免竞争引发死锁
  */
import java.io._
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecords
import scala.collection.JavaConversions._
import scala.collection.mutable

object GroupFilter extends AbstractAnalysis{
  def job(records:ConsumerRecords[String,String],groups : mutable.ListBuffer[String], groupFilterWriter: PrintWriter): Unit ={
    records.foreach(record => {
      try{
        val data = JSON.parseObject(record.value().toString)
        if(groups.contains(data.getOrDefault("group_id","").toString)){
          groupFilterWriter.append(record.value().toString + "\n")
          groupFilterWriter.flush()
        }
      }catch {
        case e:Exception => println("data err" + e)
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val pvUvConsumer = ProduceRowMsg.consumer("tornado_click","groups_filter")
    var filterGroupList =  mutable.ListBuffer[String]()
    var lastCheckTime = System.currentTimeMillis()/1000
    var fr = new FileReader("/mnt/xvdb/tmp/redshift/tornado_click/group_list")
    var br = new BufferedReader(fr)
    var line = br.readLine()
    while(line != null){
      filterGroupList.append(line)
      line = br.readLine()
    }
    println(filterGroupList)
    br.close()
    fr.close()
    br = null
    fr = null
    var f = new File("/mnt/xvdb/tmp/redshift/tornado_click/tornado_click-%s".format(getFormatDateToHour(lastCheckTime)))
    var fw = new FileWriter(f, true)
    var groupFilterWriter = new PrintWriter(fw)

    while(true){
      if((System.currentTimeMillis()/1000 - lastCheckTime)/60 > 60){
        lastCheckTime = System.currentTimeMillis()/1000
        filterGroupList = mutable.ListBuffer[String]()
        var fr = new FileReader("/mnt/xvdb/tmp/redshift/tornado_click/group_list")
        var br = new BufferedReader(fr)
        var line = br.readLine()
        while(line != null){
          filterGroupList.append(line)
          line = br.readLine()
        }
        br.close()
        fr.close()
        br = null
        fr = null
        groupFilterWriter.close()
        fw.close()
        groupFilterWriter = null
        fw = null
        f = null
        f = new File("/mnt/xvdb/tmp/redshift/tornado_click/tornado_click-%s".format(getFormatDateToHour(lastCheckTime)))
        fw = new FileWriter(f, true)
        groupFilterWriter = new PrintWriter(fw)
      }
      val records = pvUvConsumer.poll(1000)
      job(records,filterGroupList,groupFilterWriter)
    }
  }
}

