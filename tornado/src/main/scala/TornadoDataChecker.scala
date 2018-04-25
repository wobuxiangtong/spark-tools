import java.io.{File, FileWriter, PrintWriter}
import java.sql.ResultSet

import scala.collection.mutable

object TornadoDataChecker extends AbstractAnalysis{
  val timezoneList = List[Float](0.0f,5.5f,5.0f,-4.0f,-5.0f,8.0f,-7.0f,-8.0f,3.0f,1.0f)
  var timeList = mutable.ListBuffer[String]("","","","","","","","","","")
  def main(args: Array[String]): Unit = {
    while (true){
      val f = new File("/home/ubuntu/data_check.txt" )
      val fw = new FileWriter(f, true)
      val checkFile = new PrintWriter(fw)
      var currentTime = System.currentTimeMillis()/1000
      var i = -1
      var connectionQuery = ConnectionPool.getConnection.getOrElse(null)
      for (timezone <- timezoneList) {
        i += 1
        if (getFormatDate((timezone * 3600).toLong + currentTime) != timeList(i)) {
          timeList(i) = getFormatDate((timezone * 3600).toLong + currentTime)
          val statement = connectionQuery.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
          var resultSet = statement.executeQuery(PropertyBag.getProperty("adx_clk_summary_check", "").format(timeList(i), "" + timezone,timeList(i),""+timezone))
          while (resultSet.next()) {
            checkFile.append("adx_click_date_check," + resultSet.getString("dated")+","+ resultSet.getFloat("timezone")+","+resultSet.getInt("gross_clk_flag")+"\n")
          }
          resultSet = statement.executeQuery(PropertyBag.getProperty("adx_conv_summary_check","").format(timeList(i),""+timezone,timeList(i),""+timezone))
          while(resultSet.next()){
            checkFile.append("adx_conv_data_check," + resultSet.getString("dated")+","+ resultSet.getFloat("timezone")+","+ resultSet.getInt("gross_conv_flag")+","+ resultSet.getInt("valid_conv_flag")+","+ resultSet.getInt("notify_conv_flag")+"\n")
          }
          if(i == 0){
//            println(PropertyBag.getProperty("gmt_clk_summary_check","").format(timeList(i),timeList(i)))
            resultSet = statement.executeQuery(PropertyBag.getProperty("gmt_clk_summary_check","").format(timeList(i),timeList(i)))
            while (resultSet.next()) {
              checkFile.append("gmt_click_date_check," + resultSet.getString("dated")+","+ resultSet.getInt("gross_clk_flag")+"\n")
            }
            resultSet = statement.executeQuery(PropertyBag.getProperty("gmt_conv_summary_check","").format(timeList(i),timeList(i)))
            while(resultSet.next()){
              checkFile.append("gmt_conv_data_check," + resultSet.getString("dated")+","+ resultSet.getInt("gross_conv_flag")+","+ resultSet.getInt("valid_conv_flag")+","+ resultSet.getInt("notify_conv_flag")+"\n")
            }
          }
        }
      }
      checkFile.flush()
      fw.flush()
      checkFile.close()
      fw.close()
      Thread.sleep(30*60*1000)
    }

  }

}
