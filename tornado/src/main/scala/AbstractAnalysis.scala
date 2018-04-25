import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Calendar, Random, TimeZone}
import scala.util.Try

/**
  * Created by moca on 2017/6/5.
  * 抽象类定义公共方法以及变量
  */
 abstract class AbstractAnalysis {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  val timestampDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val simpleDateToHourFormat = new SimpleDateFormat("yyyyMMddHH")


  def getRandomMesc() : Int ={
    (new Random).nextInt(1000)
  }

  def getFormatDate(unix:Long): String = {
//    println("------------------" + unix* 1000 + ";" + dateFormat.format(unix*1000))
    dateFormat.format(unix*1000)
  }


  def getFormatDateToHour(unix:Long): String = {
    simpleDateToHourFormat.format(unix*1000)
  }
  def getMod(timestr:Int,mod:Int) :Int = {
    timestr%mod
  }

  def hasNotChar(str1: String, str2:String): Boolean ={
    !str1.contains(str2)
  }

  def getMod(str:String,mod:Int):Int = {
    val charArray = str.toCharArray
    var sum = 0
    for(c <- charArray){
      sum = sum + c.toByte
    }
    sum%mod
  }

  def get_last(xs:Seq[String]): String = {
     Try(xs.last).getOrElse("0").toString
  }
  def filterEmptyStr(str : String): String ={
    var returnStr = str
    if(str == null || str.isEmpty || str.toLowerCase().equals("null") || str.trim.length==0){
      returnStr = ""
    }
    returnStr
  }

  def getTheDate():String ={
    val calendar = Calendar.getInstance()

    calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) - 1)

    val suffix = simpleDateFormat.format(calendar.getTime())

    return  suffix
  }
  def getAddedDate(day : Int):String ={
    val calendar = Calendar.getInstance()

    calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + day)

    val suffix = dateFormat.format(calendar.getTime())

    return  suffix
  }

  def timeZoneToTime(timeZone:String):String = {

    if(timeZone.toFloat >= 0){
      val minutes:String = if (((timeZone.toFloat - timeZone.toFloat.toInt) * 60).toInt > 9){
        ((timeZone.toFloat - timeZone.toFloat.toInt) * 60).toInt.toString
      }else{
        "0" + ((timeZone.toFloat - timeZone.toFloat.toInt) * 60).toInt.toString
      }
      println("GMT+" + timeZone.toFloat.toInt + ":" + minutes)
      "GMT+" + timeZone.toFloat.toInt + ":" + minutes
    }else{
      val minutes:String = if (((-(timeZone.toFloat - timeZone.toFloat.toInt)) * 60).toInt > 9){
        ((-(timeZone.toFloat - timeZone.toFloat.toInt)) * 60).toInt.toString

      }else{
        "0" + ((-(timeZone.toFloat - timeZone.toFloat.toInt)) * 60).toInt.toString
      }
      println("GMT" + timeZone.toFloat.toInt + ":" + minutes)
      "GMT" + timeZone.toFloat.toInt + ":" + minutes
    }
  }


  def getTTL(timeZone:String):Long = {
    86400000 - ((System.currentTimeMillis +  (timeZone.toFloat*3600000).toLong) % 86400000)
  }
  def getTTLByTimeZone(timeZone:String):Int ={
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.DATE, calendar.get(Calendar.DATE))
    TimeZone.setDefault(TimeZone.getTimeZone(timeZoneToTime(timeZone)))
    val calenderTime = calendar.getTime()
    println(calenderTime)
    86400 - (calenderTime.getHours * 3600 + calenderTime.getMinutes * 60)
  }

  def getTimeStamp(str : String):Long = {
    timestampDateFormat.parse(str).getTime()/1000
  }

  def getMd5(inputStr: String): String = {
      MessageDigest.getInstance("MD5").digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") {_ + _}
  }

  def retentFlag(eventTime:Int,installTime:Int,day:Int):Int = {
    if((eventTime  - installTime) < (day * 24 * 3600)){
      1
    }else{
      0
    }
  }
  def retentDays(eventTime:Int,installTime:Int):Int = {
    (eventTime  - installTime)/(24 * 3600)
  }
  def uvGenerator(ip:String,ua:String,gaid:String,idfa:String,adid:String) :String = {
    ip + ua + gaid + idfa + adid
  }
  def getDateRange(date:String): Int ={
    require(date.replace("-","").toInt <= getAddedDate(-1).replace("-","").toInt,"截至时间不能大于昨天")
    var dateRange = 1
    while(getAddedDate(-dateRange) != date){
      dateRange = dateRange + 1
    }
    return dateRange
  }
  def domainSelect(str1:String,str2:String):String = {
    if(str1 != null && str1 != ""){
      str1
    }else if(str2 != null && str2 != ""){
      str2
    }else{
      ""
    }
  }

  def uvSelect(str1:String,str2:String,str3:String,str4:String,str5:String,str6:String,str7:String,str8:String,str9:String):String ={

    if(str1 != "" && str1 != null){
      str1
    }else if(str2 != "" && str2 != null){
      str2
    }else if(str3 != "" && str3 != null){
      str3
    }else if(str4 != "" && str4 != null){
      str4
    }else if(str5 != "" && str5 != null){
      str5
    }else if(str6 != "" && str6 != null){
      str6
    }else{
      str7 + str8 + str9
    }
  }

}
