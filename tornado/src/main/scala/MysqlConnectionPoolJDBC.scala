/**
  * Created by uni on 2017/08/02
  *  Mysql 数据库连接池，通过jdbc建立连接
  */
import java.sql.{Connection, DriverManager}

import com.mysql.jdbc.PreparedStatement
import org.slf4j.LoggerFactory

object MysqlConnectionPoolJDBC {

  val logger = LoggerFactory.getLogger(this.getClass)
  private val connection_num = PropertyBag.getProperty("jdbc.connection.numbers","10").toInt //产生连接数
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new Array[Connection](connection_num) //连接池
  private val driver = PropertyBag.getProperty("jdbc.driver","com.mysql.jdbc.Driver")
//  private val url = PropertyBag.getProperty("bonecp.jdb.curl","jdbc:mysql://192.168.1.15:3306/adstailor_statistics_batch")
//  private val username = PropertyBag.getProperty("jdbc.username","root")
//  private val password = PropertyBag.getProperty("jdbc.password","123456")
  private val url = PropertyBag.getProperty("bonecp.jdb.curl","")
  private val username = PropertyBag.getProperty("bonecp.username","")
  private val password = PropertyBag.getProperty("bonecp.password","")


  /**
    * 初始化连接
    */
  private def initConn(): Option[Connection] = {
    try {
      Class.forName(driver)
      val conn = DriverManager.getConnection(url, username, password)
      conn.setAutoCommit(false)

      Some(conn)
    } catch {
      case exception:Exception =>
        logger.warn("Error in creation of connection " + exception.getStackTrace)
        None
    }
  }

  /**
    * 初始化连接池
    */
   def initConnectionPool(connection_num : Int): Array[Connection] = {
      if (current_num == 0) {
        for (i <- 1 to connection_num) {
          initConn() match {
            case Some(conn) => pools(current_num) = conn
            case None => pools(current_num) = null
          }
          current_num += 1
        }
      }
      pools
  }

  /**
    * 获得连接
    */
  def getConn(index:Int,connection_num:Int):Connection={
    if (current_num == 0){
      initConnectionPool(connection_num)
    }
    pools(index)
  }

  /**
    *设置连接，连接失效时使用
    */
  def setConn(index:Int):Connection = {
    initConn() match {
      case Some(conn) => pools(index) = conn
      case None => null
    }
    pools(index)
  }

  def main(args: Array[String]): Unit = {
    val a :Int = 3
    println(a + "<--------------------")


//    val conn_1 = getConn(1,10)
//    val conn_2 = getConn(2,10)
//    if(conn_1 != null){
////      println(current_num)
//      val statement = conn_1.createStatement()
//      statement.addBatch("select id from adx_clk_summary_daily order by id")
//      statement.addBatch("select id,dated from adx_clk_summary_daily order by id")
//      val resultSet = statement.executeBatch()
//      resultSet.foreach(x => println(x))
//      while(resultSet.next()){
//        val userName = resultSet.getLong("id")
//        println(userName,resultSet.first(),resultSet.next())
////        println("first",resultSet.first())
//      }
//    }
  }
}
