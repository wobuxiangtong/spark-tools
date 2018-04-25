/**
  * Created by Nelson on 2017/4/25.
  */
import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

object ConnectionPool {
  val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val connectionPool = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl(PropertyBag.getProperty("bonecp.jdb.curl", "jdbc:mysql://192.168.1.15:3306/adstailor_statistics"))
      config.setUsername(PropertyBag.getProperty("bonecp.username", "root"))
      config.setPassword(PropertyBag.getProperty("bonecp.password", "123456"))
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(10)
      config.setMaxConnectionsPerPartition(50)
      config.setPartitionCount(4)
//      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)

      Some(new BoneCP(config))
    } catch {
      case exception:Exception=>
        logger.warn("Error in creation of connection pool" + ExceptionUtils.getStackTrace(exception))
        None
    }
  }

  def getConnection:Option[Connection] ={
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }

  def closeConnection(connection:Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }
}
