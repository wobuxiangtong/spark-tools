import java.sql.Connection
import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

/**
  * Created by aaron on 2017/7/7.
  */
object ConnectionPoolExt {

  val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val connectionPool = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl(PropertyBag.getProperty("bonecp.jdb.curl.Ext", "jdbc:mysql://moca-ip-server.cbukzprwgirb.ap-southeast-1.rds.amazonaws.com:3336/tornado_batch?useSSL=false&tinyInt1isBit=false&rewriteBatchedStatements=true&useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&jdbcCompliantTruncation=false"))
//      config.setJdbcUrl(PropertyBag.getProperty("bonecp.jdb.curl.Ext", "jdbc:mysql://192.168.1.15:3306/tornado_batch"))
      config.setUsername(PropertyBag.getProperty("bonecp.username", "mocaip"))
      config.setPassword(PropertyBag.getProperty("bonecp.password", "newmocaip"))
//      config.setUsername(PropertyBag.getProperty("bonecp.username", "root"))
//      config.setPassword(PropertyBag.getProperty("bonecp.password", "123456"))
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
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
