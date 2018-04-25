import java.util.Properties

/**
  * Created by Nelson on 2017/5/8.
  */
object PropertyBag {
  private val propertyBag = {
    try {
      val in = getClass.getResourceAsStream("tornado.properties")
      val props = new Properties()
      props.load(in)
      in.close()
      Some(props)
    } catch {
      case exception:Exception =>
        println(exception.printStackTrace())
        None
    }
  }

  def getProperty(key:String, defaultValue:String):String = {
    propertyBag match {
      case Some(props) => props.getProperty(key, defaultValue)
      case None => ""
    }
  }
}
