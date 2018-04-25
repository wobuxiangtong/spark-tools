import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
case class Person(name: String, address: Address, children: List[Child])
case class Person1(name: String, age: Int)
case class Group(name: String, persons: Seq[Person1], leader: Person1,obj:Object)
case class Child(name: String, age: Int, birthdate: Option[java.util.Date])
case class Address(street: String, city: String = "")
object Json2CaseClass {
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  implicit val formats = DefaultFormats
  import com.fasterxml.jackson.annotation.JsonSubTypes.Type
  import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}


  def main(args: Array[String]): Unit = {
    val json = parse("""
         { "name": "joe",
           "address": {
             "street": "Bulevard"

           },
           "children": [
             {
               "name": "Mary",
               "age": 5,
               "birthdate": "2004-09-04T18:06:22Z"
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }
       """)
    val aaaa = json.extract[Person]
    println(aaaa.address)



    val jeroen = Person1("Jeroen", 26)
    val martin = Person1("Martin", 54)

//    val originalGroup = Group("Scala ppl", Seq(jeroen,martin), martin)
//     originalGroup: Group = Group(Scala ppl,List(Person(Jeroen,26), Person(Martin,54)),Person(Martin,54))

//    val groupJson = JsonUtil.toJson(originalGroup)
//    println(groupJson)
     val groupJson: String =
       """
         |{"name":"Scala ppl","persons":[{"name":"Jeroen","age":26},{"name":"Martin","age":54}],"leader":{"name":"Martin","age":54}}
       """.stripMargin

    val group = JsonUtil.fromJson[Group](groupJson)
    // group: Group = Group(Scala ppl,List(Person(Jeroen,26), Person(Martin,54)),Person(Martin,54))
    println(group)


  }

}


import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtil {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
//  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)

  def fromJson[T](json: String): Group = {
    mapper.readValue(json,classOf[Group])
  }
}
