import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


//class ValidGroupCount extends UserDefinedAggregateFunction {
//  //输入数据类型
//  override def inputSchema: StructType = {
//    StructType(StructField("validTpye",StringType, nullable = true) :: StructField("clickId",StringType, nullable = true) :: Nil)
//  }
//
//  override def bufferSchema: StructType = {
//    StructType(StructField("count",IntegerType, nullable = true) :: StructField("buffer",MapType(StringType,IntegerType), nullable = true) :: Nil)
//  }
//
//  override def dataType: DataType = IntegerType
//
//  override def deterministic: Boolean = true
//
//  override def initialize(buffer: MutableAggregationBuffer): Unit = {
////    println(s">>> initialize (buffer: $buffer)")
//    // NOTE: Scala's update used under the covers
//    buffer(0) = 0
//    buffer(1) = scala.collection.Map[String,Int]()
//  }
//
//  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
////    println(s">>> update (buffer: $buffer -> input: $input)",input.getString(0) == "")
//    val map = buffer.getMap[String,Int](1)
//    if((input.getString(0)== "") && (map.getOrElse(input.getString(1),0) == 0)) {
//      buffer(0) = buffer.getInt(0) + 1
//      buffer(1) = map + (input.getString(1) -> 1)
//    }
//  }
//
//  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
////    println(s">>> merge (buffer: $buffer -> row: $row)")
//    buffer(0) = buffer.getInt(0) + row.getInt(0)
//    buffer(1) = buffer.getMap[String,Int](1) ++ row.getMap[String,Int](1)
//  }
//
//  override def evaluate(buffer: Row): Any = {
////    println(s">>> evaluate (buffer: $buffer)")
//    buffer.getInt(0)
//  }
//}
//
//class InvalidGroupCount extends UserDefinedAggregateFunction {
//  //输入数据类型
//  override def inputSchema: StructType = {
//    StructType(StructField("item",StringType, nullable = true) :: StructField("clickId",StringType, nullable = true) :: Nil)
//  }
//
//  override def bufferSchema: StructType = {
//    StructType(StructField("count",IntegerType, nullable = true) :: StructField("buffer",MapType(StringType,IntegerType), nullable = true) :: Nil)
//  }
//
//  override def dataType: DataType = IntegerType
//
//  override def deterministic: Boolean = true
//
//  override def initialize(buffer: MutableAggregationBuffer): Unit = {
//    //    println(s">>> initialize (buffer: $buffer)")
//    // NOTE: Scala's update used under the covers
//    buffer(0) = 0
//    buffer(1) = scala.collection.Map[String,Int]()
//  }
//
//  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//    //    println(s">>> update (buffer: $buffer -> input: $input)",input.getString(0) == "")
//    val map = buffer.getMap[String,Int](1)
//    if(map.getOrElse(input.getString(1),0) == 0) {
//      buffer(0) = buffer.getInt(0) + 1
//      buffer(1) = map + (input.getString(1) -> 1)
//    }
//  }
//
//  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
//    //    println(s">>> merge (buffer: $buffer -> row: $row)")
//    buffer(0) = buffer.getInt(0) + row.getInt(0)
//    buffer(1) = buffer.getMap[String,Int](1) ++ row.getMap[String,Int](1)
//  }
//
//  override def evaluate(buffer: Row): Any = {
//    //    println(s">>> evaluate (buffer: $buffer)")
//    buffer.getInt(0)
//  }
//}
//
//class NotifyGroupCount extends UserDefinedAggregateFunction {
//  //输入数据类型
//  override def inputSchema: StructType = {
//    StructType(StructField("item",IntegerType, nullable = true) :: StructField("clickId",StringType, nullable = true) :: Nil)
//  }
//
//  override def bufferSchema: StructType = {
//    StructType(StructField("count",IntegerType, nullable = true) :: StructField("buffer",MapType(StringType,IntegerType), nullable = true) :: Nil)
//  }
//
//  override def dataType: DataType = IntegerType
//
//  override def deterministic: Boolean = true
//
//  override def initialize(buffer: MutableAggregationBuffer): Unit = {
////    println(s">>> initialize (buffer: $buffer)")
//    // NOTE: Scala's update used under the covers
//
//    buffer(0) = 0
//    buffer(1) = scala.collection.Map[String,Int]()
//  }
//
//  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
////    println(s">>> update (buffer: $buffer -> input: $input)",input.getInt(0) == 0)
//    val map = buffer.getMap[String,Int](1)
//    if(input.getInt(0) == 0 && (map.getOrElse(input.getString(1),0) == 0)){
//      buffer(0) = buffer.getInt(0) + 1
//      buffer(1) = map + (input.getString(1) -> 1)
//
//    }
//  }
//
//  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
////    println(s">>> merge (buffer: $buffer -> row: $row)")
//    buffer(0) = buffer.getInt(0) + row.getInt(0)
//    buffer(1) = buffer.getMap[String,Int](1) ++ row.getMap[String,Int](1)
//  }
//
//  override def evaluate(buffer: Row): Any = {
////    println(s">>> evaluate (buffer: $buffer)")
//    buffer.getInt(0)
//  }
//}
//
//class ProfitSum extends UserDefinedAggregateFunction {
//  //输入数据类型
//  override def inputSchema: StructType = {
//    StructType(StructField("item",DoubleType, nullable = true) :: StructField("clickId",StringType, nullable = true) :: Nil)
//  }
//
//  override def bufferSchema: StructType = {
//    StructType(StructField("sum",DoubleType, nullable = true) :: StructField("buffer",MapType(StringType,IntegerType), nullable = true) :: Nil)
//  }
//
//  override def dataType: DataType = DoubleType
//
//  override def deterministic: Boolean = true
//
//  override def initialize(buffer: MutableAggregationBuffer): Unit = {
//    //    println(s">>> initialize (buffer: $buffer)")
//    // NOTE: Scala's update used under the covers
//
//    buffer(0) = 0.0
//    buffer(1) = scala.collection.Map[String,Int]()
//  }
//
//  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//    //    println(s">>> update (buffer: $buffer -> input: $input)",input.getInt(0) == 0)
//    val map = buffer.getMap[String,Int](1)
//    if(map.getOrElse(input.getString(1),0) == 0){
//      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
//      buffer(1) = map + (input.getString(1) -> 1)
//    }
//  }
//
//  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
//    //    println(s">>> merge (buffer: $buffer -> row: $row)")
//    buffer(0) = buffer.getDouble(0) + row.getDouble(0)
//    buffer(1) = buffer.getMap[String,Int](1) ++ row.getMap[String,Int](1)
//  }
//
//  override def evaluate(buffer: Row): Any = {
//    //    println(s">>> evaluate (buffer: $buffer)")
//    buffer.getDouble(0)
//  }
//}
//
//class TacSum extends UserDefinedAggregateFunction {
//  //输入数据类型
//  override def inputSchema: StructType = {
//    StructType(StructField("item",DoubleType, nullable = true) :: StructField("clickId",StringType, nullable = true) :: Nil)
//  }
//
//  override def bufferSchema: StructType = {
//    StructType(StructField("sum",DoubleType, nullable = true) :: StructField("buffer",MapType(StringType,IntegerType), nullable = true) :: Nil)
//  }
//
//  override def dataType: DataType = DoubleType
//
//  override def deterministic: Boolean = true
//
//  override def initialize(buffer: MutableAggregationBuffer): Unit = {
//    //    println(s">>> initialize (buffer: $buffer)")
//    // NOTE: Scala's update used under the covers
//
//    buffer(0) = 0.0
//    buffer(1) = scala.collection.Map[String,Int]()
//  }
//
//  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//    //    println(s">>> update (buffer: $buffer -> input: $input)",input.getInt(0) == 0)
//    val map = buffer.getMap[String,Int](1)
//    if(map.getOrElse(input.getString(1),0) == 0){
//      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
//      buffer(1) = map + (input.getString(1) -> 1)
//    }
//  }
//
//  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
//    //    println(s">>> merge (buffer: $buffer -> row: $row)")
//    buffer(0) = buffer.getDouble(0) + row.getDouble(0)
//    buffer(1) = buffer.getMap[String,Int](1) ++ row.getMap[String,Int](1)
//  }
//
//  override def evaluate(buffer: Row): Any = {
//    //    println(s">>> evaluate (buffer: $buffer)")
//    buffer.getDouble(0)
//  }
//}

class ValidGroupCount extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = {
    StructType(StructField("validTpye",StringType, nullable = true)  :: Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("count",IntegerType, nullable = true)  :: Nil)
  }

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //    println(s">>> initialize (buffer: $buffer)")
    // NOTE: Scala's update used under the covers
    buffer(0) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //    println(s">>> update (buffer: $buffer -> input: $input)",input.getString(0) == "")
//    val map = buffer.getMap[String,Int](1)
    if(input.getString(0)== "") {
      buffer(0) = buffer.getInt(0) + 1
    }
  }

  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
    //    println(s">>> merge (buffer: $buffer -> row: $row)")
    buffer(0) = buffer.getInt(0) + row.getInt(0)
  }

  override def evaluate(buffer: Row): Any = {
    //    println(s">>> evaluate (buffer: $buffer)")
    buffer.getInt(0)
  }
}

class InvalidGroupCount extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = {
    StructType(StructField("item",StringType, nullable = true) :: Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("count",IntegerType, nullable = true) :: Nil)
  }

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //    println(s">>> initialize (buffer: $buffer)")
    // NOTE: Scala's update used under the covers
    buffer(0) = 0
    buffer(1) = scala.collection.Map[String,Int]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //    println(s">>> update (buffer: $buffer -> input: $input)",input.getString(0) == "")
//    val map = buffer.getMap[String,Int](1)
//    if(map.getOrElse(input.getString(1),0) == 0) {
      buffer(0) = buffer.getInt(0) + 1
//      buffer(1) = map + (input.getString(1) -> 1)
//    }
  }

  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
    //    println(s">>> merge (buffer: $buffer -> row: $row)")
    buffer(0) = buffer.getInt(0) + row.getInt(0)
  }

  override def evaluate(buffer: Row): Any = {
    //    println(s">>> evaluate (buffer: $buffer)")
    buffer.getInt(0)
  }
}

class NotifyGroupCount extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = {
    StructType(StructField("item",IntegerType, nullable = true) :: Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("count",IntegerType, nullable = true) :: Nil)
  }

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //    println(s">>> initialize (buffer: $buffer)")
    // NOTE: Scala's update used under the covers

    buffer(0) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //    println(s">>> update (buffer: $buffer -> input: $input)",input.getInt(0) == 0)
    if(input.getInt(0) == 1){
      buffer(0) = buffer.getInt(0) + 1

    }
  }

  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
    //    println(s">>> merge (buffer: $buffer -> row: $row)")
    buffer(0) = buffer.getInt(0) + row.getInt(0)
  }

  override def evaluate(buffer: Row): Any = {
    //    println(s">>> evaluate (buffer: $buffer)")
    buffer.getInt(0)
  }
}

class ProfitSum extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = {
    StructType(StructField("item",DoubleType, nullable = true) :: Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("sum",DoubleType, nullable = true) :: Nil)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //    println(s">>> initialize (buffer: $buffer)")
    // NOTE: Scala's update used under the covers

    buffer(0) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //    println(s">>> update (buffer: $buffer -> input: $input)",input.getInt(0) == 0)
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)

  }

  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
    //    println(s">>> merge (buffer: $buffer -> row: $row)")
    buffer(0) = buffer.getDouble(0) + row.getDouble(0)
  }

  override def evaluate(buffer: Row): Any = {
    //    println(s">>> evaluate (buffer: $buffer)")
    buffer.getDouble(0)
  }
}

class TacSum extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = {
    StructType(StructField("item",DoubleType, nullable = true) :: Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("sum",DoubleType, nullable = true) :: Nil)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //    println(s">>> initialize (buffer: $buffer)")
    // NOTE: Scala's update used under the covers

    buffer(0) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //    println(s">>> update (buffer: $buffer -> input: $input)",input.getInt(0) == 0)
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
  }

  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
    //    println(s">>> merge (buffer: $buffer -> row: $row)")
    buffer(0) = buffer.getDouble(0) + row.getDouble(0)
  }

  override def evaluate(buffer: Row): Any = {
    //    println(s">>> evaluate (buffer: $buffer)")
    buffer.getDouble(0)
  }
}