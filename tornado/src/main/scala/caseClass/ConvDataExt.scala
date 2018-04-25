package caseClass

/**
  * Created by aaron on 2017/9/27.
  */
case class ConvDataExt (
  val income:Float,   //收益
  val timezone:Float,
  val payout:Float,  //支出
  val aff_pub:String = "",
  val conv_timestamp:Int,//转化时间
  val invalid_type:String = "",//无效原因
  val campaign_id:Int,
  val aff_id:Int,
  val forwarded:Int,//是否计费（0：通知，1：不通知）
  val income_currency:String = "",//收益使用的货币
  val payout_currency:String = "",
  val group_id:Int,
  val algorithm:Int,
  val net_profit :Float,
  val tac :Float
  ) extends Serializable
