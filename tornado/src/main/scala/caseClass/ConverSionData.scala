package caseClass

case class ConverSionData(
                           val income:String = "", //收益
                           val country:String = "",//
                           val device_model:String = "",
                           val city:String = "",
                           val timezone:String = "",
                           val clk_timestamp:Int,
                           val payout:String = "",//支出
                           val aff_pub:String = "",
                           val device_type:String = "",
                           val osv:String = "",
                           val notify_clk_timestamp:Int,
                           val province:String = "",
                           val conv_timestamp:Int,//转化时间
                           val match_type:String = "",//类型匹配
                           val income_usd:String = "",//收益的美元价格
                           val installer_source:String = "",//安装来源
                           val notify_conv_timestamp:Int,//回传转化时间戳
                           val invalid_type:String = "",//无效原因
                           val notify_clk2conv_duration:Int,//回传转化时差
                           val campaign_id:Int,
                           val lat:String = "",
                           val lng:String = "",
                           val os:String = "",
                           val payout_usd:String = "",
                           val ip:String = "",
                           val advertising_id:String = "",
                           val aff_id:Int,
                           val message_id:String = "",//
                           val forwarded:Int,//是否计费（0：通知，1：不通知）
                           val locale_country:String = "",
                           val income_currency:String = "",//收益使用的货币
                           val payout_currency:String = "",
                           val country_code:String = "",
                           val referrer:String = "",
                           val click_id:String = "",
                           val group_id:Int,
                           val device_brand:String = "",
                           val notify_advertising_id:String = "",//回传标识符
                           val aff_sub:String = "",
                           val locale_language:String = "",
                           val imei:String = "",//手机串码
                           val algorithm:Int = 0,
                           val net_profit:Float,
                           val tac:Int
                         ) extends Serializable