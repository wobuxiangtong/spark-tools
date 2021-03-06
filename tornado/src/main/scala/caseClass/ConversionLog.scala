package caseClass

case class ConversionLog(
           val click_id: String = "",
           val clk_timestamp: Int = 0,
           val group_id: Int = 0,
           val campaign_id: Int = 0,
           val timezone: Float = 0.0f,
           val message_id: String = "",
           val income_currency: String = "",
           val income: Float = 0.0f,
           val net_profit: Float = 0.0f,
           val forwarded: Int = 0,
           val bundle_name: String = "",
           val category_ids: String = "",
           val aff_id: Int = 0,
           val optimize: Boolean = false,
           val incentive: Boolean = false,
           val aff_pub: String = "",
           val aff_sub: String = "",
           val aff_sub2: String = "",
           val aff_sub3: String = "",
           val aff_sub4: String = "",
           val aff_sub5: String = "",
           val device_type: String = "",
           val device_model: String = "",
           val os: String = "",
           val osv: String = "",
           val ip: String = "",
           val country_code: String = "",
           val region_ids: String = "",
           val province_geoname_id: Int = 0,
           val city_geoname_id: Int = 0,
           val lat: Float = 0.0f,
           val lon: Float = 0.0f,
           val connection_type: String = "",
           val carrier: String = "",
           val gaid: String = "",
           val idfa: String = "",
           val adid: String = "",
           val keywords: String = "",
           val token: String = "",
           val algorithm: Int = 0,
           val tracking_link: String = "",
           val score: Float = 0.0f,
           val match_type: String = "",
           val referrer: String = "",
           val install_source: String = "",
           val app_version: String = "",
           val organic: Boolean = false,
           val ngaid: String = "",
           val nidfa: String = "",
           val nclk_timestamp: Int = 0,
           val nconv_timestamp: Int = 0,
           val nclk2conv_timestamp: Int = 0,
           val invalid_type: String = "",
           val payout_currency: String = "",
           val payout: Float = 0.0f,
           val tac: Float = 0.0f,
           val conv_timestamp: Int = 0,
           val err: String = ""
         ) extends Serializable
