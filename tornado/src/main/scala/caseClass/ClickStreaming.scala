package caseClass

/**
  * Created by uni on date 2017/08/04.
  * Email：uniwow@sina.com
  * moblie：18811029214
  *点击流数据对象 共计41字段
  */

case class ClickStreaming(
                          val click_id:String = "",                                     //"a4o3_bb0b8f695b6a43c88d9799b6926d557d1501659373355"  点击id
                          val group_id:Int,                                             //12345
                          val aff_id:Int,                                               //10004,  渠道id
                          val new_aff:Int,                                              //10004,  子渠道
                          val aff_offer:String = "",                                    //mc_mobikwik_in_ios",
                          val aff_pub:String = "",                                      //mobd1416944557d52cb",
                          val aff_sub:String = "",                                      //59817f754291932d0e3ee360",
                          val pid:String = "",                                          //PID_FB49EECE0A21AA37B170F493FEE948B6",
                          val uid:String = "",                                          //UID_104EB9005F136AB497CF863A57D61EA5",
                          val did:Int,                                                  //0,
                          val imei:String = "",                                         //"-",
                          val idfa:String = "",                                         //"-",
                          val country_code:String = "",                                 //"IN",
                          val country:String = "",                                      //"India",
                          val city:String = "",                                         //"unknown",
                          val os:String = "",                                           //"IOS",
                          val osv:String = "",                                          //"0.0.0",
                          val device_type:String = "",                                  //"Mobile",
                          val locale_country:String = "",                               //"US",
                          val locale_language:String = "",                              //"en",
                          val referer:String = "",                                      //"unknown",
                          val carrier:String = "",                                      //"unknown",    //运营商
                          val recommended:Int,                                          //0,
                          val repeat:Boolean,                                           //true,
                          val create_time:String = "",                                          //1501659373,
                          val timezone:Float,                                          //5.5,
                          val click_time:Int,                                         //1500000000,
                          val adid:String = "",                                         //"-",
                          val campaign_id:Int,                                          //12301,    项目ID
                          val province:String = "",                                     //"unknown",
                          val province_geoname_id:Int,                                  //0,
                          val city_geoname_id:Int,                                      //0,
                          val click_from:String = "",                                   //"42.106.115.191",
                          val gaid:String = "-",                                                 //1,      点击之后回传的点击唯一序列
                          val remote_addr:String = "",                                  //"42.106.115.191",
                          val redirect_to:String = "",                                  //"42.106.115.191",
                          val lat:String = "",                                          //"20.0",
                          val lng:String = "",                                          //"70.0",
                          val device_make:String = "",                                  //"unknown",
                          val device_model:String = "",                                 //"Mac OS X (iPhone)",
                          val ua:String = "") extends Serializable                     //"Safari"
