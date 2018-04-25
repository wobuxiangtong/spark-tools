package caseClass

/**
  * Created by moca on 2018/4/18.
  */
case class Impression (
                        var id:String,
                        var banner:Banner,
                        var video:Video,
                        var native:Native,
                        var displaymanager:String,
                        var displaymanagerver:String,
                        var instl:Int = 0,
                        var tagid:String,
                        var bidfloor:Float=0,
                        var bidfloorcur:String="USD",
                        var secure:Int,
                        var iframebuster:List[String],
                        var pmp:Pmp
                      ) extends Serializable
