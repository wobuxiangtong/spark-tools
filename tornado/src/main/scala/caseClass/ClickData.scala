package caseClass

case class ClickData(
                           tz: Double,
                           demand: String,
                           cid: String,
                           crid: String,
                           dcost: Double,
                           dcostcur: String,
                           srevenue: String,
                           srevenuecur: String,
                           supply: String,
                           tagid: String,
                           uagrp:String,
                           site: Site,
                           app: App,
                           geo: GeoS,
                           device: Device,
                           user: User,
                           clickts: Long
                         ) extends Serializable
