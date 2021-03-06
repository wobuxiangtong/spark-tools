DROP TABLE IF EXISTS `conv_collection`;
CREATE TABLE IF NOT EXISTS `conv_collection` (
  click_id VARCHAR(60) NOT NULL PRIMARY KEY,
  invalid_type CHAR(4) NOT NULL DEFAULT "",
  group_id INT(11) UNSIGNED NOT NULL DEFAULT 0,
  campaign_id INT(11) UNSIGNED NOT NULL DEFAULT 0,
  aff_id INT(11) UNSIGNED NOT NULL DEFAULT 0,
  aff_pub VARCHAR(255) NOT NULL DEFAULT "",
  aff_sub VARCHAR(1024) NOT NULL DEFAULT "",
  aff_sub2 VARCHAR(1024) NOT NULL DEFAULT "",
  country_code CHAR(3) NOT NULL DEFAULT "",
  ip VARCHAR(40) NOT NULL DEFAULT "",
  nip VARCHAR(40) NOT NULL DEFAULT "",
  carrier VARCHAR(60) NOT NULL DEFAULT "",
  ncarrier VARCHAR(60) NOT NULL DEFAULT "",
  device_type VARCHAR(20) NOT NULL DEFAULT "",
  device_make VARCHAR(60) NOT NULL DEFAULT "",
  device_model VARCHAR(60) NOT NULL DEFAULT "",
  gaid VARCHAR(36) NOT NULL DEFAULT "",
  ngaid VARCHAR(36) NOT NULL DEFAULT "",
  idfa VARCHAR(36) NOT NULL DEFAULT "",
  nidfa VARCHAR(36) NOT NULL DEFAULT "",
  app_version VARCHAR(60) NOT NULL DEFAULT "",
  clk_timestamp INT(10) UNSIGNED NOT NULL DEFAULT 0,
  install_timestamp INT(10) UNSIGNED NOT NULL DEFAULT 0,
  conv_timestamp INT(10) UNSIGNED NOT NULL DEFAULT 0,
  nclk_timestamp INT(10) UNSIGNED NOT NULL DEFAULT 0,
  ninstall_timestamp INT(10) UNSIGNED NOT NULL DEFAULT 0,
  income DECIMAL(5, 2) NOT NULL DEFAULT 0, 
  income_currency CHAR(3) NOT NULL DEFAULT "",
  payout  DECIMAL(5, 2) NOT NULL DEFAULT 0,
  payout_currency CHAR(3) NOT NULL DEFAULT "",
  forwarded TINYINT(1) UNSIGNED NOT NULL DEFAULT 0,
  deducted TINYINT(1) UNSIGNED NOT NULL DEFAULT 0,
  deduction_reason VARCHAR(255) NOT NULL DEFAULT "",
  key conv_collection_group_id(group_id),
  key conv_collection_campaign_id(campaign_id),
  key conv_collection_aff(aff_id, aff_pub),
  key conv_collection_invalid_type(invalid_type),
  key conv_collection_conv_timestamp(conv_timestamp),
  key conv_collection_forwarded(forwarded),
  key conv_collection_deducted(deducted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
