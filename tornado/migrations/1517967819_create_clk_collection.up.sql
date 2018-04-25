CREATE TABLE IF NOT EXISTS `clk_collection` (
  click_id VARCHAR(60) NOT NULL PRIMARY KEY,
  group_id INT(11) UNSIGNED NOT NULL DEFAULT 0,
  campaign_id INT(11) UNSIGNED NOT NULL DEFAULT 0,
  aff_id INT(11) UNSIGNED NOT NULL DEFAULT 0,
  aff_pub VARCHAR(255) NOT NULL DEFAULT "",
  clk_timestamp INT(10) UNSIGNED NOT NULL DEFAULT 0,
  key clk_collection_campaign_id(campaign_id),
  key clk_collection_affiliate(aff_id,aff_pub),
  key clk_collection_clk_timestamp(clk_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
