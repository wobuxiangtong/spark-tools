ALTER TABLE adx_daily_summary CHANGE aff_pub aff_pub varchar(255) NOT NULL DEFAULT "";
ALTER TABLE gmt_daily_summary CHANGE aff_pub aff_pub varchar(255) NOT NULL DEFAULT "";
ALTER TABLE adx_clk_daily_summary CHANGE aff_pub aff_pub varchar(255) NOT NULL DEFAULT "";
ALTER TABLE gmt_clk_daily_summary CHANGE aff_pub aff_pub varchar(255) NOT NULL DEFAULT "";
ALTER TABLE adx_conv_daily_summary CHANGE aff_pub aff_pub varchar(255) NOT NULL DEFAULT "";
ALTER TABLE gmt_conv_daily_summary CHANGE aff_pub aff_pub varchar(255) NOT NULL DEFAULT "";
ALTER TABLE adx_invalid_conv_daily_summary CHANGE aff_pub aff_pub varchar(255) NOT NULL DEFAULT "";
ALTER TABLE gmt_event_daily_summary CHANGE aff_pub aff_pub varchar(255) NOT NULL DEFAULT "";
ALTER TABLE adx_event_daily_summary CHANGE aff_pub aff_pub varchar(255) NOT NULL DEFAULT "";