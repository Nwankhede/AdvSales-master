# create raw table
CREATE TABLE IF NOT EXIST Programatic_raw (date string,ssp string,
    deal string,
                  advertiser string,
                             country string,
                                     device_category string,
                                                     agency string,
                                                            property string,
                                                                     marketplace string,
                                                                                 integration_type_id string,
                                                                                                     monetization_channel_id string,
                                                                                                                             ad_unit_id string,
                                                                                                                                        total_impressions string,
                                                                                                                                                          total_revenue string,
                                                                                                                                                                        viewable_impressions string,
                                                                                                                                                                                             measurable_impressions string,
                                                                                                                                                                                                                    revenue_share_percent string
)
# create staging table
# create dim and fact tables
