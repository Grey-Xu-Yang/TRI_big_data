CREATE TABLE greyxu_tri_sum_data AS
SELECT
  year,
  industry_sector_code,
  SUM(on_site_release_total)/100000 AS on_site_release_total,
  SUM(off_site_release_total)/100000 AS off_site_release_total,
  SUM(energy_recover_on)/100000 AS energy_recover_on,
  SUM(energy_recover_of)/100000 AS energy_recover_of,
  SUM(recycling_on_site)/100000 AS recycling_on_site,
  SUM(recycling_off_site)/100000 AS recycling_off_site,
  SUM(treatment_on_site)/100000 AS treatment_on_site,
  SUM(treatment_off_site)/100000 AS treatment_off_site,
  SUM(production_waste)/100000 AS production_waste 
FROM greyxu_tri_data
GROUP BY year, industry_sector_code;

create 'greyxu_tri_sum', 'sector'

CREATE EXTERNAL TABLE greyxu_tri_sum_hive (
  year_industry_sector STRING,
  on_site_release_total DOUBLE,
  off_site_release_total DOUBLE,
  energy_recover_on DOUBLE,
  energy_recover_of DOUBLE,
  recycling_on_site DOUBLE,
  recycling_off_site DOUBLE,
  treatment_on_site DOUBLE,
  treatment_off_site DOUBLE,
  production_waste DOUBLE)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping' = ':key,sector:on_site_release_total,sector:off_site_release_total,sector:energy_recover_on,sector:energy_recover_of,sector:recycling_on_site,sector:recycling_off_site,sector:treatment_on_site,sector:treatment_off_site,sector:production_waste'
)
TBLPROPERTIES ('hbase.table.name' = 'greyxu_tri_sum');

INSERT OVERWRITE TABLE greyxu_tri_sum_hive
SELECT
  CONCAT(industry_sector_code, '_', CAST(year AS STRING)) AS key,
  on_site_release_total,
  off_site_release_total,
  energy_recover_on,
  energy_recover_of,
  recycling_on_site,
  recycling_off_site,
  treatment_on_site,
  treatment_off_site,
  production_waste
FROM greyxu_tri_sum_data;
