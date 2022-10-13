with a_data as (
  SELECT 
    PARSE_JSON(_airbyte_data) as json_data
  FROM miir-testing.google_analytics.cf_website_bronze_tbl
)

SELECT
  json_data.ga_continent,
  json_data.ga_subcontinent,
  json_data.ga_countryIsoCode,
  json_data.ga_region,
  json_data.ga_regionIsoCode,
  json_data.ga_city,
  json_data.ga_date,
  json_data.ga_sessions,
  json_data.ga_transactions,
  json_data.ga_transactionRevenue
FROM a_data