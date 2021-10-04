/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    Try changing "table" to "view" below
*/
{{ config(materialized='incremental') }}
select  
      {{ dbt_utils.surrogate_key('c.campaign_id') }}  as campaign_key,
     11 as vendor_key, 
     c.campaign_id as campaign_id,
     c.advertiser_id as advertiser_id, 
     c.campaign_name as campaign_name,
     'TIKTOK' as vendor,
     c.objective_type as objective,
     NULL as ad_campaign_name,
     getdate() as campaign_start_date,
     getdate() as campaign_end_date,
     'event_type' as event_type,
     c.budget as budget,
     c.opt_status as status,
     c.create_time as source_created_at,
     c.updated_at as source_updated_at,
     current_timestamp as dwh_created_at,
     current_timestamp as dwh_updated_at
       from  tiktok_ads.campaign_history c 

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where c._fivetran_synced > (select max(dwh_created_at) from {{ this }})
{% endif %}