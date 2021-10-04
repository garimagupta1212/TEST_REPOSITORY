select  


    {{ dbt_utils.surrogate_key('c.campaign_key') }}  as id,

  (select date_key from dim_dates where date =cast(getdate() as date)) as date_key, 

	 c.campaign_key as campaign_key,

	 c.campaign_id as campaign_id,  

	 ah.ad_id as ad_id,

	 ah.ad_name as ad_name,

	 ar.clicks as clicks,

	 ar.video_watched_2_s as  views,

	 ar.impressions as impressions,

	 ar.spend as spend,

	 'device' as device,

	 'TikTok' as source,

	 'Channel' as channel,

	 ar.conversion as conversion,

	 ar.conversion_rate as conversion_value,

	 ar.cpc as cost_per_click,

	 'return_on_advt' as return_on_advertising_spend,

	 current_timestamp as dwh_created_at,

	 current_timestamp as dwh_updated_at
	 
  
  from tiktok_ads.ad_report_daily ar
  left join (SELECT DISTINCT ad_id ,ad_name, adgroup_id , campaign_id
          FROM tiktok_ads.ad_history ah) ah on ar.ad_id=ah.ad_id 
  left outer join  {{'dim_campaigns_test'}} c on c.campaign_id=ah.campaign_id 

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where c._fivetran_synced > (select max(dwh_created_at) from {{ this }})
{% endif %}
  
