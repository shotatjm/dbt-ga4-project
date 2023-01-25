DECLARE start_date, end_date, host STRING;

SET start_date = "20191201";
SET end_date = "20191201";
SET host = "bunshun.jp";

CREATE OR REPLACE TABLE tmp_us.base_events
(
    event_date DATE,
    event_timestamp INT64,
    event_name STRING,
    event_params ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>,
    event_previous_timestamp INT64,
    event_bundle_sequence_id INT64,
    event_server_timestamp_offset INT64,
    user_id STRING,
    user_pseudo_id STRING,
    privacy_info STRUCT<analytics_storage STRING, ads_storage STRING, uses_transient_token STRING>,
    user_properties ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64, set_timestamp_micros INT64>>>,
    user_first_touch_timestamp INT64,
    device STRUCT<category STRING, mobile_brand_name STRING, mobile_model_name STRING, mobile_marketing_name STRING, mobile_os_hardware_model STRING, operating_system STRING, operating_system_version STRING, vendor_id STRING, advertising_id STRING, language STRING, is_limited_ad_tracking STRING, time_zone_offset_seconds INT64, browser STRING, browser_version STRING, web_info STRUCT<browser STRING, browser_version STRING, hostname STRING>>,
    geo STRUCT<continent STRING, country STRING, region STRING, city STRING, sub_continent STRING, metro STRING>,
    app_info STRUCT<id STRING, version STRING, install_store STRING, firebase_app_id STRING, install_source STRING>,
    traffic_source STRUCT<name STRING, medium STRING, source STRING>,
    stream_id STRING,
    platform STRING,
) PARTITION BY event_date;

INSERT INTO tmp_us.base_events
SELECT
    PARSE_DATE('%Y%m%d', `date`) AS event_date,
    CAST((visitStartTime + CAST((hits.time / 1000) AS INT64)) * POW(10, 6) AS INT64) AS event_timestamp, -- UAでは秒数/GA4ではマイクロ秒数
    'page_view' AS event_name,
    ARRAY<
      STRUCT<
        key STRING,
        value STRUCT<
          string_value STRING,
          int_value INT64,
          float_value FLOAT64,
          double_value FLOAT64
        >
      >
    >[
      (
        'page_location',
        (CONCAT('https://', hits.page.hostname, hits.page.pagePath), NULL, NULL, NULL)
      ),
      (
        'page_title',
        (hits.page.pageTitle, NULL, NULL, NULL)
      ),
      (
        'page_referrer',
        (hits.referer, NULL, NULL, NULL)
      ),
      (
        'ga_session_id',
        (NULL, visitStartTime, NULL, NULL)
      ), -- GA4のsession_idは、session_startイベントのタイムスタンプ（秒数）
      (
        'ga_session_number',
        (NULL, visitNumber, NULL, NULL)
      ),
      (
        'source',
        (IF(hits.hitNumber = 1, trafficSource.source, NULL), NULL, NULL, NULL)
      ),
      (
        'medium',
        (IF(hits.hitNumber = 1, trafficSource.medium, NULL), NULL, NULL, NULL)
      ),
      (
        'campaign',
        (IF(hits.hitNumber = 1, trafficSource.campaign, NULL), NULL, NULL, NULL)
      ),
      (
        'content',
        (IF(hits.hitNumber = 1, trafficSource.adContent, NULL), NULL, NULL, NULL)
      ),
      (
        'term',
        (IF(hits.hitNumber = 1, trafficSource.keyword, NULL), NULL, NULL, NULL)
      ),
      (
        'page_view_id',
        (NULL, NULL, NULL, NULL)
      ),
      (
        'article_id',
        ((SELECT value FROM UNNEST(hits.customDimensions) WHERE index = 12), NULL, NULL, NULL)
      ),
      (
        'published_at',
        ((SELECT value FROM UNNEST(hits.customDimensions) WHERE index = 11), NULL, NULL, NULL)
      ),
      (
        'page_type',
        ((SELECT value FROM UNNEST(hits.customDimensions) WHERE index = 3), NULL, NULL, NULL)
      ),
      (
        'article_type',
        ((SELECT value FROM UNNEST(hits.customDimensions) WHERE index = 13), NULL, NULL, NULL)
      ),
      (
        'series',
        ((SELECT value FROM UNNEST(hits.customDimensions) WHERE index = 8), NULL, NULL, NULL)
      )
    ] AS event_params,
    SAFE_CAST(NULL AS INT64) AS event_previous_timestamp,
    SAFE_CAST(NULL AS INT64) AS event_bundle_sequence_id,
    SAFE_CAST(NULL AS INT64) AS event_server_timestamp_offset,
    userId AS user_id,
    clientId AS user_pseudo_id,
    STRUCT<
      analytics_storage STRING,
      ads_storage STRING,
      uses_transient_token STRING
    >(
      privacyInfo.analytics_storage,
      privacyInfo.ads_storage,
      NULL
    ) AS privacy_info,
    SAFE_CAST(NULL AS ARRAY<
      STRUCT<
        key STRING,
        value STRUCT<
          string_value STRING,
          int_value INT64,
          float_value FLOAT64,
          double_value FLOAT64,
          set_timestamp_micros INT64
        >
      >
    >) AS user_properties,
    SAFE_CAST(NULL AS INT64) AS user_first_touch_timestamp,
    STRUCT<
      category STRING,
      mobile_brand_name STRING,
      mobile_model_name STRING,
      mobile_marketing_name STRING,
      mobile_os_hardware_model STRING,
      operating_system STRING,
      operating_system_version STRING,
      vendor_id STRING,
      advertising_id STRING,
      language STRING,
      is_limited_ad_tracking STRING,
      time_zone_offset_seconds INT64,
      browser STRING,
      browser_version STRING,
      web_info STRUCT<
        browser STRING,
        browser_version STRING,
        hostname STRING
      >
    >(
      device.deviceCategory,
      device.mobileDeviceBranding,
      device.mobileDeviceModel,
      device.mobileDeviceMarketingName,
      NULL,
      device.operatingSystem,
      device.operatingSystemVersion,
      NULL,
      NULL,
      device.language,
      "No",
      NULL,
      NULL,
      NULL,
      (
        device.browser,
        device.browserVersion,
        host
      )
    ) AS device,
    STRUCT< continent STRING,
            country STRING,
            region STRING,
            city STRING,
            sub_continent STRING,
            metro STRING >(
      geoNetwork.continent,
      geoNetwork.country,
      geoNetwork.region,
      geoNetwork.city,
      geoNetwork.subContinent,
      geoNetwork.metro
    ) AS geo,
    SAFE_CAST(NULL AS STRUCT<
      id STRING,
      version STRING,
      install_store STRING,
      firebase_app_id STRING,
      install_source STRING
    >) AS app_info,
    SAFE_CAST(NULL AS STRUCT<
      name STRING,
      medium STRING,
      source STRING
    >) AS traffic_source,
    SAFE_CAST(NULL AS STRING) AS stream_id,
    'WEB' AS platform,
FROM
    `hogehoge.ga_sessions_*`,
    UNNEST(hits) AS hits
WHERE
    _TABLE_SUFFIX between start_date AND end_date
    AND hits.type = 'PAGE'
