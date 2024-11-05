WITH raw_subscription_data AS (
    SELECT
        _id AS subscription_id,
        company_id,
        status,
        subType,
        billingPeriod,
        startDate,
        endDate,
        nextBillingDate
    FROM {{ ref('subscription') }}
)

SELECT CURRENT_DATE AS PARTITION_DATE, * FROM raw_subscription_data