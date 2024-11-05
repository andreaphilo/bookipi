WITH raw_subscription_payment_data AS (
    SELECT
        _id AS subscription_payment_id,
        subscription_id,
        status,
        date,
        billingPeriod,
        amount,
        tax,
        totalExcludingTax
    FROM {{ ref('subscription_payment') }}
)

SELECT CURRENT_DATE AS PARTITION_DATE, * FROM raw_subscription_payment_data