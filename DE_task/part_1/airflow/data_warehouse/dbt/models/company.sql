WITH raw_company_data AS (
    SELECT
        _id AS company_id,
        owner AS user_id,
        company_name,
        COALESCE(country, NULL)
        state,
        currency,
        email

    FROM {{ ref('company') }}
)

SELECT CURRENT_DATE AS PARTITION_DATE, * FROM raw_company_data