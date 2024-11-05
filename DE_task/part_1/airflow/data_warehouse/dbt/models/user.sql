WITH raw_user_data AS (
    SELECT
        _id AS user_id,
        email,
        default_company_id,
        CONCAT(first_name, " ", last_name) AS name
    FROM {{ ref('user') }}
)

SELECT CURRENT_DATE AS PARTITION_DATE, * FROM raw_user_data