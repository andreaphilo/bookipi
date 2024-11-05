WITH raw_invoice_item_data AS (
    SELECT
        _id AS invoice_id,
        company_id,
        COALESCE(nt, NULL) AS notes
        name as item_name,
        quantity,
        price
    FROM {{ ref('invoice_item') }}
)

SELECT CURRENT_DATE AS PARTITION_DATE, * FROM raw_invoice_item_data

