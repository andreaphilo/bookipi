WITH raw_invoice_payments_data AS (
    SELECT
        _id AS invoice_id,
        company_id,
        COALESCE(nt, NULL) AS notes,
        amount,
        date
    FROM {{ ref('invoice_payments') }}
)

SELECT CURRENT_DATE AS PARTITION_DATE, * FROM raw_invoice_payments_data