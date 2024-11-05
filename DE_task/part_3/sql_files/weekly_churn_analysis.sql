WITH weekly_data AS (
    SELECT
        DATE_TRUNC(startDate, WEEK(MONDAY)) AS week_start,
        COUNTIF(status = 'active') AS new_subscriptions,
        COUNTIF(status = 'canceled') AS canceled_subscriptions,
        COUNTIF(status = 'terminated' AND endDate IS NOT NULL) AS terminated_subscriptions
    FROM
        `your_project.your_dataset.subscription`
    WHERE
        startDate BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
        OR endDate BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
    GROUP BY
        week_start
)

SELECT * FROM weekly_data
ORDER BY week_start DESC
