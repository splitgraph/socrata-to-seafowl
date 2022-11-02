WITH snapshot_times AS (
    SELECT
        date_trunc('day', sg_image_created) AS snapshot_time_day,
        *
    FROM socrata.dataset_history
    -- Exclude old scrapes (big diffs)
    WHERE sg_image_created::timestamp > '2022-01-01 00:00:00'::timestamp
),
all_observations AS (
    SELECT * FROM (
        SELECT
            date,
            LAG(date, 1) OVER (ORDER BY date) AS prev_date
        FROM (SELECT DISTINCT snapshot_time_day AS date
            FROM snapshot_times)
    )
    WHERE prev_date IS NOT NULL
),
daily_diff AS (
    SELECT
        COALESCE(ao.date, curr.snapshot_time_day)::timestamp AS date,
        COALESCE(prev.id, curr.id) AS id,
        prev.id IS NULL AS is_added
    FROM
        snapshot_times prev INNER JOIN all_observations ao ON ao.prev_date = prev.snapshot_time_day
        FULL OUTER JOIN snapshot_times curr
        ON ao.date = curr.snapshot_time_day
        AND prev.id = curr.id
    WHERE (prev.id IS NULL OR curr.id IS NULL)
    -- Exclude the first scrape (prev_date is NULL -> looks like we added 40k datasets)
    AND COALESCE(ao.date, curr.snapshot_time_day)::timestamp > '2022-08-08 00:00:00'::timestamp
)
SELECT date AS day, id, is_added FROM daily_diff ORDER BY 1, 2
