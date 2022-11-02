WITH snapshot_times AS (
    SELECT
        date_trunc('day', sg_image_created) AS snapshot_time_day,
        date_trunc('week', sg_image_created) AS snapshot_time_week,
        date_trunc('month', sg_image_created) AS snapshot_time_month,
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
        COALESCE(prev.domain, curr.domain) AS domain,
        COALESCE(prev.id, curr.id) AS id,
        COALESCE(prev.name, curr.name) AS name,
        COALESCE(prev.description, curr.description) AS description,
        COALESCE(prev.created_at, curr.created_at) AS created_at,
        COALESCE(prev.updated_at, curr.updated_at) AS updated_at,
        prev.id IS NULL AS is_added
    FROM
        snapshot_times prev INNER JOIN all_observations ao ON ao.prev_date = prev.snapshot_time_day
        FULL OUTER JOIN snapshot_times curr
        ON ao.date = curr.snapshot_time_day
        AND prev.id = curr.id
        AND prev.domain = curr.domain
    WHERE (prev.id IS NULL OR curr.id IS NULL)
    AND COALESCE(ao.date, curr.snapshot_time_day)::timestamp > '2022-08-08 00:00:00'::timestamp
    ORDER BY 1, 2, 4
)
SELECT * FROM daily_diff
