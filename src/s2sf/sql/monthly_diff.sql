WITH by_period AS (
    SELECT date_trunc('month', date) AS period,
    *
    FROM socrata.daily_diff
),
period_stats AS (
    SELECT
        period,
        id,
        is_added,
        ROW_NUMBER() OVER (PARTITION BY period, id ORDER BY date ASC) = 1 AS is_earliest,
        ROW_NUMBER() OVER (PARTITION BY period, id ORDER BY date DESC) = 1 AS is_latest
    FROM by_period
),
first_last_in_period AS (
    SELECT * FROM period_stats WHERE is_earliest OR is_latest
),
period_diff AS (
    SELECT
        e.period AS period,
        e.id AS id,
        l.is_added AS is_added
    FROM first_last_in_period e INNER JOIN first_last_in_period l
    ON e.period = l.period
    AND e.id = l.id
    AND e.is_earliest
    AND l.is_latest
    -- If the dataset was added at the beginning and deleted at the end, nothing changed
    -- in this period (same if it was deleted and added again), so we can exclude this diff.
    -- Otherwise, we'll look at the final state of the dataset.
    AND e.is_added = l.is_added
)
SELECT
    period AS month,
    id,
    is_added
FROM period_diff
ORDER BY period ASC, id ASC
