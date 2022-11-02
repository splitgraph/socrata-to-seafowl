-- A catalog of all datasets, including whether they still exist in the latest scrape,
-- when they were added and when they were deleted

WITH snapshot_times AS (
    SELECT
        date_trunc('day', sg_image_created) AS snapshot_time_day,
        *
    FROM socrata.dataset_history
    WHERE sg_image_created::timestamp > '2022-01-01 00:00:00'::timestamp
),
dataset_first_last AS (
    SELECT
        id,
        MIN(sg_image_created) AS first_seen,
        MAX(sg_image_created) AS last_seen
    FROM snapshot_times
    GROUP BY id
),
exists_in_latest AS (
    SELECT id
    FROM dataset_first_last
    WHERE last_seen = (SELECT MAX(last_seen) FROM dataset_first_last)
),
all_datasets AS (
    SELECT
        l.id AS id,
        l.domain AS domain,
        l.name AS name,
        l.description AS description,
        l.updated_at AS updated_at,
        l.created_at AS created_at,
        l.resource AS resource,
        l.classification AS classification,
        l.metadata AS metadata,
        l.permalink AS permalink,
        l.link AS link,
        l.owner AS owner,

        f.sg_image_created AS first_seen,
        l.sg_image_created AS last_seen,
        f.sg_image_tag AS first_seen_in_tag,
        l.sg_image_tag AS last_seen_in_tag,
        f.sg_image_hash AS first_seen_in_image,
        l.sg_image_hash AS last_seen_in_image,

        (exists_in_latest.id IS NOT NULL) AS exists_in_latest
    FROM snapshot_times f
        JOIN dataset_first_last fl
            ON f.id = fl.id AND f.sg_image_created = fl.first_seen
        JOIN snapshot_times l
            ON l.id = fl.id AND l.sg_image_created = fl.last_seen
        -- TODO: finding out if an image is still latest is weird. If we join on a single-row
        -- table here, this makes some join optimization not work and causes an OOM. We can't use
        -- a subquery here either (DF doesn't support something like
        --    (SELECT MAX(sg_image_created) FROM snapshot_times) = l.last_seen_in_image
        -- but it does support subqueries in WHERE, so we hack it a bit and join on yet another
        -- table.
        LEFT JOIN exists_in_latest ON fl.id = exists_in_latest.id
)
SELECT * FROM all_datasets ORDER BY domain, name