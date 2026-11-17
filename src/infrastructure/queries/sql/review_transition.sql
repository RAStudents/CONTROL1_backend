WITH prior AS (
    UPDATE event_versions
    SET valid_to_sequence = $3
    WHERE company_id = $1 AND event_id = $2 AND valid_to_sequence IS NULL
    RETURNING
        company_id,
        event_id,
        event_version,
        classification_version,
        acknowledgement_version,
        status,
        acknowledged_at,
        acknowledged_by,
        is_erroneous,
        erroneous_marked_at,
        erroneous_marked_by
)

INSERT INTO event_versions
(
    company_id,
    event_id,
    event_version,
    classification_version,
    acknowledgement_version,
    valid_from_sequence,
    status,
    acknowledged_at,
    acknowledged_by,
    is_erroneous,
    erroneous_marked_at,
    erroneous_marked_by
)
SELECT
    company_id,
    event_id,
    event_version + 1,
    classification_version + $4::int,
    acknowledgement_version + (NOT $4)::int,
    $3,
    CASE WHEN $4 THEN status ELSE 'ACKNOWLEDGED' END,
    CASE WHEN $4 THEN acknowledged_at ELSE clock_timestamp() END,
    CASE WHEN $4 THEN acknowledged_by ELSE $5 END,
    is_erroneous OR $4,
    CASE WHEN $4 THEN clock_timestamp() ELSE erroneous_marked_at END,
    CASE WHEN $4 THEN $5 ELSE erroneous_marked_by END
FROM prior
