SELECT
    id,
    name,
    created_at,
    CASE WHEN restriction IS NULL THEN 'ACTIVE' ELSE 'RESTRICTED' END status
FROM companies
WHERE
    ($1::text IS NULL OR strpos(lower(name), lower($1)) > 0)
    AND ($2 = 'ALL' OR (restriction IS NULL) = ($2 = 'ACTIVE'))
    AND (
        $3::uuid IS NULL OR ($4 = 'NAME_ASC' AND (name, id) > ($5::text, $3))
        OR ($4 = 'CREATED_DESC' AND (created_at, id) < ($6::timestamptz, $3))
    )
ORDER BY
    CASE WHEN $4 = 'NAME_ASC' THEN name END ASC, CASE WHEN $4 = 'NAME_ASC' THEN id END ASC,
    CASE WHEN $4 = 'CREATED_DESC' THEN created_at END DESC, id DESC
LIMIT $7
