SELECT jsonb_build_object('event_types', (SELECT coalesce(jsonb_agg(jsonb_build_object(
    'id', id, 'name', name, 'icon_key', icon_key
) ORDER BY id), '[]') FROM event_types
WHERE company_id = $1),
'locations', (SELECT coalesce(jsonb_agg(
    jsonb_build_object(
        'id', l.id, 'name', l.name, 'devices',
        (
            SELECT
                coalesce(
                    jsonb_agg(jsonb_build_object('id', d.id, 'name', d.name) ORDER BY d.id), '[]'
                )
            FROM devices d
            WHERE d.company_id = l.company_id AND d.location_id = l.id
        )
    ) ORDER BY l.id
), '[]') FROM locations l
WHERE l.company_id = $1))
