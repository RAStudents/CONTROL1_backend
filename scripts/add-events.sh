#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8080}"
COMPANY_ID="${COMPANY_ID:?set COMPANY_ID to the target company UUID}"
CONTROL_EMAIL="${CONTROL_EMAIL:-demo@localhost.test}"
VIDEO_URL="${VIDEO_URL-https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4}"

if [[ -z "${CONTROL_PASSWORD:-}" ]]; then
    read -rsp "Пароль для ${CONTROL_EMAIL}: " CONTROL_PASSWORD
    printf '\n'
fi

export CONTROL_EMAIL CONTROL_PASSWORD VIDEO_URL

login() {
    local body response
    body="$(python3 -c 'import json,os; print(json.dumps({"email":os.environ["CONTROL_EMAIL"],"password":os.environ["CONTROL_PASSWORD"]}))')"
    response="$(curl -fsS --max-time 15 \
        -H 'Content-Type: application/json' \
        -d "$body" "$API_BASE/api/auth/login")"

    ACCESS_TOKEN="$(printf '%s' "$response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
    TOKEN_REFRESH_AT="$(( $(date +%s) + 840 ))"
    export ACCESS_TOKEN TOKEN_REFRESH_AT
    printf 'Вход выполнен.\n'
}

login
i=0

while true; do
    if (( $(date +%s) >= TOKEN_REFRESH_AT )); then
        login
    fi

    i=$((i + 1))
    payload="$(python3 - "$i" <<'PY'
import json, os, random, sys, uuid
from datetime import datetime, timedelta, timezone

i = int(sys.argv[1])
types = [
    ("person_detected", "SECURITY", "warehouse_1", "cam_03"),
    ("ppe_violation", "PPE", "workshop_1", "cam_01"),
    ("equipment_fault", "EQUIPMENT", "workshop_1", "sensor_02"),
]
event_type, category, location, device = types[(i - 1) % len(types)]
offset = random.randint(1, 14 * 24 * 60 * 60)
if i % 2:
    offset = -offset  # прошлое; чётные события — будущее
occurred_at = (
    datetime.now(timezone.utc) + timedelta(seconds=offset)
).isoformat(timespec="seconds").replace("+00:00", "Z")

event_id = "LAN-DEMO-" + uuid.uuid4().hex
print(json.dumps({"events": [{
    "id": event_id,
    "event_type_id": event_type,
    "category_code": category,
    "location_id": location,
    "device_id": device,
    "occurred_at": occurred_at,
    "preview_url": "https://placehold.co/640x360.jpg?text=" + event_id,
    "video_url": os.environ["VIDEO_URL"] or None,
}]}))
PY
)"
    idempotency_key="$(python3 -c 'import uuid; print(uuid.uuid4())')"

    response="$(curl -fsS --max-time 20 \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Idempotency-Key: $idempotency_key" \
        -d "$payload" \
        "$API_BASE/api/v1/companies/$COMPANY_ID/events/ingest")"

    printf '%s\n' "$response" | python3 -c '
import json,sys
r=json.load(sys.stdin)
print("Добавлено: {}, sequence {}–{}".format(r["count"],r["first_sequence"],r["last_sequence"]))
'
    sleep 3
done
