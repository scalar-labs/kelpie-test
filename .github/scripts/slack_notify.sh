#!/usr/bin/env bash
# Post a benchmark Slack message. The mode is the first argument.
#
# Modes:
#   report   - daily benchmark report with the throughput-vs-date image
#   anomaly  - 3-sigma throughput anomaly alert
#
# Common env:
#   SLACK_TOKEN    - bot token (if empty, the script skips silently)
#   SLACK_CHANNEL  - target Slack channel
#   PRODUCT        - product label, e.g. "ScalarDB" / "ScalarDL"
#   PAGE_URL       - dashboard URL
#   RUN_URL        - workflow run URL
#
# report env:
#   IMG_URL        - public image URL
#   IMG_FILE       - local path to the generated PNG (its presence gates the image block)
#
# anomaly env:
#   DIRECTION LATEST MEAN SD LOWER UPPER DEV

set -uo pipefail

MODE="${1:?mode (report|anomaly) is required}"

: "${SLACK_CHANNEL:?SLACK_CHANNEL is not set}"
: "${PRODUCT:?PRODUCT is not set}"
: "${PAGE_URL:?PAGE_URL is not set}"
: "${RUN_URL:?RUN_URL is not set}"

if [ -z "${SLACK_TOKEN:-}" ]; then
  echo "SLACK_TOKEN is not set; skipping Slack notification."
  exit 0
fi

# Post a JSON body to Slack. Returns non-zero when Slack reports a failure.
post_slack() {
  local response
  response=$(curl -sS -X POST https://slack.com/api/chat.postMessage \
    -H "Authorization: Bearer ${SLACK_TOKEN}" \
    -H "Content-Type: application/json; charset=utf-8" \
    --data "$1")
  echo "${response}"
  if [ "$(echo "${response}" | jq -r '.ok')" != "true" ]; then
    echo "Slack post failed." >&2
    return 1
  fi
}

case "${MODE}" in
  report)
    : "${IMG_URL:?IMG_URL is not set}"

    # GitHub Pages takes a moment to redeploy after the push; wait for the new
    # PNG to be reachable so Slack fetches the fresh image.
    image_ready=false
    if [ -s "${IMG_FILE:-}" ]; then
      for _ in $(seq 1 24); do
        code=$(curl -s -o /dev/null -w "%{http_code}" "${IMG_URL}" || echo "000")
        if [ "${code}" = "200" ]; then
          image_ready=true
          break
        fi
        sleep 5
      done
    fi

    if [ "${image_ready}" = "true" ]; then
      BODY=$(jq -n \
        --arg channel "${SLACK_CHANNEL}" \
        --arg product "${PRODUCT}" \
        --arg img "${IMG_URL}" \
        --arg page "${PAGE_URL}" \
        --arg run "${RUN_URL}" \
        '{
          channel: $channel,
          text: ("Daily " + $product + " benchmark"),
          blocks: [
            {type: "section",
             text: {type: "mrkdwn",
                    text: ("*Daily " + $product + " benchmark*\nConcurrency 16, last 14 days. <" + $page + "|Dashboard> · <" + $run + "|Workflow run>")}},
            {type: "image", image_url: $img, alt_text: "Throughput vs Date"}
          ]
        }')
    else
      BODY=$(jq -n \
        --arg channel "${SLACK_CHANNEL}" \
        --arg product "${PRODUCT}" \
        --arg page "${PAGE_URL}" \
        --arg run "${RUN_URL}" \
        '{channel: $channel,
          text: ("Daily " + $product + " benchmark (image not yet deployed - <" + $page + "|dashboard> · <" + $run + "|run>)")}')
    fi

    # A failed report post should fail the job, as it did before extraction.
    post_slack "${BODY}"
    ;;

  anomaly)
    : "${DIRECTION:?DIRECTION is not set}"

    if [ "${DIRECTION}" = "low" ]; then
      HEADER=":small_red_triangle_down: ${PRODUCT} throughput dropped below the 3-sigma band"
    else
      HEADER=":chart_with_upwards_trend: ${PRODUCT} throughput rose above the 3-sigma band"
    fi
    TEXT=$(printf '%s (concurrency 16)\nLatest: *%s* ops/s (%s sigma from mean)\n14-day mean: %s, SD: %s, 3-sigma band: [%s, %s]\n<%s|Dashboard> | <%s|Workflow run>' \
      "${HEADER}" "${LATEST}" "${DEV}" "${MEAN}" "${SD}" "${LOWER}" "${UPPER}" "${PAGE_URL}" "${RUN_URL}")
    BODY=$(jq -n --arg channel "${SLACK_CHANNEL}" --arg text "${TEXT}" '{channel: $channel, text: $text}')

    # The alert is best-effort; don't fail the job if Slack is unhappy.
    post_slack "${BODY}" || true
    ;;

  *)
    echo "Unknown mode: ${MODE}" >&2
    exit 2
    ;;
esac
