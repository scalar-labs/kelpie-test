#!/usr/bin/env bash
# Create a JIRA ticket for a failed daily test or benchmark.
#
# Usage:
#   create_jira_ticket.sh <name> <type> <run_url>
#
# Required env:
#   JIRA_AUTH         - "<email>:<api-token>" for curl -u
#   JIRA_ASSIGNEE_ID  - Atlassian account id of the default assignee
#
# Optional env:
#   JIRA_PROJECT_KEY        - JIRA project key (default: DLT)
#   JIRA_BOARD_ID           - Board id used to look up the active sprint (default: 1)
#   JIRA_DESCRIPTION_EXTRA  - Extra text appended to the ticket description
#   JIRA_LOG_ARTIFACT       - Name of an uploaded log artifact, referenced in the
#                             ticket's environment field

set -uo pipefail

TEST_NAME="${1:-unknown-test}"
TEST_TYPE="${2:-unknown-type}"
RUN_URL="${3:-}"

: "${JIRA_AUTH:?JIRA_AUTH is not set}"
: "${JIRA_ASSIGNEE_ID:?JIRA_ASSIGNEE_ID is not set}"

PROJECT_KEY="${JIRA_PROJECT_KEY:-DLT}"
BOARD_ID="${JIRA_BOARD_ID:-1}"
DATE=$(date +"%Y-%m-%d")

# Look up the active sprint id (best-effort; ticket is still created without it).
SPRINT_ID=$(curl -s -u "${JIRA_AUTH}" \
  -H "Content-Type: application/json" \
  "https://scalar-labs.atlassian.net/rest/agile/1.0/board/${BOARD_ID}/sprint?state=active" \
  | jq -r '.values[0].id // empty' 2>/dev/null || true)

# Assemble the description.
DESCRIPTION="Test: ${TEST_NAME}
Type: ${TEST_TYPE}
Date: ${DATE}
Run: ${RUN_URL}"

if [ -n "${JIRA_DESCRIPTION_EXTRA:-}" ]; then
  DESCRIPTION="${DESCRIPTION}

${JIRA_DESCRIPTION_EXTRA}"
fi

ENVIRONMENT="Workflow run: ${RUN_URL}"
if [ -n "${JIRA_LOG_ARTIFACT:-}" ]; then
  ENVIRONMENT="${ENVIRONMENT}
Log artifact: ${JIRA_LOG_ARTIFACT}"
fi

PAYLOAD=$(jq -n \
  --arg project_key "${PROJECT_KEY}" \
  --arg assignee_id "${JIRA_ASSIGNEE_ID}" \
  --arg summary "[Failure report ${DATE}] Daily ${TEST_TYPE}: ${TEST_NAME}" \
  --arg description "${DESCRIPTION}" \
  --arg environment "${ENVIRONMENT}" \
  --arg sprint_id "${SPRINT_ID}" \
  '{
    fields: ({
      project: { key: $project_key },
      assignee: { id: $assignee_id },
      summary: $summary,
      description: $description,
      environment: $environment,
      issuetype: { name: "Bug" }
    } + (if $sprint_id != "" then { customfield_10008: ($sprint_id | tonumber) } else {} end))
  }')

HTTP_CODE=$(curl -s -o /tmp/jira_response.json -w "%{http_code}" \
  -X POST \
  --url https://scalar-labs.atlassian.net/rest/api/2/issue \
  --user "${JIRA_AUTH}" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --data "${PAYLOAD}")

if [ "${HTTP_CODE}" = "201" ]; then
  KEY=$(jq -r '.key' </tmp/jira_response.json)
  echo "Created JIRA ticket: ${KEY}"
else
  echo "Failed to create JIRA ticket (HTTP ${HTTP_CODE})"
  cat /tmp/jira_response.json
  exit 1
fi
