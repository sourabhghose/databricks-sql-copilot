#!/usr/bin/env bash
#
# Deploy SQL Observability Co-Pilot to a Databricks workspace.
#
# Usage:
#   ./scripts/deploy.sh --profile <cli-profile> --warehouse <id> [options]
#
# Options:
#   --profile, -p    Databricks CLI profile name (required)
#   --warehouse, -w  SQL warehouse ID (required)
#   --app-name, -n   App name (default: sql-obs-copilot)
#   --auth-mode, -a  Auth mode: obo or sp (default: obo)
#   --genie-space    Genie Space ID (optional, leave blank to skip)
#   --create         Create the app if it doesn't exist
#   --help, -h       Show this help message
#
# Examples:
#   # First deploy to a new workspace
#   ./scripts/deploy.sh -p my-workspace -w abc123 --create
#
#   # Redeploy to existing app
#   ./scripts/deploy.sh -p my-workspace -w abc123
#
#   # Deploy with SP auth and Genie
#   ./scripts/deploy.sh -p DEFAULT -w 75fd8278393d07eb -a sp --genie-space 01f11d330b1e17349370616c86cb90ba

set -euo pipefail

PROFILE=""
WAREHOUSE_ID=""
APP_NAME="sql-obs-copilot"
AUTH_MODE="obo"
GENIE_SPACE_ID=""
CREATE_APP=false
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

usage() {
  head -27 "$0" | tail -25
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile)    PROFILE="$2"; shift 2 ;;
    -w|--warehouse)  WAREHOUSE_ID="$2"; shift 2 ;;
    -n|--app-name)   APP_NAME="$2"; shift 2 ;;
    -a|--auth-mode)  AUTH_MODE="$2"; shift 2 ;;
    --genie-space)   GENIE_SPACE_ID="$2"; shift 2 ;;
    --create)        CREATE_APP=true; shift ;;
    -h|--help)       usage ;;
    *) echo "Unknown option: $1"; usage ;;
  esac
done

if [[ -z "$PROFILE" || -z "$WAREHOUSE_ID" ]]; then
  echo "ERROR: --profile and --warehouse are required."
  usage
fi

echo "=== SQL Observability Co-Pilot Deployer ==="
echo "  Profile:      $PROFILE"
echo "  Warehouse:    $WAREHOUSE_ID"
echo "  App name:     $APP_NAME"
echo "  Auth mode:    $AUTH_MODE"
echo "  Genie space:  ${GENIE_SPACE_ID:-<none>}"
echo ""

# Verify CLI auth
echo "→ Verifying CLI authentication..."
if ! databricks auth profiles 2>/dev/null | grep -q "$PROFILE.*YES"; then
  echo "ERROR: Profile '$PROFILE' is not valid. Run: databricks auth login <host> --profile=$PROFILE"
  exit 1
fi
echo "  ✓ Profile is valid"

# Step 1: Create app if requested
if $CREATE_APP; then
  echo ""
  echo "→ Creating app '$APP_NAME'..."
  if databricks apps get "$APP_NAME" --profile "$PROFILE" &>/dev/null; then
    echo "  ✓ App already exists, skipping creation"
  else
    databricks apps create "$APP_NAME" \
      --description "SQL Observability Co-Pilot" \
      --profile "$PROFILE" \
      --no-compute
    echo "  ✓ App created"
  fi
fi

# Step 2: Generate target-specific app.yaml
echo ""
echo "→ Generating app.yaml for this deployment..."
BACKUP="$PROJECT_DIR/app.yaml.bak"
cp "$PROJECT_DIR/app.yaml" "$BACKUP"

cat > "$PROJECT_DIR/app.yaml" <<YAML
command:
  - "sh"
  - "scripts/start.sh"

env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: "$WAREHOUSE_ID"
  - name: AUTH_MODE
    value: "$AUTH_MODE"
  - name: UNIFIED_OBSERVABILITY_CATALOG
    value: "main"
  - name: UNIFIED_OBSERVABILITY_SCHEMA
    value: "unified_observability"
  - name: SPARK_HOTSPOT_LIMIT
    value: "25"
  - name: SQL_FRESHNESS_SLO_MINUTES
    value: "30"
  - name: SPARK_FRESHNESS_SLO_MINUTES
    value: "120"
  - name: PHOTON_FRESHNESS_SLO_MINUTES
    value: "1440"
  - name: GENIE_SPACE_ID
    value: "$GENIE_SPACE_ID"
YAML
echo "  ✓ app.yaml generated"

# Step 3: Sync source code
WORKSPACE_PATH="/Workspace/Shared/$APP_NAME"
echo ""
echo "→ Syncing source code to $WORKSPACE_PATH..."
databricks sync "$PROJECT_DIR" "$WORKSPACE_PATH" --full --profile "$PROFILE"
echo "  ✓ Source code synced"

# Step 4: Deploy
echo ""
echo "→ Deploying app..."
databricks apps deploy "$APP_NAME" \
  --source-code-path "$WORKSPACE_PATH" \
  --mode SNAPSHOT \
  --profile "$PROFILE" \
  --output json
echo "  ✓ Deployment complete"

# Step 5: Set OBO scopes if auth mode is obo
if [[ "$AUTH_MODE" == "obo" ]]; then
  echo ""
  echo "→ Configuring OBO scopes..."
  databricks api patch "/api/2.0/apps/$APP_NAME" \
    --profile "$PROFILE" \
    --json '{"user_api_scopes":["sql","dashboards.genie","catalog.tables:read","catalog.schemas:read","catalog.catalogs:read"]}' \
    >/dev/null
  echo "  ✓ OBO scopes configured"
fi

# Restore original app.yaml
mv "$BACKUP" "$PROJECT_DIR/app.yaml"

echo ""
echo "=== Deployment Summary ==="
APP_URL=$(databricks apps get "$APP_NAME" --profile "$PROFILE" --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('url','unknown'))" 2>/dev/null || echo "unknown")
SP_ID=$(databricks apps get "$APP_NAME" --profile "$PROFILE" --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id','unknown'))" 2>/dev/null || echo "unknown")

echo "  App URL:  $APP_URL"
echo "  SP ID:    $SP_ID"
echo ""
echo "Post-deploy checklist:"
echo "  1. Grant SP '$SP_ID' CAN_USE on warehouse '$WAREHOUSE_ID'"
if [[ -n "$GENIE_SPACE_ID" ]]; then
  echo "  2. Grant SP '$SP_ID' CAN_RUN on Genie space '$GENIE_SPACE_ID'"
fi
echo ""
echo "Done!"
