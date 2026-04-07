# Datafabric DAC Migration Inventory

## Goal

Use the original Dremio UI build while migrating DAC backend capabilities into Spring Boot services under `com.datafabric`.

## Phase 1

- Bootstrap and auth shell
  - `login`
  - `server_status`
  - `settings`
  - `users/preferences`
- Catalog and navigation
  - `sources`
  - `source/{name}`
  - `home/{name}`
  - `catalog`
  - `resourcetree`
- SQL runner shell
  - `scripts`
  - `sql-runner/session`
  - `sql/functions`
  - `trees`

## Phase 2

- Explore and dataset lifecycle
  - `datasets/new_untitled_sql`
  - `datasets/new_tmp_untitled_sql`
  - `datasets/.../summary`
  - `dataset/{path}/version/{version}/preview`
  - `dataset/{path}/version/{version}/run`
- Jobs
  - `jobs`
  - `job/{id}`
  - `job/{id}/data`
  - `queryProfile`

## Phase 3

- Source and format operations
  - `source/{source}/folder/...`
  - `source/{source}/dataset/...`
  - `file_format`
  - `folder_format`
- Collaboration and governance
  - wiki/tag
  - starred preferences
  - scripts ownership

## Phase 4

- Reflection and acceleration
  - dataset acceleration settings
  - reflection jobs
  - refresh orchestration
- Real-time capabilities
  - websocket notifications
  - job progress fanout

## Current Status

- Phase 1 bootstrap shell: in progress
- Phase 1 SQL runner shell: in progress
- Phase 2 onward: pending
