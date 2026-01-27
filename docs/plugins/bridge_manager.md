# Bridge Manager Usage Guide

This guide explains how to use the **Bridge Manager** to ingest real-time data from external sources into Gulp using bridges.

## Table of Contents

- [Bridge Manager Usage Guide](#bridge-manager-usage-guide)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [What is a Bridge?](#what-is-a-bridge)
  - [Architecture](#architecture)
    - [Data Flow](#data-flow)
  - [Prerequisites](#prerequisites)
  - [Getting Started](#getting-started)
    - [1. Verify a Bridge is Registered](#1-verify-a-bridge-is-registered)
    - [2. List Available Bridges](#2-list-available-bridges)
    - [3. Start an Ingestion Task](#3-start-an-ingestion-task)
    - [4. Monitor Ingestion Tasks](#4-monitor-ingestion-tasks)
    - [5. Stop an Ingestion Task](#5-stop-an-ingestion-task)
    - [6. Delete an Ingestion Task](#6-delete-an-ingestion-task)
  - [API Reference](#api-reference)
    - [List Bridges](#list-bridges)
    - [List Ingestion Tasks](#list-ingestion-tasks)
    - [Create and Start Ingestion](#create-and-start-ingestion)
    - [Stop Ingestion](#stop-ingestion)
    - [Delete Ingestion](#delete-ingestion)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
    - [Bridge Not Found](#bridge-not-found)
    - [Task Already Ongoing](#task-already-ongoing)
    - [Bridge Connection Timeout](#bridge-connection-timeout)
    - [Task Status Shows Failed](#task-status-shows-failed)
    - [Data Not Appearing in Gulp](#data-not-appearing-in-gulp)
  - [Related Documentation](#related-documentation)
  - [Building a Bridge](#building-a-bridge)
    - [Required Bridge Endpoints](#required-bridge-endpoints)
    - [`/start_ingestion` Request](#start_ingestion-request)
    - [`/stop_ingestion` Request](#stop_ingestion-request)
    - [Bridge Registration](#bridge-registration)
    - [Sending Data to Gulp](#sending-data-to-gulp)

## Overview

The **Bridge Manager** is a Gulp extension plugin that manages external bridge applications for real-time data ingestion. It provides a centralized way to:

- Register and manage bridges
- Create and control ingestion tasks
- Monitor ingestion status
- Handle lifecycle events (start, stop, delete)

## What is a Bridge?

A **Gulp Bridge** is an external microservice (or a "standard" application) that connects Gulp to an external data source (e.g., a SIEM, Velociraptor instance, network sensor, or any other log source). The bridge:

1. Fetches or receives data from the external source
2. Transforms it into the required format (GulpDocument)
3. Sends data to Gulp via the `/ingest_raw` REST API or `/ws_ingest_raw` WebSocket

Bridges are useful when:

- The external source doesn't expose a queryable API
- You need real-time streaming of data

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              GULP Server                                │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      Bridge Manager Plugin                      │    │
│  │                                                                 │    │
│  │  ┌─────────────────┐    ┌─────────────────────────────────────┐ │    │
│  │  │ Bridges Table   │    │ Bridge Tasks Table                  │ │    │
│  │  │ - bridge_id     │    │ - bridge_task_id                    │ │    │
│  │  │ - name          │    │ - bridge_id                         │ │    │
│  │  │ - url           │    │ - operation_id                      │ │    │
│  │  │ - status        │    │ - plugin_params                     │ │    │
│  │  │ - bridge_token  │    │ - status (ongoing/done/failed)      │ │    │
│  │  └─────────────────┘    │ - error                             │ │    │
│  │                         └─────────────────────────────────────┘ │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                     │                                   │
│                   Bridge Manager API calls                              │
│                   - /register_bridge                                    │
│                   - /unregister_bridge                                  │
│                   - /create_start_ingestion                             │
│                   - /stop_ingestion                                     │
│                   - /delete_ingestion                                   │
│                   - /list_bridges                                       │
│                   - /list_ingestion_tasks                               │
└─────────────────────────────────────│───────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    │          HTTP/HTTPS               │
                    ▼                                   ▼
          ┌─────────────────┐                 ┌─────────────────┐
          │   Bridge A      │                 │   Bridge B      │
          │ (Velociraptor)  │                 │ (Custom SIEM)   │
          │                 │                 │                 │
          │  Exposes:       │                 │  Exposes:       │
          │  /start_ingest  │                 │  /start_ingest  │
          │  /stop_ingest   │                 │  /stop_ingest   │
          └────────┬────────┘                 └────────┬────────┘
                   │                                   │
                   ▼                                   ▼
          ┌─────────────────┐                 ┌─────────────────┐
          │ External Source │                 │ External Source │
          │ (Velociraptor)  │                 │ (SIEM)          │
          └─────────────────┘                 └─────────────────┘
```

### Data Flow

1. **Bridge Registration**: A bridge registers itself with Gulp via `/register_bridge`
2. **Task Creation**: User requests ingestion via `/create_start_ingestion`
3. **Ingestion Start**: Bridge Manager calls the bridge's `/start_ingestion` endpoint
4. **Data Flow**: Bridge fetches data and sends it to Gulp via `/ingest_raw` or `/ws_ingest_raw`
5. **Task Completion**: User stops ingestion via `/stop_ingestion` or bridge reports status via `set_bridge_task_status`

## Prerequisites

Before using the Bridge Manager, ensure:

1. **Gulp is running** with the [Bridge Manager extension](../../src/gulp/plugins/extension/bridge_manager.py) installed
2. **You have a valid Gulp token** with `INGEST` permission
3. **A bridge application is deployed** and accessible from Gulp
4. **A Gulp Operation exists** to ingest data into

## Getting Started

### 1. Verify a Bridge is Registered

Before starting an ingestion task, verify that your bridge is registered with Gulp. Bridges typically register themselves on startup by calling the `/register_bridge` endpoint.

### 2. List Available Bridges

To see all registered bridges:

```bash
curl -X POST "http://localhost:8080/list_bridges" \
  -H "Content-Type: application/json" \
  -d '{
    "token": "YOUR_GULP_TOKEN"
  }'
```

**Response:**

```json
{
  "status": "success",
  "timestamp_msec": 1701278479259,
  "req_id": "abc123",
  "data": [
    {
      "id": "a1b2c3d4e5",
      "name": "velociraptor_bridge",
      "url": "http://bridge-host:8888",
      "status": "ready"
    }
  ]
}
```

### 3. Start an Ingestion Task

Create and start an ingestion task to begin receiving data:

```bash
curl -X POST "http://localhost:8080/create_start_ingestion?token=YOUR_GULP_TOKEN&bridge_id=a1b2c3d4e5&operation_id=your-operation-id" \
  -H "Content-Type: application/json" \
  -d '{
    "plugin": "raw",
    "mapping_parameters": {
      "mapping_file": "your_mapping.json",
      "mapping_id": "default"
    },
    "custom_parameters": {
      "source_query": "EventType=FileCreation",
      "poll_interval_sec": 5
    }
  }'
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `token` | query | Gulp authentication token with INGEST permission |
| `bridge_id` | query | ID of the bridge to use (from `/list_bridges`) |
| `operation_id` | query | Gulp operation ID to ingest data into |
| `plugin_params` (body) | `GulpPluginParameters` | Configuration for the bridge and ingestion |

**The `plugin_params` body** follows the `GulpPluginParameters` structure:

- `plugin`: The ingestion plugin to use (usually `raw`)
- `mapping_parameters`: Field mapping configuration (see [Mapping documentation](./plugins_and_mapping.md#mapping-101))
- `custom_parameters`: Bridge-specific parameters (varies by bridge implementation)

**Response:**

```json
{
  "status": "success",
  "timestamp_msec": 1701278479259,
  "req_id": "abc123",
  "data": {
    "bridge_task_id": "task123456"
  }
}
```

### 4. Monitor Ingestion Tasks

List and monitor running ingestion tasks:

```bash
# List all tasks
curl -X POST "http://localhost:8080/list_ingestion_tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "token": "YOUR_GULP_TOKEN"
  }'

# Filter by status
curl -X POST "http://localhost:8080/list_ingestion_tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "token": "YOUR_GULP_TOKEN",
    "flt": {
      "status": "ongoing"
    }
  }'

# Filter by bridge
curl -X POST "http://localhost:8080/list_ingestion_tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "token": "YOUR_GULP_TOKEN",
    "flt": {
      "bridge_id": "a1b2c3d4e5"
    }
  }'
```

**Task Status Values:**

| Status | Description |
|--------|-------------|
| `ongoing` | Task is actively ingesting data |
| `done` | Task completed successfully |
| `failed` | Task encountered an error (check `error` field) |
| `null` | Task is created but not started or has been stopped |

### 5. Stop an Ingestion Task

Stop a running ingestion task without deleting it:

```bash
curl -X POST "http://localhost:8080/stop_ingestion?token=YOUR_GULP_TOKEN" \
  -H "Content-Type: application/json" \
  -d '"task123456"'
```

The task status will be reset to `null` and can be restarted later.

### 6. Delete an Ingestion Task

Stop and permanently delete an ingestion task:

```bash
curl -X DELETE "http://localhost:8080/delete_ingestion?token=YOUR_GULP_TOKEN&bridge_task_id=task123456"
```

## API Reference

### List Bridges

**Endpoint:** `POST /list_bridges`

Lists all registered bridges. Supports filtering.

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `token` | query/header | string | Yes | Authentication token with INGEST permission |
| `flt` | body | `GulpCollabFilter` | No | Filter criteria |
| `req_id` | query | string | No | Request tracking ID |

**Filter Examples:**

```json
// Filter by status
{ "status": "ready" }

// Filter by name
{ "names": ["my_bridge"] }

// Filter by URL pattern (if supported)
{ "url": "http://localhost:8888" }
```

---

### List Ingestion Tasks

**Endpoint:** `POST /list_ingestion_tasks`

Lists ingestion tasks. Supports filtering.

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `token` | query/header | string | Yes | Authentication token with INGEST permission |
| `flt` | body | `GulpCollabFilter` | No | Filter criteria |
| `req_id` | query | string | No | Request tracking ID |

**Filter Examples:**

```json
// Filter by bridge
{ "bridge_id": "abc123" }

// Filter by operation
{ "operation_ids": ["op123"] }

// Filter by status
{ "status": "ongoing" }
```

---

### Create and Start Ingestion

**Endpoint:** `POST /create_start_ingestion`

Creates an ingestion task and instructs the bridge to start ingesting data.

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `token` | query/header | string | Yes | Authentication token with INGEST permission |
| `bridge_id` | query | string | Yes | ID of the bridge to use |
| `operation_id` | query | string | Yes | Target Gulp operation ID |
| `plugin_params` | body | `GulpPluginParameters` | Yes | Ingestion configuration |
| `req_id` | query | string | No | Request tracking ID |

**Body (`plugin_params`) Example:**

```json
{
  "plugin": "raw",
  "mapping_parameters": {
    "mapping_file": "windows.json",
    "mapping_id": "windows"
  },
  "custom_parameters": {
    "query": "SELECT * FROM events",
    "interval": 10
  }
}
```

---

### Stop Ingestion

**Endpoint:** `POST /stop_ingestion`

Stops a running ingestion task.

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `token` | query/header | string | Yes | Authentication token with INGEST permission |
| `bridge_task_id` | body | string | Yes | Task ID to stop |
| `req_id` | query | string | No | Request tracking ID |

---

### Delete Ingestion

**Endpoint:** `DELETE /delete_ingestion`

Stops and deletes an ingestion task.

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `token` | query/header | string | Yes | Authentication token with INGEST permission |
| `bridge_task_id` | query | string | Yes | Task ID to delete |
| `req_id` | query | string | No | Request tracking ID |

## Configuration

The Bridge Manager can be configured via `gulp_cfg.json`:

```json
{
  "bridge_manager": {
    "bridge_timeout_sec": 30
  }
}
```

| Setting | Default | Description |
|---------|---------|-------------|
| `bridge_timeout_sec` | 30 | HTTP timeout when calling bridge endpoints |

## Troubleshooting

### Bridge Not Found

**Error:** `Bridge not found with id: xxx`

**Solution:**
1. Check if the bridge registered successfully: `POST /list_bridges`
2. Ensure the bridge can reach Gulp to register

### Task Already Ongoing

**Error:** `bridge task already ongoing with id xxx`

**Solution:**
1. Stop the existing task: `POST /stop_ingestion`
2. Or delete it: `DELETE /delete_ingestion`
3. Then create a new task

### Bridge Connection Timeout

**Error:** `bridge start_ingestion error: timeout`

**Solution:**
1. Increase `bridge_timeout_sec` in configuration
  
  ~~~json
  {
    "bridge_manager": {
      "bridge_timeout_sec": 60
    }
  }
  ~~~
2. Verify network connectivity between Gulp and bridge
3. Check bridge logs for errors

### Task Status Shows Failed

**Cause:** The bridge reported an error during ingestion.

**Solution:**
1. Check the task's `error` field via `/list_ingestion_tasks`
2. Review bridge logs for detailed error messages
3. Verify the external source is accessible from the bridge

### Data Not Appearing in Gulp

**Cause:** Data may be buffered or ingestion may have failed silently.

**Solution:**
1. Verify task status is `ongoing`: `/list_ingestion_tasks`
2. Check bridge logs to confirm data is being sent
3. Verify the operation and context exist in Gulp
4. Check Gulp's OpenSearch for ingested documents

---

## Related Documentation

- [Integration Guide](./integration.md) - How bridges feed data to Gulp
- [Plugins and Mapping](./plugins_and_mapping.md) - Understanding ingestion plugins and field mapping
- [Architecture](./architecture.md) - Gulp's overall architecture
- [gulp-sdk-python](https://github.com/mentat-is/gulp-sdk-python) - Python SDK for building bridges

---

## Building a Bridge

If you want to create your own bridge, it must implement the following HTTP endpoints:

### Required Bridge Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/start_ingestion` | POST | Start ingesting data for a task |
| `/stop_ingestion` | POST | Stop ingesting data for a task |

### `/start_ingestion` Request

Gulp will call this endpoint with:

```json
{
  "bridge_id": "your_bridge_id",
  "bridge_task_id": "task_id",
  "operation_id": "gulp_operation_id",
  "plugin_params": {
    "plugin": "raw",
    "mapping_parameters": { ... },
    "custom_parameters": { ... }
  }
}
```

**Headers:**
- `Authorization: Bearer <bridge_token>` (the token your bridge provided at registration)
- `Content-Type: application/json`

### `/stop_ingestion` Request

```json
{
  "bridge_id": "your_bridge_id",
  "bridge_task_id": "task_id"
}
```

### Bridge Registration

Your bridge should register with Gulp on startup:

```bash
curl -X POST "http://gulp-host:8080/register_bridge?token=INGEST_TOKEN&name=my_bridge&url=http://my-bridge:8888&bridge_token=MY_SECRET_TOKEN"
```

The `bridge_token` is a secret your bridge generates and provides to Gulp. Gulp will use this token in the `Authorization` header when calling your bridge's endpoints.

### Sending Data to Gulp

Once ingestion is started, your bridge should fetch data from the external source and send it to Gulp:

**Via REST API:**
```bash
curl -X POST "http://gulp-host:8080/ingest_raw?token=INGEST_TOKEN&operation_id=xxx&context=my_context&source=my_source" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "@timestamp": "2024-01-15T10:30:00.000Z",
      "gulp.timestamp": 1705312200000000000,
      "event.code": "file_created",
      "file.path": "/var/log/auth.log"
    }
  ]'
```

**Via WebSocket:**
Connect to `ws://gulp-host:8080/ws_ingest_raw` for real-time streaming.

For complete bridge development guidance, see the [gulp-sdk-python](https://github.com/mentat-is/gulp-sdk-python) repository.