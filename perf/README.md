# Performance Notes

This desktop app is optimized for a single local operator, not for internet-scale traffic.

## What Changed

- The desktop shell now attaches only to the fixed local service port `8765`.
- The backend no longer auto-shifts to `8766+`, which avoids split-brain local sessions.
- Focus-mode polling now uses one aggregated endpoint, `/api/focus-snapshot`, instead of firing multiple independent requests on every cycle.

## Why 10M Users Is A Different System

To support large-scale traffic, this project would need:

- a real API service layer instead of `http.server`
- centralized state storage instead of local JSON files
- queue-based strategy execution
- external market-data fanout
- multi-instance service discovery and health checks
- distributed load generation

## Suggested Load-Testing Path

1. Keep this app as the operator desktop shell.
2. Extract API handlers into a deployable service.
3. Put strategy execution behind a worker queue.
4. Load test the extracted API separately with distributed generators.

## Included Script

See `k6-focus-snapshot.js` for a starting point that pounds the focus snapshot and health endpoints after service extraction.
