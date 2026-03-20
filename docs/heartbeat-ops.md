# Heartbeat Operations

This document captures the latest heartbeat automation we just deployed for the Qlik MCP workspace.

## Key changes
1. **Config cleanup:** `agents.defaults.heartbeat` now uses only supported keys (`every`, `target`, `accountId`, `to`, `prompt`, `lightContext`). The old `isolatedSession` flag was removed to keep the Gateway from rejecting the heartbeat configuration.
2. **Daemon shift:** `scripts/heartbeat_daemon.sh` now logs each invocation, runs between 07:00 and 22:00, and fires the `heartbeat.sh` helper every 15 minutes (down from 30) to guarantee timely Telegram reports.
3. **Validation/doctor:** Running `openclaw doctor` ensures the new settings are valid, and a manual invocation of `scripts/heartbeat.sh` demonstrates the `openclaw system event --mode now` call succeeds against the current Kanban text.

## Running the automated heartbeat
- Start the daemon with `nohup bash scripts/heartbeat_daemon.sh >/logs/heartbeat-daemon.log 2>&1 &` and confirm it appears via `ps -ef | grep heartbeat_daemon.sh`.
- Each run writes a timestamped entry to `logs/heartbeat-daemon.log` and creates a heartbeat log entry + Kanban update, so you can verify at https://github.com/frankyh75/qlik-mcp-server/kanban/status.md.
- If the Telemessage still fails, check `logs/heartbeat-daemon.log` and the gateway log around the reported timestamp; the script logs the canonical chat id, so the failure reason will appear there.

## Tests/documentation produced
- Manual `scripts/heartbeat.sh` run once to demonstrate the event send (recorded in `logs/heartbeat.md`).
- `openclaw doctor` executed to prove there are no config errors (reporting the new `heartbeat.directPolicy` and session notes).
- Kanban entry updated automatically via `scripts/update-kanban-status.sh` (see Auto section in `docs/kanban/status.md`).

## Next steps
- Watch the next automated tick (~15 minutes after daemon start) to ensure Telegram receives the message.
- If new Kanban state is desired, add an entry to `backlog/qlik-ideas.md` or a project card describing the feature so the Heartbeat text stays relevant.
