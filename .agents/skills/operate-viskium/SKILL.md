---
name: operate-viskium
description: Use local Viskium MCP tools to inspect bounded status, read one structured observation, or request one explicitly authorized camera snapshot. Apply whenever a task asks an agent to see through Viskium, inspect its perception state, or use the local camera through this repository.
---

# Operate Viskium

Use only the public Viskium MCP tools. The server owns the camera handle; do not open the camera through shell commands, OpenCV scripts, browser APIs, or another tool while Viskium is serving it.

Before the first camera call, confirm that the five Viskium tools are available. A fresh clone keeps
the project MCP server disabled; an unavailable or disabled server means the camera tools are absent
for this task. In that case, stop instead of substituting another camera path, a CLI capture, or a
generic camera tool. Explain that the user must prepare the environment and data root, grant
consent, set `mcp_servers.viskium.enabled = true` in `.codex/config.toml`, trust the project, and
restart the Codex host. Do not perform those setup or consent actions merely to satisfy the request.

`viskium_vision_challenge_v1` and `viskium_verify_vision_challenge_v1` are synthetic, bounded
challenge tools. They do not require consent, open the camera, or replace the camera ordering below.

1. Call `viskium_status_v1` first. This call is read-only and must not open the camera.
2. Prefer `viskium_latest_observation_v1` when a structured observation can answer the request and status reports a configured producer. If status reports `producer: not_configured`, skip this tool because the current server cannot populate it. Keep `wait_ms` and `max_age_ms` no larger than the task needs.
3. Call `viskium_snapshot_v1` only when the user needs current visual evidence. Request one image at a time and the smallest useful `max_edge_px`.
4. A snapshot attempt consumes quota before hardware access, including busy and timeout outcomes. Retry a transient result at most once only when the user explicitly provisioned enough quota; otherwise stop. Treat denials, exhausted quota, missing extras, and unavailable hardware as final. Do not loop, raise configured limits, switch camera APIs, or grant consent yourself.
5. Keep returned image bytes ephemeral. Do not save, upload, log, cache, or embed them in another artifact unless the user separately asks for that output.

Consent is an out-of-band user action. If it is absent, explain the exact local command the user may choose to run; never run it merely to satisfy an agent call:

```text
uv run --no-sync viskium consent grant --data-root .viskium --scope observation.read --scope snapshot.read --duration-seconds 300 --snapshot-quota 2 --sensitivity-ceiling identifiable
```

Viskium exposes no camera-open, camera-close, record-video, continuous-stream, or consent-grant MCP tool. Do not simulate continuous capture by repeatedly calling the snapshot tool. The library contains bounded continuous-capture components, but the current MCP composition does not start that pipeline.

After camera work, remind the user that they can close access immediately rather than waiting for expiry:

```text
uv run --no-sync viskium consent revoke --data-root .viskium
```

They can then restore `enabled = false` in `.codex/config.toml` and restart Codex. Do not revoke consent or edit that configuration unless the user explicitly asks.
