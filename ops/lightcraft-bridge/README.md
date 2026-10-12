# Isolated LightCraft image worker (POC only)

This directory contains a **local-only prototype**, not a customer-facing image service.
It uses the upstream `lightcraft-cli` binary inside a Docker container and a small SQLite
queue to prove the asynchronous workflow on an inexpensive CPU-only VPS.

## Runtime requirements
- Linux + systemd, Docker CLI/daemon, Python 3 with Pillow.
- A previously built local Docker image tagged `lightcraft-cli:poc-76b5377`.
- Install worker as `/opt/lightcraft/bridge/worker.py`.
- Create private directories (0700): `/opt/lightcraft/bridge/{incoming,jobs,results}`.
- Copy the supplied systemd service and timer to `/etc/systemd/system/` and enable the timer.
- No HTTP server, reverse proxy, public TCP port, SaaS credentials, or customer files are included.

## Worker states and data retention

`QUEUED → PROCESSING → READY_FOR_DELIVERY → DONE` (or `FAILED`).

- `submit tenant idempotency_key incoming_filename`: validate an image (JPEG, PNG, WebP; max 40 MB / 40 MP),
  copy into its checksummed private job directory, persist metadata in SQLite WAL, and remove
  the temporary incoming upload once that copy is safely committed.
- `run-once`: atomically claim one queued job, render a resized JPEG (long edge <=1600),
  verify the actual JPEG, dimensions, hash and EXIF policy. Work is contained by
  `--network none --read-only --memory 4g --cpus 2 --pids-limit 128 --cap-drop ALL`,
  running as UID 10001 with an isolated writable output directory.
- `status tenant job_id`: return only tenant-scoped metadata, no image data or filesystem path.
- `retry tenant job_id`: requeue a failed job only if its source still exists.
- **`READY_FOR_DELIVERY` means the optimized result is verified *locally*, not yet delivered.**
- **`ack-delivery tenant job_id SHA256` is a trusted *internal* operation only.**
  It must be invoked *after* an authorized destination library has durably stored the
  optimized image and verified its SHA-256. The worker then records `DONE` and removes
  the staged original and local optimized copy. Duplicate valid acknowledgments are idempotent.
- A checksum mismatch must never trigger deletion. A successful render by itself must never trigger deletion.

## Interface to a future SaaS integration

The publicly exposed agent interface, if chosen, should be a single high-level
`manage_image_optimization` tool with `submit`, `status`, `retry` actions.
**Never expose the raw local `ack-delivery` command to end-user agents**.
`nhadat.store` must authorize the tenant and the project/media IDs using server-side
credentials before creating/fetching jobs. Users must not supply arbitrary filesystem paths.
An authenticated, encrypted inter-server transport or signed object-store workflow must be
built before actual tenants can submit photos. Transfer images outside the LLM context.
The library backend must verify and acknowledge only after the destination commit succeeds.
For lower pressure on the website VPS, direct upload to a private staging store rather
than proxying image bytes through the application server.

## Operational installation
Systemd:
- `lightcraft-queue-worker.service`: processes one image per invocation.
- `lightcraft-queue-worker.timer`: retries checks roughly every 30 seconds when idle,
  and starts automatically after reboot.
- All jobs remain on private local storage and no listener is opened.

Example after placing **synthetic test** `test.png` into the private incoming directory:

```sh
python3 /opt/lightcraft/bridge/worker.py submit qa-tenant example-key test.png
systemctl start lightcraft-queue-worker.service
python3 /opt/lightcraft/bridge/worker.py status qa-tenant JOB_UUID
# Upload optimized result to a separate trusted library and verify SHA256 first.
# Only the trusted backend may then call:
python3 /opt/lightcraft/bridge/worker.py ack-delivery qa-tenant JOB_UUID VERIFIED_SHA256
```

## Observed POC acceptance

- Local 24-megapixel synthetic PNG → 1600×1067 JPEG: **152,019 bytes**.
- Two isolated QA tenants with identical idempotency keys: unique job IDs; wrong-tenant lookup rejected.
- Repeated same-tenant idempotency key: no duplicate; different content: rejected.
- Invalid tenant and invalid input encoding: rejected.
- Queue driven by systemd timer: **DONE** in an earlier pilot; after adding delivery gating:
  **READY_FOR_DELIVERY** verified before simulated destination ACK.
- Wrong ACK digest: rejected with both originals still intact.
- A simulated copy to a local QA library with matching hash, then ACK: **DONE**, temp files removed;
  repeated ACK: idempotent. This is **not** proof of a remote production library transfer.
- No new public listening port; host SSH + existing loopback service only.

## Unfinished gates — NOT production ready

1. Implement authenticated/mTLS or signed-object delivery bridge between the SaaS backend and this worker.
2. Integrate project/library authorization with existing `nhadat.store` media tools and workflows.
3. Add secure upload, durable destination ACK, source/output retention/garbage collection, backups, and alerting.
4. Test real estate photos, EXIF/GPS stripping, all supported file types, malformed/oversized inputs,
   concurrency, crash recovery, max processing time, and rate/tenant quotas.
5. Move deployment into versioned image digest with rollback and CI.
6. Verify source and brand licensing before redistributing.
7. Do not change production billing, permissions, domain mappings, or publication behavior.

This branch deliberately does not auto-deploy, expose ports, or change any tenant.
