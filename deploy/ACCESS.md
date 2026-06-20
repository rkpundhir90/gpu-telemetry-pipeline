# Cluster access reference (for Swagger / API calls)

Quick reference for reaching the deployed API — including the Swagger UI — once
the service is running on minikube. Use this when you want to make API calls
from Swagger at a later stage.

## Key facts

| | |
|---|---|
| **minikube node IP** | `192.168.49.2` (docker driver default; re-check with `minikube ip` — it can change if the cluster is recreated) |
| **Namespace** | `gpu-telemetry` |
| **Service** | `gpu-telemetry-gpu-telemetry-api` (NodePort) |
| **NodePort** | `30080` → container `8080` |
| **Health** | `/healthz` (liveness), `/readyz` (readiness — pings the DB) |
| **Swagger UI** | `/swagger/index.html` |
| **OpenAPI spec** | `/swagger/doc.json` |
| **API base path** | `/api/v1` |
| **Endpoints** | `GET /api/v1/gpus`, `GET /api/v1/gpus/{id}/telemetry` |

## From WSL / Linux (node IP is routable)

```bash
MIP=$(minikube ip)            # 192.168.49.2
# Swagger UI:
xdg-open  "http://$MIP:30080/swagger/index.html"   # or just open in a browser
# Health / API:
curl "http://$MIP:30080/healthz"
curl "http://$MIP:30080/readyz"
curl "http://$MIP:30080/api/v1/gpus"
# Telemetry for a GPU (newest first; optional window + limit):
curl "http://$MIP:30080/api/v1/gpus/<uuid>/telemetry?limit=20"
curl "http://$MIP:30080/api/v1/gpus/<uuid>/telemetry?start_time=2025-07-18T20:42:34Z&end_time=2025-07-18T21:00:00Z"
```

> Endpoints return data only once the Streamer + Collector have populated
> TimescaleDB. `GET /api/v1/gpus` lists the GPUs seen so far; use one of those
> UUIDs as `<uuid>` above.

## From the Windows host (browser → Swagger)

The docker driver's cluster network isn't routable from Windows, so use
minikube's native tunnel (binds `127.0.0.1` on the WSL host, reachable from
Windows via WSL2 localhost forwarding):

```bash
make service-url      # prints e.g. http://127.0.0.1:33905  — keep it running
```

Then in a Windows browser open:

```
http://localhost:<port>/swagger/index.html
```

> The tunnel **port is assigned by minikube and changes each run**, and the
> `minikube service` process must stay open. The node IP + NodePort (`30080`)
> above are the stable values; the tunnel port is only for Windows-host access.

## Making calls from Swagger ("Try it out")

The OpenAPI base path is `/`, so Swagger issues requests against the **same
origin** that served the page. No extra config needed:

- Opened via NodePort → calls go to `http://<minikube-ip>:30080/...`
- Opened via the tunnel → calls go to `http://localhost:<port>/...`

Both work because the same host/port serves the UI and the API.

## Verified

`/swagger/index.html`, `/swagger/doc.json`, and `/healthz` all return `HTTP 200`
via `http://192.168.49.2:30080`. The data endpoints (`/api/v1/gpus`,
`/api/v1/gpus/{id}/telemetry`) are implemented and read from TimescaleDB — they
return live data once the Streamer/Collector have populated the store, and `[]`
before then. `/readyz` returns `503` if the datastore is unreachable.
