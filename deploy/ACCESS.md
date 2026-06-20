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
| **Health** | `/healthz` |
| **Swagger UI** | `/swagger/index.html` |
| **OpenAPI spec** | `/swagger/doc.json` |
| **API base path** | `/api/v1` |

## From WSL / Linux (node IP is routable)

```bash
MIP=$(minikube ip)            # 192.168.49.2
# Swagger UI:
xdg-open  "http://$MIP:30080/swagger/index.html"   # or just open in a browser
# Health / API:
curl "http://$MIP:30080/healthz"
curl "http://$MIP:30080/api/v1/gpus"
```

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
via `http://192.168.49.2:30080`. Note the API handlers themselves are still
stubs and return `501 Not Implemented` until implemented.
