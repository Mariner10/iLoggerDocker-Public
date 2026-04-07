# iLoggerDocker

A Dockerized telemetry stack that ingests iCloud device location data, stores it in PostGIS/TimescaleDB, and routes events between modules over a WebSocket mesh.

This README walks you through standing the stack up from nothing on a fresh machine.

---

## 1. What you get

When `docker compose up` finishes, you will have the following containers running on a shared `device_network` bridge:

| Service | Container | Role |
|---|---|---|
| `db` | `tracking_postgis` | PostgreSQL 16 + PostGIS + TimescaleDB. Persists all telemetry. Exposes `5432`. |
| `database_interactor` | `database_interactor` | Mesh node that owns all reads/writes to `db`. |
| `icloud_interactor` | `icloud_interactor` | Polls iCloud (via `pyicloud`) for device location and publishes updates onto the mesh. |
| `socket_server` | `socket_server` | Central WebSocket message broker. Exposes `8765`. |
| `socket_ears` | `socket_ears` | Ingests logs from every module and rotates them into `vault_data`. |
| `watchdog` | `watchdog` | Mounts the host Docker socket and restarts the `socket_server` if it becomes unhealthy. |

---

## 2. Prerequisites

You need the following installed on the host before cloning:

- **Docker Engine** 24+ and the **Compose v2** plugin (`docker compose ...`)
- **git**
- A free TCP port `5432` (Postgres) and `8765` (mesh socket) on the host
- An **Apple ID** with an iCloud-enabled device you want to track
  - 2FA must already be handled — see §5 for the first-run login flow
- ~3 GB of RAM available for the database container (see `mem_limit` in `docker-compose.yml`)

That's it. Everything else (Python, Postgres, PostGIS, Timescale, the mesh libraries) is baked into the images.

---

## 3. Clone the repo

```bash
git clone <your-fork-or-origin-url> iLoggerDocker
cd iLoggerDocker
```

Repository layout:

```
iLoggerDocker/
├── docker-compose.yml
├── lib/                     # Shared SocketCore library, copied into each image at build time
└── modules/
    ├── db/                  # Postgres + PostGIS + Timescale image
    ├── database_interactor/ # DB mesh node
    ├── icloud_interactor/   # iCloud polling mesh node
    ├── socket_server/       # Mesh broker
    ├── socket_ears/         # Centralized logging sink
    └── watchdog/            # Container health supervisor
```

---

## 4. Create the `.env` file

Compose reads all secrets from a `.env` file next to `docker-compose.yml`. Create one:

```bash
cat > .env <<'EOF'
# --- Postgres ---
POSTGRES_USER=ilogger
POSTGRES_PASSWORD=change-me-to-something-long
POSTGRES_DB=ilogger

# --- iCloud credentials ---
# Apple ID email and password used by icloud_interactor
APPID=you@example.com
APPSECRET=your-apple-id-password

# Device name as it appears in Find My (e.g. "Carter's iPhone")
MAIN_DEVICE=My iPhone
EOF
```

Notes:

- `APPSECRET` is your Apple ID password, **not** an app-specific password — `pyicloud` signs in as a browser would.
- If your Apple ID has 2FA enabled (it should), the first run will need an interactive code entry. See §5.
- Keep `.env` out of git. Add it to `.gitignore` if it isn't already.

---

## 5. First-time iCloud 2FA handshake

`icloud_interactor` stores its session in a volume inside the container. On a brand-new machine, the first login will fail because Apple will demand a 2FA code. The simplest way to bootstrap the session:

1. Start only the iCloud container attached to your terminal:
   ```bash
   docker compose run --rm icloud_interactor python -m pyicloud --username "$APPID"
   ```
2. Enter your password and the 2FA code Apple sends to a trusted device.
3. Once `pyicloud` reports a successful login, exit. The session cookie is now cached and the normal `docker compose up` will run headless.

Re-run this procedure any time Apple invalidates the session (typically every ~30 days, or after a password change).

---

## 6. Build and run the stack

From the project root:

```bash
docker compose build
docker compose up -d
```

What happens on first boot:

1. `db` initializes the PostGIS + Timescale cluster using the credentials from `.env` and runs any bootstrap SQL baked into `modules/db/`.
2. `database_interactor` waits for `db` to report healthy (`pg_isready`), then connects.
3. `socket_server` opens port `8765`.
4. `socket_ears`, `icloud_interactor`, and `watchdog` come up and register with the mesh.

Check status:

```bash
docker compose ps
docker compose logs -f socket_server icloud_interactor database_interactor
```

All services should eventually report `(healthy)`. If `icloud_interactor` is restarting, re-do §5.

---

## 7. Verifying it works

- **Mesh broker is listening:**
  ```bash
  nc -vz localhost 8765
  ```
- **Database is reachable:**
  ```bash
  docker exec -it tracking_postgis psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\dt'
  ```
  You should see the telemetry tables created by `database_interactor`.
- **Location data is flowing:** tail the database interactor logs — you should see insert activity shortly after `icloud_interactor` publishes its first position:
  ```bash
  docker compose logs -f database_interactor
  ```
- **Centralized logs:** the rotated log files live in the `vault_data` volume, written by `socket_ears`:
  ```bash
  docker run --rm -v ilogger_vault_data:/logs alpine ls /logs
  ```
  (Replace `ilogger_vault_data` with the actual volume name from `docker volume ls` if your project prefix differs.)

---

## 8. Day-to-day operations

| Task | Command |
|---|---|
| Start | `docker compose up -d` |
| Stop | `docker compose down` |
| Rebuild after code changes | `docker compose up -d --build` |
| Tail everything | `docker compose logs -f` |
| Tail one service | `docker compose logs -f <service>` |
| Shell into a container | `docker compose exec <service> sh` |
| Wipe the database (destructive) | `docker compose down -v` |

The `postgres_data` and `vault_data` volumes persist across `down`/`up` cycles. Only `down -v` removes them.

---

## 9. Troubleshooting

- **`database_interactor` keeps restarting with connection refused** — `db` hasn't finished initializing yet. Give it 30–60s on the first boot; Compose's health check gating will flip it to healthy automatically.
- **`icloud_interactor` logs `2SA required`** — re-run the interactive 2FA step in §5.
- **Port 5432 or 8765 already in use** — edit the `ports:` mapping in `docker-compose.yml` (e.g. `"15432:5432"`) and point external clients at the new host port.
- **Out-of-memory kills on `db`** — raise `mem_limit`/`memswap_limit` for the `db` service in `docker-compose.yml`. The defaults (3 GB) are a floor, not a target.
- **`watchdog` can't see other containers** — confirm `/var/run/docker.sock` is mounted and that Docker Desktop (macOS/Windows) has granted socket access to the container.

---

## 10. Next steps

Once the core stack is up you can:

- Point a WebSocket client at `ws://<host>:8765` and speak the mesh protocol directly.
- Connect any SQL client to `postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@<host>:5432/$POSTGRES_DB` for ad-hoc queries against the telemetry history.
- Add your own mesh node by copying one of the `modules/*` directories as a template — each service is a thin Python process that imports `lib/SocketCore` and registers with `socket_server`.
