# Cluster Viewer

Simple cluster monitoring dashboard intended to run directly on an HPC login node or gateway host.

## What it shows

- Host health: uptime, load average, memory pressure, root disk usage
- Slurm queue summary: job counts by state, active users, your current jobs
- Node summary from `sinfo`: state, CPU allocation, memory, features
- Raw scheduler command diagnostics to make debugging easy

## Why this shape

This app is intentionally zero-dependency. It uses Python's standard library only, which makes it easier to deploy on a cluster where package installation is inconvenient or restricted.

## Run locally

```bash
python3 app.py
```

Then open `http://127.0.0.1:8000`.

You can also choose a bind address and port:

```bash
python3 app.py --host 127.0.0.1 --port 8080
```

## Deployment on the cluster

Recommended pattern:

1. Run the app on a login node or internal gateway where `sinfo` and `squeue` work.
2. Bind it to `127.0.0.1`.
3. Put nginx, Apache, or your institution's existing reverse proxy in front of it.
4. Restrict access with campus SSO, VPN, or IP rules if this should stay internal to the lab.

Example systemd user service:

```ini
[Unit]
Description=Cluster Viewer
After=network.target

[Service]
WorkingDirectory=/path/to/columbiaclusterviewer
ExecStart=/usr/bin/python3 /path/to/columbiaclusterviewer/app.py --host 127.0.0.1 --port 8000
Restart=always

[Install]
WantedBy=default.target
```

## Slurm assumptions

The scheduler collector currently assumes Slurm and reads:

- `sinfo --Node --Format=%n|%t|%C|%m|%f`
- `squeue --noheader --Format=%i|%T|%u|%P|%M|%D|%R`
- `squeue --noheader --user $USER --Format=%i|%T|%P|%M|%D|%R`

If those commands are unavailable, the dashboard still loads and shows scheduler diagnostics so you can adapt the collector for your environment.

## Next steps worth adding

- GPU metrics via `nvidia-smi` or scheduler GRES summaries
- Historical sampling written to a small SQLite database
- Alerts for login node overload or abnormal queue growth
- Authentication in front of the dashboard
