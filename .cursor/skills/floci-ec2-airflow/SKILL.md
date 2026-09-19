---
name: floci-ec2-airflow
description: >-
  Operates OpenWeather ETL pipeline with Airflow 3.3.1 on Floci-emulated EC2
  (SSH, standalone, 8080 tunnel, weather_dag, local S3). Use when the user
  mentions Floci, EC2, Airflow, reconnect after reboot, Remote SSH, weather_dag,
  or S3 my-s3-for-cv/weather.
---

# Floci EC2 + Airflow 3.3.1

Project: `Data-Engineering_ETL_AWS-EC2_e_Airflow`. [Floci](https://floci.io) emulator; **not** paid AWS.

## Important Paths

| What | Local Repo | EC2 (container) |
|------|------------|-----------------|
| DAG (Git) | `airflow/dags/weather_dag.py` | `/root/airflow/dags/weather_dag.py` |
| Secrets | `.env` (gitignore) | `/root/airflow/.env` (gitignore) |
| EC2 Template | `airflow/.env.example` | copy and fill in the instance |
| Airflow home | — | `/root/airflow` (`AIRFLOW_HOME`) |
| Python venv | — | `/root/.venv` (Python 3.12) |
| InstanceId | — | `i-445fcef7e259b7f85` |
| SSH | `~/.ssh/config` host `floci-ec2-eddy` | port **2200**, user **root** |
| S3 bucket | `my-s3-for-cv` | EC2 endpoint: `http://172.17.0.1:4566` |

**Synchronize DAG:** copy `airflow/dags/weather_dag.py` → `/root/airflow/dags/` (Remote SSH or paste). Never commit `.env` with API keys.

## After PC Reboot (checklist)

```bash
docker ps
cd ~/1_GitHub/Data-Engineering_ETL_AWS-EC2_e_Airflow
floci start --persist ./floci-data
eval $(floci env)
aws ec2 start-instances --instance-ids i-445fcef7e259b7f85
sleep 5
docker ps | grep floci-ec2
```

If SSH fails:

```bash
docker exec floci-ec2-i-445fcef7e259b7f85 bash -c "mkdir -p /run/sshd && /usr/sbin/sshd"
ssh -i ~/.ssh/id_ed25519 -p 2200 root@127.0.0.1
```

**Do not** use `run-instances` to reconnect — this creates a new instance. Use `start-instances` on the existing instance.

## Airflow Standalone + UI

Terminal 1 (inside SSH):

```bash
source /root/.venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow standalone
```

Admin password: `cat ~/airflow/simple_auth_manager_passwords.json.generated` (use `cat`, don't execute the file).

Terminal 2 (on PC, tunnel — keep open):

```bash
ssh -i ~/.ssh/id_ed25519 -p 2200 -L 8080:localhost:8080 root@127.0.0.1
```

Browser: **http://localhost:8080** · user **admin**.

## DAG `weather_dag`

- Tasks: `extract_weather` → `transform_to_csv` → `load_to_s3`
- Airflow 3: `schedule=@daily`, decorators `@task`
- OpenWeather: geocoding `Vitória,BR` + weather by `lat/lon` (no `q=` in `/weather`)
- UI: **toggle ON** (resume) + **Trigger** (manual trigger)
- **Mapped = false** is normal (not dynamic task mapping)

Validate S3 on PC:

```bash
eval $(floci env)
aws s3 ls s3://my-s3-for-cv/weather/ --recursive
```

## VS Code Remote SSH

Host `floci-ec2-eddy` in `~/.ssh/config` → open folder `/root` or `/root/airflow`.

## Security

- Never commit `.env`, private SSH keys, or Airflow passwords.
- OpenWeather key only in local `.env` and `/root/airflow/.env` on EC2.

## Docs in Repo

- [README.md](../../README.md) — complete guide
- [using_floci.md](../../using_floci.md) — S3 and common errors
