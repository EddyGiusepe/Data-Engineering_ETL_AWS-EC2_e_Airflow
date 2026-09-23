<h1 align="center">Data Engineering: ETL AWS EC2 and Airflow</h1>

<font color="pink">Senior Data Scientist.: Dr. Eddy Giusepe Chirinos Isidro</font>


![](architecture_and_workflow_stack/etl_pipeline_architecture.png)

ETL pipeline (OpenWeather → transformation → S3) orchestrated with Apache Airflow. The reference tutorial is the video [How to build and automate a python ETL pipeline with airflow on AWS EC2](https://www.youtube.com/watch?v=uhQ54Dgp6To).

In this repository we practice **without spending money on Amazon**, using [Floci](https://floci.io): a local AWS emulator (S3, EC2, …). The `aws s3 …` and `aws ec2 …` commands are the **same** as in the cloud; only the destination (`localhost:4566`) and credentials (`test` / `test`) change.

Long S3 details (mistakes we've already made, persistence): [using_floci.md](using_floci.md).

---

## What the video does vs. what we do here

| In the video (real AWS) | Here (Floci, local, free) |
|---|---|
| AWS Console, Ubuntu, `t2.small`, public IP | CLI + Docker container that **pretends** to be EC2 |
| SSH `ubuntu@ec2-….amazonaws.com` | SSH `root@127.0.0.1` on port **2200** (or `2200–2299`) |
| S3 on Amazon (costs money) | S3 on Floci (**Storage** in the UI) |
| Airflow 2.x on EC2 | **Airflow 3.3.1** + **uv** on Floci EC2 |
| `.pem` downloaded from AWS | Local SSH key pair `~/.ssh/id_ed25519` |

We reproduce the **flow** of the video (S3 + EC2 + SSH + ETL afterward), not the AWS console screen nor the old package versions.

```
OpenWeather API  →  Airflow 3.3.1 (on EC2)  →  CSV  →  S3
     (weather)              (orchestrates)             (stores)
```

---

## Status of what we've done so far

| Step | Status |
|---|---|
| Floci with persistence (`./floci-data`) | Done |
| S3 bucket `my-s3-for-cv` + CV upload | Done |
| SSH key `floci-eddy` imported into Floci | Done |
| Instance `ec2-instance-eddy` (`running`) | Done |
| SSH `root@127.0.0.1:2200` | Done |
| Local `.env` with OpenWeather API (repo root) | Done (not committed) |
| Airflow 3.3.1 + `weather_dag` DAG → S3 | Done |
| DAG versioned in [`airflow/dags/`](airflow/dags/weather_dag.py) | Done |

---

## S3 vs. EC2 (so you don't mix them up)

- **S3** = a drawer. It stores objects (PDF, CSV, image). It doesn't run programs.
- **EC2** = a computer. It powers on, you log in via SSH, install Python/Airflow. It's the "where the job runs."

Example already done: the CV `Data_Science_Eddy_en.pdf` is in local S3:

- bucket: `my-s3-for-cv`
- key: `eddy-cv-directory/Data_Science_Eddy_en.pdf`
- URI: `s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf`

In the Floci UI: S3 = **Storage**. EC2 = **Compute**. Key pair = **Networking → Key Pairs**. `http://localhost:4566` is an API, not a file folder.

---

## Environment variables and secrets (`.env`)

**Never commit** real keys. `.gitignore` already ignores `.env` and `floci-data/`.

### In the root of this repository (your machine)

Versioned template: [`.env.example`](.env.example)

```bash
cp .env.example .env
# Edit .env locally and fill in the value (do not paste it in chat or Git)
```

Expected content (no real value in the repo):

```env
API_KEY_OPENWEATHER=your_key_here
```

Used by the OpenWeather test script (`testing_openweather_api.py`, when present in the repo) with `python-dotenv`. The key comes from [home.openweathermap.org](https://home.openweathermap.org).

### Floci credentials (automatic)

`eval $(floci env)` exports **fake** credentials just for the local emulator:

- `AWS_ACCESS_KEY_ID=test`
- `AWS_SECRET_ACCESS_KEY=test`
- `AWS_ENDPOINT_URL=http://localhost:4566` (or `http://localhost.floci.io:4566`)

This is **not** a production secret; it's Floci's default.

### SSH key (your machine)

| File | Location | Goes to Git? | Goes to Floci? |
|---|---|---|---|
| `~/.ssh/id_ed25519` | private, only on your PC | **Never** | **No** |
| `~/.ssh/id_ed25519.pub` | public | optional (not necessary) | **Yes** (`import-key-pair`) |

`ssh -i ~/.ssh/id_ed25519` **reads** the private key locally; it is **not sent** to the instance.

### Inside the EC2 instance (later, for Airflow)

Once we install Airflow on the instance, variables like `AIRFLOW_HOME` and, if needed, `API_KEY_OPENWEATHER` can live in `~/airflow/.env` **inside the container** — also outside Git. We'll document this in section 4 without exposing values.

---

## 0. Prerequisites (one time)

- **Docker** running: `docker ps`
- **Floci CLI**: `curl -fsSL https://floci.io/install.sh | sh` → `floci --version`
- **AWS CLI v2** (official; `apt install awscli` on recent Ubuntu **fails**):

```bash
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version
```

Floci console (optional): `http://localhost:4500`. AWS API: `http://localhost:4566`.

Optional — avoid the pager (`less`) on `aws` commands:

```bash
export AWS_PAGER=""
```

---

## 1. Start Floci (every terminal / every reboot)

In the project folder:

```bash
cd ~/1_GitHub/Data-Engineering_ETL_AWS-EC2_e_Airflow
floci start --persist ./floci-data
eval $(floci env)
echo $AWS_ENDPOINT_URL
```

- `--persist ./floci-data` stores S3 (and EC2 state) on disk.
- `eval $(floci env)` points **this** terminal at the emulator.
- Every new tab: run `eval $(floci env)` again.

Stop: `floci stop`. After restarting your PC: `floci start --persist ./floci-data` + `eval $(floci env)`.

---

## 2. S3 — create a bucket and upload the CV

```bash
cd ~/1_GitHub/Data-Engineering_ETL_AWS-EC2_e_Airflow
eval $(floci env)

aws s3 mb s3://my-s3-for-cv
aws s3 cp Data_Science_Eddy_en.pdf s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf
aws s3 ls s3://my-s3-for-cv --recursive
```

**Two separate commands** (`mb` once; `cp` for each file). Don't combine `mb` and `cp` on the same line.

---

## 3. EC2 — bring up Ubuntu, name it, and log in via SSH

Goal: have a Linux machine accessible via SSH, just like in the video **before** Airflow.

Floci doesn't create a VM on Amazon: `run-instances` brings up a **Docker container**. `ami-ubuntu2204` = Ubuntu 22.04 from the Floci catalog. `t2.micro` is just metadata.

### 3.1 SSH key (required)

`aws ec2 create-key-pair` on Floci generates a **dummy** key — it **doesn't work** for SSH. Import **your own** public key.

Generate a key pair (if one doesn't already exist):

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N "" -C "floci-eddy"
ls -l ~/.ssh/id_ed25519 ~/.ssh/id_ed25519.pub
```

Import it into Floci (one time):

```bash
eval $(floci env)
aws ec2 import-key-pair \
  --key-name floci-eddy \
  --public-key-material fileb://$HOME/.ssh/id_ed25519.pub
```

Check it:

```bash
aws ec2 describe-key-pairs --output table
```

In the UI: **Networking → Key Pairs → `floci-eddy`**.

### 3.2 Create the instance with a friendly name

**Important (recent AWS CLI v2):** use `--count 1`, **not** `--min-count` / `--max-count` (error: `Unknown options: --min-count, --max-count`).

The name shown in the UI is the **`Name`** tag, not a `--name` parameter:

```bash
eval $(floci env)

aws ec2 run-instances \
  --image-id ami-ubuntu2204 \
  --instance-type t2.micro \
  --key-name floci-eddy \
  --count 1 \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=ec2-instance-eddy}]'
```

The JSON response may open in the pager (`less`) — a `:` prompt at the end. Press **`q`** to return to the terminal. The instance **has already been created**.

Check its state:

```bash
aws ec2 describe-instances \
  --query "Reservations[].Instances[].{Name:Tags[?Key=='Name']|[0].Value,Id:InstanceId,State:State.Name}" \
  --output table
```

In the UI: **Compute** → Name **`ec2-instance-eddy`**, State **`running`**.

Rename an already existing instance (alternative):

```bash
aws ec2 create-tags \
  --resources i-COLE_O_ID \
  --tags Key=Name,Value=ec2-instance-eddy
```

### 3.3 SSH

| Item | Value on Floci | In the video (real AWS) |
|---|---|---|
| User | **`root`** | `ubuntu` |
| Host | **`127.0.0.1`** | public IP `ec2-….amazonaws.com` |
| Port | **`2200–2299`** on host | `22` |
| Key | `-i ~/.ssh/id_ed25519` (private, **without** `.pub`) | `.pem` file |

Find the port (on **your PC**, outside SSH):

```bash
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

Real example from this project:

```text
floci-ec2-i-445fcef7e259b7f85   0.0.0.0:2200->22/tcp
```

Log in:

```bash
ssh -i ~/.ssh/id_ed25519 -p 2200 root@127.0.0.1
```

If you get a key permission error: `chmod 600 ~/.ssh/id_ed25519`.

Inside the instance (sanity check):

```bash
hostname          # e.g.: 2e8bfc63ff70 (container ID)
uname -a
cat /etc/os-release | head -3   # Ubuntu 22.04
ls                              # empty home is normal
exit                            # back to your PC
```

### 3.4 Restarting the instance (after rebooting your PC)

Use this flow **every time** you turn the computer back on. **Do not** run `run-instances` again — that creates a **new** machine and you lose whatever was inside (Airflow, DAGs, `.venv`).

| Situation | Command |
|----------|---------|
| Instance **already exists** (e.g. `ec2-instance-eddy`) | **`start-instances`** |
| First time / deleted with `terminate-instances` | **`run-instances`** (section 3.2) |

**Why does SSH fail after a reboot?** The Floci UI may show the instance as **`running`**, but the **Docker container** (`floci-ec2-i-…`) only comes back once you start Floci and issue **`start-instances`**. Sometimes the container comes up, but **internal SSH** hasn't yet — that's where step 4 below comes in.

Instance for this project (note down your own):

- **InstanceId:** `i-445fcef7e259b7f85`
- **Name:** `ec2-instance-eddy`
- **SSH port on host:** `2200` (check with `docker ps`)

**Copyable checklist** (on your PC, in the project folder):

```bash
# 0) Docker needs to be running
docker ps

# 1) Floci (if it stopped when the PC was shut down)
cd ~/1_GitHub/Data-Engineering_ETL_AWS-EC2_e_Airflow
floci start --persist ./floci-data
eval $(floci env)

# 2) Restart the existing EC2 instance (NOT run-instances)
aws ec2 start-instances --instance-ids i-445fcef7e259b7f85

sleep 5
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

Expected in `docker ps`:

```text
floci-ec2-i-445fcef7e259b7f85   0.0.0.0:2200->22/tcp
```

**3) Log in via SSH** (change `2200` if `docker ps` shows a different port):

```bash
ssh -i ~/.ssh/id_ed25519 -p 2200 root@127.0.0.1
```

**4) If you get `Connection refused` or `Connection reset`** — container is up, SSH isn't yet. On **your PC** (outside SSH):

```bash
docker exec floci-ec2-i-445fcef7e259b7f85 bash -c "mkdir -p /run/sshd && /usr/sbin/sshd"
```

Try the `ssh` command again. The Docker container's name is **`floci-ec2-` + InstanceId** (e.g. `floci-ec2-i-445fcef7e259b7f85`).

**5) Check that Airflow is still installed** (inside SSH):

```bash
source /root/.venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow version
ls ~/airflow
```

**6) Bring the UI back up** (dedicated terminal, inside SSH):

```bash
airflow standalone
```

**7) Browser** — another terminal on your PC, tunnel (leave it open).

This is to open Airflow in the browser, i.e., connect the Airflow container to the browser.

```bash
ssh -i ~/.ssh/id_ed25519 -p 2200 -L 8080:localhost:8080 root@127.0.0.1
```

→ **http://localhost:8080** · user **`admin`** · password in:

`cat ~/airflow/simple_auth_manager_passwords.json.generated`

**VS Code Remote SSH** (`floci-ec2-eddy` → `/root/airflow`) still works after restarting — it just needs step 3 (SSH) to work.

### 3.5 Delete the instance

Via CLI (recommended):

```bash
eval $(floci env)
aws ec2 terminate-instances --instance-ids i-COLE_O_ID
```

**UI:** **`terminated`** instances may remain visible for ~1 hour (normal behavior). The trash icon in the UI does **not** remove it immediately like the AWS console does — use `terminate-instances` if you need to guarantee it.

**`terminated`** instances do not accept SSH. Create another one with `run-instances` (the `floci-eddy` key remains under Networking).

Rows named **`image`** in the UI are entries from the **AMI catalog**, not your instances.

---

## 4. Airflow 3.3.1 + `weather_dag` DAG (on Floci EC2)

We reproduce the video with a current stack: [Airflow 3.3.1](https://pypi.org/project/apache-airflow/) + [uv](https://docs.astral.sh/uv/) + Python **3.12** on EC2 (`/root/.venv`).

Differences from the video (Airflow 2):

- `schedule_interval=` → `schedule=`
- `execution_date` → `logical_date`
- Admin password: `cat ~/airflow/simple_auth_manager_passwords.json.generated` (**read** it with `cat`, don't execute the file)

### 4.0 Local repo vs. EC2 (two places)

| Artifact | Local repo (Git) | EC2 (where it runs) |
|----------|------------------|-----------------|
| DAG | [`airflow/dags/weather_dag.py`](airflow/dags/weather_dag.py) | `/root/airflow/dags/weather_dag.py` |
| Secrets | [`.env.example`](.env.example) at root | `/root/airflow/.env` ([`airflow/.env.example`](airflow/.env.example)) |
| Airflow / venv | — | `/root/airflow`, `/root/.venv` |

The [`airflow/`](airflow/) folder in the repo **mirrors** the EC2 structure (`dags/`). After editing in Git, **copy** the `.py` file to the instance (Remote SSH). **Never** commit `.env` with the API key.

Cursor agent: skill [`.cursor/skills/floci-ec2-airflow/`](.cursor/skills/floci-ec2-airflow/SKILL.md) (post-reboot checklist, paths, UI).

### 4.1 Install Airflow (first time, inside SSH)

```bash
apt update && apt install -y curl ca-certificates
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

export AIRFLOW_HOME=~/airflow
export AIRFLOW_VERSION=3.3.1
export PYTHON_VERSION=3.12

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

uv venv --python 3.12 /root/.venv
source /root/.venv/bin/activate
uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
uv pip install requests python-dotenv boto3 pandas
```

### 4.2 `.env` on the EC2 instance

Create `/root/airflow/.env` (template: [`airflow/.env.example`](airflow/.env.example)):

```env
API_KEY_OPENWEATHER=your_key_here
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
AWS_ENDPOINT_URL=http://172.17.0.1:4566
```

From **inside** the EC2 instance, `localhost:4566` points to the container itself. Use **`172.17.0.1:4566`** (Docker gateway) to reach the Floci S3 on the host.

### 4.3 VS Code Remote SSH (as in the video)

`~/.ssh/config`:

```sshconfig
Host floci-ec2-eddy
    HostName 127.0.0.1
    Port 2200
    User root
    IdentityFile ~/.ssh/id_ed25519
```

Connect → open **`/root`** or **`/root/airflow`**. Create/edit `dags/weather_dag.py` or copy it from the repo.

### 4.4 Start Airflow + open the UI in the browser

**Terminal 1** (SSH, leave it running):

```bash
source /root/.venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow standalone
```

**Terminal 2** (on your PC, tunnel — leave it open):

```bash
ssh -i ~/.ssh/id_ed25519 -p 2200 -L 8080:localhost:8080 root@127.0.0.1
```

Browser: **http://localhost:8080** · login **`admin`** + password from the JSON file (`cat`, section 3.4).

### 4.5 `weather_dag` DAG — deploy and run

Code: [`airflow/dags/weather_dag.py`](airflow/dags/weather_dag.py)

Pipeline:

```
extract_weather  →  transform_to_csv  →  load_to_s3
   (OpenWeather)      (CSV string)      (s3://my-s3-for-cv/weather/)
```

In the Airflow UI:

1. **Dags** → **`weather_dag`**
2. **Toggle ON** (unpause — otherwise it won't run on schedule)
3. **Trigger** (▶) — immediate manual run
4. **Tasks** / **Runs** tab — three green tasks = success

**Mapped = false** in the UI = a normal task (not *dynamic task mapping*). **`@task` operator** = TaskFlow API.

Check the CSV in S3 (**local** terminal):

```bash
eval $(floci env)
aws s3 ls s3://my-s3-for-cv/weather/ --recursive
aws s3 cp s3://my-s3-for-cv/weather/vitoria_YYYYMMDD.csv /tmp/clima.csv
```

Example already generated: `s3://my-s3-for-cv/weather/vitoria_20260916.csv`.

---

## Errors we've encountered (quick reference)

| Symptom | Cause | Solution |
|---|---|---|
| `Unknown options: --min-count, --max-count` | AWS CLI v2.36+ | Use `--count 1` |
| Terminal stuck on `:` after JSON | `less` pager | Press `q`; or `export AWS_PAGER=""` |
| `import-key-pair` file not found | SSH key pair didn't exist in `~/.ssh/` | `ssh-keygen` and repeat the import |
| UI doesn't remove terminated instance | ~1h tombstone in Floci | `aws ec2 terminate-instances` |
| Only `i-…` shows up in the UI | No Name tag | `--tag-specifications` or `create-tags` |
| SSH `Connection refused` after reboot | EC2 container stopped | `floci start` + `aws ec2 start-instances` |
| SSH `Connection reset` with container up | `sshd` didn't start in the container | `docker exec … /usr/sbin/sshd` (section 3.4) |
| Floci UI says `running`, SSH won't connect | Metadata ≠ Docker container | `docker ps` should list `floci-ec2-i-…` |

---

## References

- Video: <https://www.youtube.com/watch?v=uhQ54Dgp6To>
- Floci: <https://floci.io> · EC2: <https://floci.io/floci/services/ec2/>
- Airflow 3.3.1: <https://airflow.apache.org/docs/apache-airflow/stable/start.html>
- OpenWeather Current Weather: <https://openweathermap.org/api/current.md>
- AWS CLI v2: <https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>

Thank God!
