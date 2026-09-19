# Using Floci (Local S3) and Uploading CV

Senior Data Scientist: Dr. Eddy Giusepe Chirinos Isidro

This document records, step-by-step, what we did to use [Floci](https://floci.io) — a local AWS emulator — and upload the PDF CV to the **local** S3. Nothing goes to Amazon's cloud and there is no charge.

CV File (at the root of this repository): `Data_Science_Eddy_en.pdf`

Selected Bucket:

- Bucket name: `my-s3-for-cv`
- Object (key): `eddy-cv-directory/Data_Science_Eddy_en.pdf`
- Full URI: `s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf`

---

## 1. What is Floci

Floci installs an AWS services emulator (S3, EC2, SQS, etc.) **on your machine**.

- You communicate with it using the **AWS CLI** or SDK (`boto3`), with the **same commands** as real AWS.
- The API endpoint is `http://localhost:4566` (not an S3 "folders" website).
- Local credentials: `test` / `test`. No AWS account needed.

Important difference:

| | Floci | Real AWS |
|---|---|---|
| `aws s3 mb`, `cp`, `ls` commands | identical | identical |
| Destination | `localhost:4566` | Amazon cloud |
| Credential | `eval $(floci env)` → `test` | Your IAM access key |
| Cost | none | S3 charges |

---

## 2. Prerequisites

### 2.1 Docker

Floci runs as a container. Verify:

```bash
docker ps -a
```

If the command works, Docker is OK. You don't need the `neo4j` container (or any other) running.

### 2.2 AWS CLI v2

On recent Ubuntu, **this fails** (package removed from apt):

```bash
sudo apt install awscli
# E: Package 'awscli' has no installation candidate
```

**Do not** use `curl ... awscli.amazonaws.com/v2/install.sh | bash` — AWS does not document this script.

**Official installation** (Linux x86_64):

```bash
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version
```

You must see `aws-cli/2.x.x`. If `unzip` doesn't exist: `sudo apt install unzip`.

Ubuntu shortcut: `sudo snap install aws-cli --classic`.

---

## 3. Install Floci CLI (once)

```bash
curl -fsSL https://floci.io/install.sh | sh
```

Expected output (example):

```text
Floci CLI 0.2.2 installed to /usr/local/bin/floci
```

Verify: `floci --version`

---

## 4. Start the emulator

```bash
floci start
```

Wait until you see something like:

```text
Floci AWS is ready (http://localhost:4566)
```

The first time, Docker downloads the `floci/floci:latest` image; it may take some time.

### 4.1 Browser Console

You **can** open the Floci console in the browser (we saw Storage, Compute, etc.).

- **Do not** look for a tile written exactly `S3`.
- In the UI, S3 appears as **Storage**.
- `0 resources` = bucket doesn't exist yet. After `mb` + `cp`, reload and click **Storage** / **Open Storage**.

`http://localhost:4566` is the **API** endpoint. The pretty page is just the panel. For reliably "seeing" the PDF, use `aws s3 ls`.

### 4.2 Persistence (so the bucket doesn't disappear)

By default, state is **in-memory**: `floci stop`, Docker restart, or machine shutdown **deletes** the bucket and objects. The PDF on the repo disk continues; only the copy in the emulator disappears.

If Floci is already running without persistence, stop, start again recording to a local folder, and point **this** terminal to the emulator:

```bash
floci stop
floci start --persist ./floci-data
eval $(floci env)
```

What each line does:

| Command | Function |
|---|---|
| `floci stop` | Stops the container. Without prior persistence, the local S3 disappears in this step. |
| `floci start --persist ./floci-data` | Starts the emulator again and saves state to the `./floci-data` folder (creates if not exists). Next time, use the **same** path for the bucket to reappear. |
| `eval $(floci env)` | Configures **this** terminal (`AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID=test`, etc.) for `aws` to talk to Floci, not real AWS. Valid only in this tab. |

This **does not** create the bucket or upload the PDF. After these three lines, on the **first** time with persistence (the old in-memory upload doesn't return):

```bash
aws s3 mb s3://my-s3-for-cv
aws s3 cp Data_Science_Eddy_en.pdf s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf
aws s3 ls s3://my-s3-for-cv --recursive
```

On subsequent startups, if the `./floci-data` folder exists, `mb` + `cp` are not necessary — the object is already in persisted state. Still need `eval $(floci env)` in each new terminal.

Do not commit `floci-data/` to Git: it's emulator local state.

#### After turning off the machine

Floci **does not** start automatically. The `./floci-data` folder remains on disk; the emulator, not.

If you already used persistence and the file was already in local S3, when starting again execute **these** two commands (in the repo folder):

```bash
floci start --persist ./floci-data
eval $(floci env)
```

- Use **`floci start --persist ./floci-data`**. Do not use only `floci start`.
- `floci start` starts in-memory and **ignores** `./floci-data`. Storage appears empty, although the folder still exists.
- `--persist ./floci-data` is what relinks the emulator to the folder where the bucket and PDF were saved. The path must be the **same**.
- `eval $(floci env)` points **this** terminal to Floci. Without it, `aws` doesn't talk to the emulator.
- Do not run `mb` or `cp` again: the object is already in `./floci-data`. Verify with `aws s3 ls s3://my-s3-for-cv --recursive`.

---

## 5. Point THIS terminal to Floci

The AWS CLI, by itself, tries real AWS. Without environment variables, the error is:

```text
make_bucket failed: s3://... Unable to locate credentials
```

In the **same terminal** where you'll run `aws s3 ...`:

```bash
eval $(floci env)
echo $AWS_ENDPOINT_URL
echo $AWS_ACCESS_KEY_ID
```

What each does:

- `floci env` **prints** `export AWS_ENDPOINT_URL=...`, `AWS_ACCESS_KEY_ID=test`, etc.
- `eval $(...)` **executes** this output in this shell. Without `eval`, variables don't enter the terminal.
- `echo $AWS_ENDPOINT_URL` only **confirms**. Must show `http://localhost:4566` or `http://localhost.floci.io:4566`.
- `echo $AWS_ACCESS_KEY_ID` must show `test`.

This is valid **only in this tab**. Close the terminal? Run `eval $(floci env)` again.

If `cp` fails with `Could not connect to the endpoint URL: http://localhost.floci.io:4566/...`, Floci crashed or `eval` points to a host that doesn't resolve. Run `floci start` and again `eval $(floci env)`.

---

## 6. Three S3 Commands (do not mix)

Each does **one** thing. Run **one at a time**, on separate lines.

| Command | Meaning | When |
|---|---|---|
| `aws s3 mb` | *make bucket* — creates the bucket | **once** |
| `aws s3 cp` | *copy* — uploads or downloads the file | each object |
| `aws s3 ls` | *list* — lists what's in the bucket | to verify |

**`mb` does not upload a file.** Who uploads the PDF is **`cp`**.

### 6.1 Create the bucket (only once)

In the project folder:

```bash
cd ~/1_GitHub/Data-Engineering_ETL_AWS-EC2_e_Airflow
aws s3 mb s3://my-s3-for-cv
```

Expected output:

```text
make_bucket: my-s3-for-cv
```

If you run `mb` again on the same name, the API complains the bucket already exists. This is normal.

### 6.2 Upload the CV

```bash
aws s3 cp Data_Science_Eddy_en.pdf s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf
```

Expected output:

```text
upload: ./Data_Science_Eddy_en.pdf to s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf
```

Destination reading:

- `s3://` — S3 scheme
- `my-s3-for-cv` — **only** the bucket name
- `eddy-cv-directory/` — prefix (logical folder; S3 doesn't create real directory)
- `Data_Science_Eddy_en.pdf` — object name

### 6.3 List

```bash
aws s3 ls s3://my-s3-for-cv --recursive
```

`--recursive` includes what's "inside" `eddy-cv-directory/`.

### 6.4 Download back (optional, to verify)

```bash
aws s3 cp s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf /tmp/cv-from-floci.pdf
file /tmp/cv-from-floci.pdf
ls -l Data_Science_Eddy_en.pdf /tmp/cv-from-floci.pdf
```

`file` should say PDF. Same sizes = upload is correct.

---

## 7. Error we made: two commands on same line

**Wrong** (mixes `cp` and `mb` in same command):

```bash
aws s3 cp Data_Science_Eddy_en.pdf s3://aws s3 mb s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf
```

Error:

```text
Unknown options: s3,mb,s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf
```

CLI read `s3://aws` as destination and the rest (`s3 mb ...`) as invalid options.

**Right** — two commands, two lines:

```bash
aws s3 mb s3://my-s3-for-cv
aws s3 cp Data_Science_Eddy_en.pdf s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf
```

For another file in the **same** bucket, **do not** run `mb` again. Just another `cp`.

---

## 8. Complete sequence (copy and paste)

With Docker OK, Floci and AWS CLI already installed, and PDF at repo root:

```bash
cd ~/1_GitHub/Data-Engineering_ETL_AWS-EC2_e_Airflow

floci start --persist ./floci-data
eval $(floci env)
echo $AWS_ENDPOINT_URL

aws s3 mb s3://my-s3-for-cv
aws s3 cp Data_Science_Eddy_en.pdf s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf
aws s3 ls s3://my-s3-for-cv --recursive
```

If Floci is **already** running without persistence, use `floci stop` first (section 4.2). Without `--persist`, the bucket disappears when stopping the emulator.

---

## 9. Stop Floci

```bash
floci stop
```

Without `--persist`, the `my-s3-for-cv` bucket disappears.

To come back **with** persistence (and point the terminal):

```bash
floci start --persist ./floci-data
eval $(floci env)
```

If `./floci-data` already has the state, the bucket reappears. If it's the first time in this mode, run `mb` + `cp` again (section 4.2).

After **turning off the machine**, the procedure is the same: `floci start --persist ./floci-data` and then `eval $(floci env)`. Only `floci start` doesn't recover Storage. Details in section 4.2.

---

## 10. References

- Floci: <https://floci.io>
- GitHub: <https://github.com/floci-io/floci>
- Setup AWS CLI / SDK on Floci: <https://floci.io/floci/getting-started/aws-setup/>
- Official AWS CLI v2 installation: <https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>
