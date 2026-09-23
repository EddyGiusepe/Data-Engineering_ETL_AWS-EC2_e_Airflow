<h1 align="center">Data Engineering: ETL AWS EC2 and Airflow</h1>

<font color="pink">Senior Data Scientist.: Dr. Eddy Giusepe Chirinos Isidro</font>


![](architecture_and_workflow_stack/etl_pipeline_architecture.png)

Pipeline ETL (OpenWeather → transformação → S3) orquestrado com Apache Airflow. O tutorial de referência é o vídeo [How to build and automate a python ETL pipeline with airflow on AWS EC2](https://www.youtube.com/watch?v=uhQ54Dgp6To).

Neste repositório praticamos **sem gastar na Amazon**, usando o [Floci](https://floci.io): emulador local de AWS (S3, EC2, …). Os comandos `aws s3 …` e `aws ec2 …` são os **mesmos** da nuvem; muda só o destino (`localhost:4566`) e as credenciais (`test` / `test`).

Detalhes longos do S3 (erros que já cometemos, persistência): [using_floci.md](using_floci.md).

---

## O que o vídeo faz vs o que fazemos aqui

| No vídeo (AWS real) | Aqui (Floci, local, grátis) |
|---|---|
| Console AWS, Ubuntu, `t2.small`, IP público | CLI + container Docker que **finge** EC2 |
| SSH `ubuntu@ec2-….amazonaws.com` | SSH `root@127.0.0.1` na porta **2200** (ou `2200–2299`) |
| S3 na Amazon (custa) | S3 no Floci (**Storage** na UI) |
| Airflow 2.x na EC2 | **Airflow 3.3.1** + **uv** na EC2 Floci |
| `.pem` baixado da AWS | Par SSH local `~/.ssh/id_ed25519` |

Reproduzimos o **fluxo** do vídeo (S3 + EC2 + SSH + ETL depois), não a tela do console AWS nem as versões antigas dos pacotes.

```
OpenWeather API  →  Airflow 3.3.1 (na EC2)  →  CSV  →  S3
     (clima)              (orquestra)                 (guarda)
```

---

## Status do que já fizemos

| Etapa | Status |
|---|---|
| Floci com persistência (`./floci-data`) | Feito |
| Bucket S3 `my-s3-for-cv` + upload do CV | Feito |
| Chave SSH `floci-eddy` importada no Floci | Feito |
| Instância `ec2-instance-eddy` (`running`) | Feito |
| SSH `root@127.0.0.1:2200` | Feito |
| `.env` local com API OpenWeather (raiz do repo) | Feito (não commitado) |
| Airflow 3.3.1 + DAG `weather_dag` → S3 | Feito |
| DAG versionada em [`airflow/dags/`](airflow/dags/weather_dag.py) | Feito |

---

## S3 vs EC2 (para não misturar)

- **S3** = gaveta. Guarda objeto (PDF, CSV, imagem). Não executa programa.
- **EC2** = computador. Liga, você entra por SSH, instala Python/Airflow. É o “onde o job roda”.

Exemplo já feito: o CV `Data_Science_Eddy_en.pdf` está no S3 local:

- bucket: `my-s3-for-cv`
- key: `eddy-cv-directory/Data_Science_Eddy_en.pdf`
- URI: `s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf`

Na UI do Floci: S3 = **Storage**. EC2 = **Compute**. Par de chaves = **Networking → Key Pairs**. `http://localhost:4566` é API, não pasta de arquivos.

---

## Variáveis de ambiente e segredos (`.env`)

**Nunca commite** chaves reais. O `.gitignore` já ignora `.env` e `floci-data/`.

### Na raiz deste repositório (sua máquina)

Template versionado: [`.env.example`](.env.example)

```bash
cp .env.example .env
# Edite .env localmente e preencha o valor (não cole no chat nem no Git)
```

Conteúdo esperado (sem valor real no repo):

```env
API_KEY_OPENWEATHER=sua_chave_aqui
```

Usado pelo script de teste OpenWeather (`testing_openweather_api.py`, quando existir no repo) com `python-dotenv`. A chave vem de [home.openweathermap.org](https://home.openweathermap.org).

### Credenciais do Floci (automáticas)

O `eval $(floci env)` exporta credenciais **fake** só para o emulador local:

- `AWS_ACCESS_KEY_ID=test`
- `AWS_SECRET_ACCESS_KEY=test`
- `AWS_ENDPOINT_URL=http://localhost:4566` (ou `http://localhost.floci.io:4566`)

Isso **não** é segredo de produção; é o padrão do Floci.

### Chave SSH (sua máquina)

| Arquivo | Onde fica | Vai para o Git? | Vai para o Floci? |
|---|---|---|---|
| `~/.ssh/id_ed25519` | privada, só no PC | **Nunca** | **Não** |
| `~/.ssh/id_ed25519.pub` | pública | opcional (não necessário) | **Sim** (`import-key-pair`) |

O `ssh -i ~/.ssh/id_ed25519` **lê** a privada localmente; ela **não é enviada** para a instância.

### Dentro da EC2 (depois, para Airflow)

Quando instalarmos o Airflow na instância, variáveis como `AIRFLOW_HOME` e, se necessário, `API_KEY_OPENWEATHER` podem ficar em `~/airflow/.env` **dentro do container** — também fora do Git. Documentaremos na seção 4 sem expor valores.

---

## 0. Pré-requisitos (uma vez)

- **Docker** rodando: `docker ps`
- **Floci CLI**: `curl -fsSL https://floci.io/install.sh | sh` → `floci --version`
- **AWS CLI v2** (oficial; `apt install awscli` no Ubuntu recente **falha**):

```bash
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version
```

Console do Floci (opcional): `http://localhost:4500`. API AWS: `http://localhost:4566`.

Opcional — evitar pager (`less`) nos comandos `aws`:

```bash
export AWS_PAGER=""
```

---

## 1. Ligar o Floci (todo terminal / todo reboot)

Na pasta do projeto:

```bash
cd ~/1_GitHub/Data-Engineering_ETL_AWS-EC2_e_Airflow
floci start --persist ./floci-data
eval $(floci env)
echo $AWS_ENDPOINT_URL
```

- `--persist ./floci-data` guarda S3 (e estado EC2) no disco.
- `eval $(floci env)` aponta **este** terminal para o emulador.
- Cada aba nova: de novo `eval $(floci env)`.

Parar: `floci stop`. Depois de reiniciar o PC: `floci start --persist ./floci-data` + `eval $(floci env)`.

---

## 2. S3 — criar bucket e enviar o CV

```bash
cd ~/1_GitHub/Data-Engineering_ETL_AWS-EC2_e_Airflow
eval $(floci env)

aws s3 mb s3://my-s3-for-cv
aws s3 cp Data_Science_Eddy_en.pdf s3://my-s3-for-cv/eddy-cv-directory/Data_Science_Eddy_en.pdf
aws s3 ls s3://my-s3-for-cv --recursive
```

**Dois comandos separados** (`mb` uma vez; `cp` para cada arquivo). Não junte `mb` e `cp` na mesma linha.

---

## 3. EC2 — subir Ubuntu, nomear e entrar por SSH

Objetivo: ter uma máquina Linux acessível por SSH, como no vídeo **antes** do Airflow.

O Floci não cria VM na Amazon: `run-instances` sobe um **container Docker**. `ami-ubuntu2204` = Ubuntu 22.04 do catálogo Floci. `t2.micro` é só metadado.

### 3.1 Chave SSH (obrigatório)

`aws ec2 create-key-pair` no Floci gera chave **dummy** — **não serve** para SSH. Importe **sua** chave pública.

Gerar par (se ainda não existir):

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N "" -C "floci-eddy"
ls -l ~/.ssh/id_ed25519 ~/.ssh/id_ed25519.pub
```

Importar no Floci (uma vez):

```bash
eval $(floci env)
aws ec2 import-key-pair \
  --key-name floci-eddy \
  --public-key-material fileb://$HOME/.ssh/id_ed25519.pub
```

Conferir:

```bash
aws ec2 describe-key-pairs --output table
```

Na UI: **Networking → Key Pairs → `floci-eddy`**.

### 3.2 Criar a instância com nome amigável

**Importante (AWS CLI v2 recente):** use `--count 1`, **não** `--min-count` / `--max-count` (erro: `Unknown options: --min-count, --max-count`).

O nome visível na UI é a tag **`Name`**, não um parâmetro `--name`:

```bash
eval $(floci env)

aws ec2 run-instances \
  --image-id ami-ubuntu2204 \
  --instance-type t2.micro \
  --key-name floci-eddy \
  --count 1 \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=ec2-instance-eddy}]'
```

A resposta JSON pode abrir no pager (`less`) — prompt `:` no fim. Pressione **`q`** para voltar ao terminal. A instância **já foi criada**.

Conferir estado:

```bash
aws ec2 describe-instances \
  --query "Reservations[].Instances[].{Name:Tags[?Key=='Name']|[0].Value,Id:InstanceId,State:State.Name}" \
  --output table
```

Na UI: **Compute** → Name **`ec2-instance-eddy`**, State **`running`**.

Renomear instância já existente (alternativa):

```bash
aws ec2 create-tags \
  --resources i-COLE_O_ID \
  --tags Key=Name,Value=ec2-instance-eddy
```

### 3.3 SSH

| Item | Valor no Floci | No vídeo (AWS real) |
|---|---|---|
| Usuário | **`root`** | `ubuntu` |
| Host | **`127.0.0.1`** | IP público `ec2-….amazonaws.com` |
| Porta | **`2200–2299`** no host | `22` |
| Chave | `-i ~/.ssh/id_ed25519` (privada, **sem** `.pub`) | arquivo `.pem` |

Achar a porta (no **seu PC**, fora do SSH):

```bash
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

Exemplo real deste projeto:

```text
floci-ec2-i-445fcef7e259b7f85   0.0.0.0:2200->22/tcp
```

Entrar:

```bash
ssh -i ~/.ssh/id_ed25519 -p 2200 root@127.0.0.1
```

Se der erro de permissão na chave: `chmod 600 ~/.ssh/id_ed25519`.

Dentro da instância (sanity check):

```bash
hostname          # ex.: 2e8bfc63ff70 (ID do container)
uname -a
cat /etc/os-release | head -3   # Ubuntu 22.04
ls                              # home vazio é normal
exit                            # volta ao seu PC
```

### 3.4 Religar a instância (depois de reiniciar o PC)

Use este fluxo **sempre** que ligar o computador de novo. **Não** rode `run-instances` outra vez — isso cria uma máquina **nova** e você perde o que estava dentro (Airflow, DAGs, `.venv`).

| Situação | Comando |
|----------|---------|
| Instância **já existe** (ex.: `ec2-instance-eddy`) | **`start-instances`** |
| Primeira vez / apagou com `terminate-instances` | **`run-instances`** (seção 3.2) |

**Por que SSH falha após reboot?** A UI do Floci pode mostrar a instância **`running`**, mas o **container Docker** (`floci-ec2-i-…`) só volta quando você liga o Floci e manda **`start-instances`**. Às vezes o container sobe, mas o **SSH interno** ainda não — aí entra o passo 4 abaixo.

Instância deste projeto (anote a sua):

- **InstanceId:** `i-445fcef7e259b7f85`
- **Name:** `ec2-instance-eddy`
- **Porta SSH no host:** `2200` (confira com `docker ps`)

**Checklist copiável** (no seu PC, na pasta do projeto):

```bash
# 0) Docker precisa estar rodando
docker ps

# 1) Floci (se parou ao desligar o PC)
cd ~/1_GitHub/Data-Engineering_ETL_AWS-EC2_e_Airflow
floci start --persist ./floci-data
eval $(floci env)

# 2) Religar a EC2 existente (NÃO é run-instances)
aws ec2 start-instances --instance-ids i-445fcef7e259b7f85

sleep 5
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

Esperado no `docker ps`:

```text
floci-ec2-i-445fcef7e259b7f85   0.0.0.0:2200->22/tcp
```

**3) Entrar por SSH** (troque `2200` se o `docker ps` mostrar outra porta):

```bash
ssh -i ~/.ssh/id_ed25519 -p 2200 root@127.0.0.1
```

**4) Se der `Connection refused` ou `Connection reset`** — container up, SSH ainda não. No **seu PC** (fora do SSH):

```bash
docker exec floci-ec2-i-445fcef7e259b7f85 bash -c "mkdir -p /run/sshd && /usr/sbin/sshd"
```

Tente o `ssh` de novo. O nome do container no Docker é **`floci-ec2-` + InstanceId** (ex.: `floci-ec2-i-445fcef7e259b7f85`).

**5) Conferir que o Airflow ainda está instalado** (dentro do SSH):

```bash
source /root/.venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow version
ls ~/airflow
```

**6) Subir a UI de novo** (terminal dedicado, dentro do SSH):

```bash
airflow standalone
```

**7) Navegador** — outro terminal no PC, túnel (deixe aberto).

Isto é para abrir o Airflow no browser, ou seja, conectar o container Airflow com o navegador.

```bash
ssh -i ~/.ssh/id_ed25519 -p 2200 -L 8080:localhost:8080 root@127.0.0.1
```

→ **http://localhost:8080** · usuário **`admin`** · senha em:

`cat ~/airflow/simple_auth_manager_passwords.json.generated`

**VS Code Remote SSH** (`floci-ec2-eddy` → `/root/airflow`) continua valendo após religar — só precisa que o passo 3 (SSH) funcione.

### 3.5 Apagar instância

Pela CLI (recomendado):

```bash
eval $(floci env)
aws ec2 terminate-instances --instance-ids i-COLE_O_ID
```

**UI:** instâncias **`terminated`** podem continuar visíveis por ~1 hora (comportamento normal). A lixeira na UI **não** remove imediatamente como no console AWS — use `terminate-instances` se precisar garantir.

Instâncias **`terminated`** não aceitam SSH. Crie outra com `run-instances` (a chave `floci-eddy` permanece em Networking).

Linhas com nome **`image`** na UI são entradas do **catálogo de AMIs**, não instâncias suas.

---

## 4. Airflow 3.3.1 + DAG `weather_dag` (na EC2 Floci)

Reproduzimos o vídeo com stack atual: [Airflow 3.3.1](https://pypi.org/project/apache-airflow/) + [uv](https://docs.astral.sh/uv/) + Python **3.12** na EC2 (`/root/.venv`).

Diferenças em relação ao vídeo (Airflow 2):

- `schedule_interval=` → `schedule=`
- `execution_date` → `logical_date`
- Senha admin: `cat ~/airflow/simple_auth_manager_passwords.json.generated` (**ler** com `cat`, não executar o arquivo)

### 4.0 Repo local vs EC2 (dois lugares)

| Artefato | Repo local (Git) | EC2 (onde roda) |
|----------|------------------|-----------------|
| DAG | [`airflow/dags/weather_dag.py`](airflow/dags/weather_dag.py) | `/root/airflow/dags/weather_dag.py` |
| Segredos | [`.env.example`](.env.example) na raiz | `/root/airflow/.env` ([`airflow/.env.example`](airflow/.env.example)) |
| Airflow / venv | — | `/root/airflow`, `/root/.venv` |

A pasta [`airflow/`](airflow/) no repo **espelha** a estrutura da EC2 (`dags/`). Depois de editar no Git, **copie** o `.py` para a instância (Remote SSH). **Nunca** commite `.env` com API key.

Agente Cursor: skill [`.cursor/skills/floci-ec2-airflow/`](.cursor/skills/floci-ec2-airflow/SKILL.md) (checklist pós-reboot, paths, UI).

### 4.1 Instalar Airflow (primeira vez, dentro do SSH)

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

### 4.2 `.env` na EC2

Crie `/root/airflow/.env` (template: [`airflow/.env.example`](airflow/.env.example)):

```env
API_KEY_OPENWEATHER=sua_chave_aqui
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
AWS_ENDPOINT_URL=http://172.17.0.1:4566
```

De **dentro** da EC2, `localhost:4566` aponta para o próprio container. Use **`172.17.0.1:4566`** (gateway Docker) para o S3 Floci no host.

### 4.3 VS Code Remote SSH (como no vídeo)

`~/.ssh/config`:

```sshconfig
Host floci-ec2-eddy
    HostName 127.0.0.1
    Port 2200
    User root
    IdentityFile ~/.ssh/id_ed25519
```

Conectar → abrir **`/root`** ou **`/root/airflow`**. Criar/editar `dags/weather_dag.py` ou copiar do repo.

### 4.4 Subir Airflow + UI no navegador

**Terminal 1** (SSH, deixa rodando):

```bash
source /root/.venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow standalone
```

**Terminal 2** (no PC, túnel — deixa aberto):

```bash
ssh -i ~/.ssh/id_ed25519 -p 2200 -L 8080:localhost:8080 root@127.0.0.1
```

Browser: **http://localhost:8080** · login **`admin`** + senha do JSON (`cat`, seção 3.4).

### 4.5 DAG `weather_dag` — deploy e execução

Código: [`airflow/dags/weather_dag.py`](airflow/dags/weather_dag.py)

Pipeline:

```
extract_weather  →  transform_to_csv  →  load_to_s3
   (OpenWeather)      (CSV string)      (s3://my-s3-for-cv/weather/)
```

Na UI do Airflow:

1. **Dags** → **`weather_dag`**
2. **Toggle ON** (despausar — senão não roda no schedule)
3. **Acionar** (▶) — execução manual imediata
4. Aba **Tarefas** / **Execuções** — três tasks verdes = sucesso

**Mapeado = false** na UI = tarefa normal (não é *dynamic task mapping*). **Operador `@task`** = TaskFlow API.

Conferir CSV no S3 (terminal **local**):

```bash
eval $(floci env)
aws s3 ls s3://my-s3-for-cv/weather/ --recursive
aws s3 cp s3://my-s3-for-cv/weather/vitoria_YYYYMMDD.csv /tmp/clima.csv
```

Exemplo já gerado: `s3://my-s3-for-cv/weather/vitoria_20260916.csv`.

---

## Erros que já encontramos (referência rápida)

| Sintoma | Causa | Solução |
|---|---|---|
| `Unknown options: --min-count, --max-count` | AWS CLI v2.36+ | Usar `--count 1` |
| Terminal parado com `:` após JSON | Pager `less` | Tecla `q`; ou `export AWS_PAGER=""` |
| `import-key-pair` file not found | Par SSH não existia em `~/.ssh/` | `ssh-keygen` e repetir import |
| UI não apaga instância terminated | Tombstone ~1h no Floci | `aws ec2 terminate-instances` |
| Só aparece `i-…` na UI | Sem tag Name | `--tag-specifications` ou `create-tags` |
| SSH `Connection refused` após reboot | Container EC2 parado | `floci start` + `aws ec2 start-instances` |
| SSH `Connection reset` com container up | `sshd` não iniciou no container | `docker exec … /usr/sbin/sshd` (seção 3.4) |
| UI Floci diz `running`, SSH não vai | Metadado ≠ container Docker | `docker ps` deve listar `floci-ec2-i-…` |

---

## Referências

- Vídeo: <https://www.youtube.com/watch?v=uhQ54Dgp6To>
- Floci: <https://floci.io> · EC2: <https://floci.io/floci/services/ec2/>
- Airflow 3.3.1: <https://airflow.apache.org/docs/apache-airflow/stable/start.html>
- OpenWeather Current Weather: <https://openweathermap.org/api/current.md>
- AWS CLI v2: <https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>

Thank God!
