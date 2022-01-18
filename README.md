<h1 align="center">Summary Project with ETL Pipeline</h1>

<p align="center">
    <a href="#Goal">Goal</a> •
    <a href="#subject">Subject</a> •
    <a href="#Prerequisites">Prerequisites</a> •
    <a href="#Dag">Dag</a> •
    <a href="#Setting">Setting</a> •
    <a href="#Trouble_shooting">Trouble_shooting</a>
</p>

## Goal

1. CLOUD SaaS(Software-as-a-Service) 사용

   - AWS - S3 를 사용하며 Cloud 서비스 활용하기

2. SPARK 환경 구성

   - Spark 환경 구성

3. 코딩 스타일 참고
   - 프로젝트 전반적으로 배울 부분 기록

## Subject

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/115540283-9b609100-a2a6-11eb-9f48-08f3a17528d8.png></p>

- 주제 : 나라별 / 일별 코로나 확진자 추이

- 특징 : daily data

- 처리 : 일자별 데이터의 차이가 0보다 작다면 에러로 판단하여 '0' 입력

## Prerequisites

- Docker
- AWS S3
- Spark
- MongoDB
- Airflow

## Dag

<br>

### _Task_

1. Task `getLastProcessedDate`

   DB 연결 및 마지막 수집 데이터 일자 반환(없으면 '2020-01-01')

2. Task `getDate`

   1번 task가 일자를 반환하면 task(parseJsonFile) 아니면 task(endRun) 반환

3. Task `parseJsonFile`

   S3에서 데이터 다운로드 & 파일을 읽고 필요한 정보만 Parsing & Parsing data 저장 & task_id(processParsedData) 반환 중간에 실패했을 경우 task_id(endRun)

4. Task `saveToDB`

   Spark 처리한 결과 데이터(results.csv)를 읽고 mongoDB에 넣을 수 있도록 변환 후 저장(db=countyDiff) & results.csv 파일 삭제

5. Process `processParsedData`

   Spark process

<br>

### _Process_

1. dag를 통해 SparkFiles 에 저장된 parsedData.csv를 읽는다.

2. window - lead()를 통해 다음 일자 값({country}Diff) 필드 추가

3. 일자 간 차이를 저장

<br>

## Setting

<br>

### AWS

- **`create user`**

- **`setting IAM`**

- **`create S3-bucket`**

<br>

### Airflow

```yml
version: "3"
x-airflow-common: &airflow-common
  image: apache/airflow:2.0.1
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ""
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW_CONN_AWS_DEFAULT: aws://?profile=airflow-spark1&s3_config_file=/opt/airflow/creds/s3&s3_config_format=aws # aws 설정
  volumes:
    - ./airflow-data/creds:/opt/airflow/creds
    - ./dags:/opt/airflow/dags
    - ./sparkFiles:/opt/airflow/sparkFiles
    - ./airflow-data/logs:/opt/airflow/logs
    - ./airflow-data/plugins:/opt/airflow/plugins
    - ./airflow-data/airflow.cfg:/opt/airlfow/airflow.cfg
  user: "${AIRFLOW_UID:-50000}:${AIRFLOW_GID:-50000}"
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  airflow-webserver:
    build:
      context: .
      dockerfile: Dockerfile
    <<: *airflow-common
    command: webserver
    ports:
      - 8080:8080
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always

  airflow-init:
    <<: *airflow-common
    command: version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: "true"
      _AIRFLOW_WWW_USER_CREATE: "true"
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}

volumes:
  postgres-db-volume:

networks:
  default:
    name: etl_02
```

<br>

### DB(MongoDB)

compose with docker

```yml
version: "3"
services:
  mongodb:
    image: mongo
    restart: always
    container_name: mongo_container
    ports:
      - "27017:27017"
    volumes:
      - etl_02_mongodb:/data/db
    env_file:
      - .env

volumes:
  etl_02_mongodb:

networks:
  default:
    name: etl_02
```

<br>

## Trouble_shooting

`decoder.JSONDecodeError`

#### Error in task - `parseJsonFile`

<p align="center" ><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/etl_02/trouble_01_error.png" width="70%" height="60%"></p>

      원본 파일에 escape 문자가 많아서 json.load() 중 에러 발생

#### `Cause`

<p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/etl_02/trouble_01_cause.png" width="70%" height="60%"></p>

      json으로 읽기 전에 정규표현식으로 필요한 문자가 아닌 것들을 제외(replace) 선행 작업 수행

#### `In code`

```Python
# 기존
with open(filename, encoding='latin-1') as data:
	jsonData = json.load(data)

# 개선
with open(filename, encoding='latin-1') as data:
	rep_data = re.sub('[^":{}\-\[\].,"A-Za-z0-9]', '', data.read())
	jsonData = json.loads(rep_data) # rep_data는 str형이기 때문에 loads()사용
```
