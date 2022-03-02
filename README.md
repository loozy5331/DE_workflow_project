<h1> ETL 구현 실습을 위한 repo </h1>
(feat. 망가져도 괜찮은 장난감)

<h2> 목표 </h2>

 특정 업무를 주기적으로 실행시키기 위한 데이터 파이프라인 구축
 
 ![etl_pipeline drawio](https://user-images.githubusercontent.com/36221276/156315211-b5670309-ef9c-4693-8baa-4838bed0e8ae.png)

<h2> DAG 기능 </h2>
<h4> dag_stock </h4>

  주기적으로 주가 데이터를 수집하여 필요한 지표들을 계산하여 요약 테이블 생성
  1. 현재 수집된 주가 데이터를 OLTP에서 불러옴
  2. 불러온 데이터를 복사하여 DW에 저장
  3. DW에 저장된 데이터를 바탕으로 주가 지표(ex. 52주 최고가 대비 현재 가격, 200일 이동 평균) 추출
  4. 계산된 데이터를 DM에 저장

<h2> 사전 조건 </h2>

1. docker-compose를 통해 airflow 환경 구축

(<https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html>)

2. 실행시킬 서비스들 실행

```bash
git clone https://github.com/loozy5331/DE_services_project.git
```

4. <http://localhost:8080> (airflow webserver) 접속 후,

```text
Admin > Connections 에 "postgres_DB" 추가 # OLTP, DW, D 역할 수행(추후 분리 예정)
Admin > Variables 에 "service_stock_url" 추가
```

<h2> 기술 스택 </h2>

1. 언어: Python3.8

2. 환경: Docker(docker-compose, ubuntu18.04LTS)

3. 외부 패키지: apache-airflow, flask, postgreSQL, yfinance 

<h2> 개발 예정 </h2>

1. stock 이상 동작 알람 기능(slack bot)

2. 클라우드(AWS-redshit, GCP-bigquery)

3. 실시간 데이터(flink, sparkStream) 
