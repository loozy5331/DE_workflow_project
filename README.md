# ETL 구현 실습을 위한 repository (feat. 망가져도 괜찮은 장난감)

<목표>
- 주기적으로 주가 데이터를 수집하여 필요한 지표들을 계산한 요약 테이블 생성

<사전 조건>
1. docker-compose를 통해 airflow 환경 구축(https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)
2. requirements.txt 설치
3. 실행시킬 서비스들 실행 (+ DB)
<pre><code> $ git clone https://github.com/loozy5331/DE_services_project.git </code></pre>
5. http://localhost:8080 (airflow webserver) 접속 후, 
<pre><code>Admin > Connections 에 "postgres_DB" 추가
Admin > Variables 에 "service_stock_url" 추가 </code></pre>

<기술 스택>
1. 언어: Python3.8
2. 환경: Docker(docker-compose, ubuntu18.04LTS)
3. 외부 패키지:
  - apache-airflow
  - flask
  - postgreSQL
  - yfinance 

<개발 예정>
1. 이상 동작 알람 기능(slack bot)
2. 클라우드(AWS-redshit, GCP-bigquery)
3. 실시간 데이터(flink, sparkStream) 
