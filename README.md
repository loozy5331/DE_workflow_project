# ETL 구현 실습을 위한 repository (feat. 망가져도 괜찮은 장난감)

<목적>
- 주기적으로 주가 데이터를 수집하여 필요한 지표들을 계산한 요약 테이블 생성

<기술 스택>
1. 언어: Python3.8
2. 환경: Docker(docker-compose, ubuntu18.04LTS)
3. 외부 패키지:
  - apache-airflow
  - flask
  - postgreSQL
  - yfinance 
