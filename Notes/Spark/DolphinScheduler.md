# DolphinScheduler 

## 1. Config

### 1.1 DB init

```sql
CREATE DATABASE dolphinscheduler;
CREATE USER bscd WITH PASSWORD 'Boston2022';
GRANT ALL PRIVILEGES ON DATABASE dolphinscheduler TO bscd;
GRANT ALL PRIVILEGES ON all tables in schema public TO bscd;

```

 [dolphinscheduler_postgre.sql](..\..\..\..\bsc\dolphinscheluler\dolphinscheduler-2.0.0-release\sql\dolphinscheduler_postgre.sql) 

### 1.2 Install Config

