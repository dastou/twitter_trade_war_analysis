@echo off
setlocal enabledelayedexpansion

echo.
echo ==================================================
echo    Tweet Simulator - Scenario Control
echo ==================================================

if "%1"=="" (
    echo Usage: %0 [scenario] [rate]
    echo.
    echo Available scenarios:
    echo   normal               ^(balanced sentiment^)
    echo   trade_war_escalation ^(70%% negative, high volume^)
    echo   breakthrough_deal    ^(70%% positive, high volume^)
    echo   supply_chain_crisis  ^(60%% negative, medium volume^)
    echo   market_uncertainty   ^(45%% negative, medium volume^)
    echo.
    echo Examples:
    echo   %0 trade_war_escalation 25
    echo   %0 normal 10
    echo   %0 breakthrough_deal 20
    echo.
    goto :end
)

set SCENARIO=%1
set RATE=%2

if "%RATE%"=="" set RATE=15

echo Changing scenario to: %SCENARIO%
echo Setting tweet rate to: %RATE% tweets/min
echo.

echo Updating .env file...

(
echo # Kafka Configuration
echo KAFKA_BOOTSTRAP_SERVERS=localhost:9092
echo KAFKA_TOPIC_TWEETS=raw-tweets
echo KAFKA_TOPIC_PROCESSED=processed-tweets
echo KAFKA_TOPIC_ANOMALIES=anomalies
echo KAFKA_TOPIC_ALERTS=alerts
echo.
echo # Elasticsearch Configuration
echo ELASTICSEARCH_HOST=localhost:9200
echo ELASTICSEARCH_INDEX_TWEETS=tweets
echo ELASTICSEARCH_INDEX_ANOMALIES=anomalies
echo.
echo # Grafana Configuration
echo GRAFANA_URL=http://localhost:3000
echo GRAFANA_USER=admin
echo GRAFANA_PASSWORD=admin
echo GRAFANA_API_KEY=your_api_key_here
echo.
echo # Tweet Simulator Configuration
echo TWEET_RATE=%RATE%
echo SIMULATION_MODE=%SCENARIO%
echo LANGUAGE_MIX=en:0.7,fr:0.3
echo.
echo # Spark Configuration
echo SPARK_MASTER_URL=spark://localhost:7077
echo SPARK_DRIVER_MEMORY=1g
echo SPARK_EXECUTOR_MEMORY=1g
echo.
echo # Alert System Configuration
echo ALERT_THRESHOLD_SENTIMENT=0.8
echo ALERT_THRESHOLD_VOLUME=100
echo ALERT_EMAIL_ENABLED=false
) > .env.tmp

move .env.tmp .env

echo .env file updated successfully.
echo.

echo Restarting tweet-simulator...
docker-compose stop tweet-simulator
timeout /t 2 /nobreak >nul

docker-compose up -d tweet-simulator
timeout /t 3 /nobreak >nul

echo.
echo Verifying configuration...
docker-compose logs tweet-simulator | findstr "Configuration loaded" | tail -1

echo.
echo ==================================================
echo Scenario change completed!
echo ==================================================

:end
pause