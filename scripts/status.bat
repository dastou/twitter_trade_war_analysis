@echo off
echo.
echo ==================================================
echo    Tweet Simulator - System Status  
echo ==================================================
echo.

echo [1] Docker Services Status:
echo ---------------------------
docker-compose ps

echo.
echo [2] Current Configuration:
echo -------------------------
type .env | findstr "TWEET_RATE\|SIMULATION_MODE"

echo.
echo [3] Tweet Simulator Status:
echo --------------------------
docker-compose logs tweet-simulator | findstr "Configuration loaded\|Scenario\|Stats" | tail -5

echo.
echo [4] Recent Tweets Generated:
echo ---------------------------
curl -s "localhost:9200/tweets-*/_search?size=3&sort=timestamp:desc" >nul 2>&1
if %errorlevel% equ 0 (
    echo Recent tweets found in database
    echo Use: curl "localhost:9200/tweets-*/_search?size=3&sort=timestamp:desc&pretty"
) else (
    echo No tweets found yet or Elasticsearch not accessible
)

echo.
echo [5] Anomalies Detected:
echo ----------------------
curl -s "localhost:9200/anomalies-*/_search?size=1" >nul 2>&1
if %errorlevel% equ 0 (
    echo Check: curl "localhost:9200/anomalies-*/_search?size=5&pretty"
) else (
    echo Cannot check anomalies - Elasticsearch not accessible
)

echo.
echo [6] System Health:
echo -----------------
echo Kafka: 
docker-compose ps kafka | findstr "healthy" >nul && echo "   Healthy" || echo "   Check required"

echo Elasticsearch: 
curl -s "http://localhost:9200/_cluster/health" | findstr "green\|yellow" >nul && echo "   Healthy" || echo "   Not healthy"

echo Grafana: 
curl -s "http://localhost:3000/api/health" >nul 2>&1 && echo "   Healthy" || echo "   Not responding"

echo.
echo [7] Quick Actions:
echo -----------------
echo Change scenario: control-scenario.bat [scenario] [rate]
echo View live logs:  docker-compose logs -f tweet-simulator
echo Check all:       docker-compose ps
echo.
echo Available scenarios:
echo   normal, trade_war_escalation, breakthrough_deal
echo   supply_chain_crisis, market_uncertainty
echo.

pause