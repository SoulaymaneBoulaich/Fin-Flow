# start.ps1 — Native Windows PowerShell Startup Automation Script for FinFlow

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "           FinFlow Data Engineering Platform                " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Load environment variables from .env
if (Test-Path ".env") {
    Write-Host "[Info] Loading environment configurations from .env..." -ForegroundColor Yellow
    Get-Content .env | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $parts = $line.Split('=', 2)
            if ($parts.Length -eq 2) {
                $key = $parts[0].Trim()
                $value = $parts[1].Trim().Trim('"').Trim("'")
                [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
            }
        }
    }
}

# 2. Create required directories
Write-Host "[Info] Creating directory layout..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "logs/airflow" | Out-Null
New-Item -ItemType Directory -Force -Path "src/orchestration/dags" | Out-Null

# 3. Start database and storage layers
Write-Host "[1/4] Booting database and storage layer containers (Postgres, MinIO, Cassandra)..." -ForegroundColor Yellow
docker compose up -d postgres zookeeper minio cassandra
Start-Sleep -Seconds 10

# 4. Start message broker cluster
Write-Host "[2/4] Booting Kafka ingestion cluster nodes..." -ForegroundColor Yellow
docker compose up -d kafka-1 kafka-2 kafka-3
Start-Sleep -Seconds 15

# 5. Initialize schemas and S3 buckets
Write-Host "[3/4] Running schema and bucket bootstrap hooks..." -ForegroundColor Yellow
docker compose up kafka-init minio-init

# 6. Boot the rest of the orchestration and serving services
Write-Host "[4/4] Starting Airflow, Spark, Druid, and FastAPI containers..." -ForegroundColor Yellow
docker compose up -d

Write-Host ""
Write-Host "✅ FinFlow platform started! Services available at:" -ForegroundColor Green
Write-Host "   • Dashboard Console: http://localhost:8000" -ForegroundColor Green
Write-Host "   • Kafka UI:          http://localhost:8090" -ForegroundColor Green
Write-Host "   • MinIO:             http://localhost:9001  (admin / FinFlow_Secret_2024!)" -ForegroundColor Green
Write-Host "   • Airflow:           http://localhost:8080  (admin / admin123)" -ForegroundColor Green
Write-Host "   • Spark UI:          http://localhost:8081" -ForegroundColor Green
Write-Host "   • Superset:          http://localhost:8088  (admin / admin123)" -ForegroundColor Green
Write-Host ""
Write-Host "To execute test validation suite, run: python -m pytest tests/" -ForegroundColor Yellow
Write-Host ""
