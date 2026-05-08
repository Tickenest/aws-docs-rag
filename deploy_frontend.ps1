# deploy_frontend.ps1
# Builds and deploys the React frontend to S3
# Run from the project root: .\deploy_frontend.ps1

$ErrorActionPreference = "Stop"

$BUCKET   = "aws-docs-rag-frontend-9cfae7c1"
$PROFILE  = "aws-docs-rag-dev"
$REGION   = "us-east-1"

Write-Host "Building React frontend..." -ForegroundColor Cyan

Set-Location frontend
npm run build
Set-Location ..

Write-Host "Deploying to s3://$BUCKET..." -ForegroundColor Cyan

# Sync build output to S3
aws s3 sync frontend/build s3://$BUCKET `
    --delete `
    --region $REGION `
    --profile $PROFILE

Write-Host "Deploy complete." -ForegroundColor Green
Write-Host "Frontend URL: http://$BUCKET.s3-website-us-east-1.amazonaws.com" -ForegroundColor Yellow
