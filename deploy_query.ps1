# deploy_query.ps1
# Deploys the query Lambda function to AWS
# Run from the project root: .\deploy_query.ps1

$ErrorActionPreference = "Stop"

$FUNCTION_NAME = "aws-docs-rag-query"
$PROFILE       = "aws-docs-rag-dev"
$REGION        = "us-east-1"
$SOURCE_DIR    = "backend\query"
$ZIP_FILE      = "query_function.zip"

Write-Host "Deploying $FUNCTION_NAME..." -ForegroundColor Cyan

# Remove old zip if it exists
if (Test-Path $ZIP_FILE) {
    Remove-Item $ZIP_FILE
}

# Zip the Lambda source
Compress-Archive -Path "$SOURCE_DIR\*" -DestinationPath $ZIP_FILE
Write-Host "Created $ZIP_FILE" -ForegroundColor Green

# Deploy to Lambda
aws lambda update-function-code `
    --function-name $FUNCTION_NAME `
    --zip-file fileb://$ZIP_FILE `
    --region $REGION `
    --profile $PROFILE | Out-Null

Write-Host "Waiting for update to complete..."
aws lambda wait function-updated `
    --function-name $FUNCTION_NAME `
    --region $REGION `
    --profile $PROFILE

Write-Host "Deploy complete." -ForegroundColor Green

# Clean up zip
Remove-Item $ZIP_FILE
Write-Host "Cleaned up $ZIP_FILE"
