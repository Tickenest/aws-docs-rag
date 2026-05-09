# deploy_refresh.ps1
# Deploys the refresh Lambda function to AWS
# Packages beautifulsoup4 alongside the Lambda code since it is not
# included in the standard Python Lambda runtime.
# Run from the project root: .\deploy_refresh.ps1

$ErrorActionPreference = "Stop"

$FUNCTION_NAME = "aws-docs-rag-refresh"
$PROFILE       = "aws-docs-rag-dev"
$REGION        = "us-east-1"
$SOURCE_DIR    = "backend\refresh"
$PACKAGE_DIR   = "refresh_package"
$ZIP_FILE      = "refresh_function.zip"

Write-Host "Deploying $FUNCTION_NAME..." -ForegroundColor Cyan

# Clean up any previous build artifacts
if (Test-Path $PACKAGE_DIR) { Remove-Item -Recurse -Force $PACKAGE_DIR }
if (Test-Path $ZIP_FILE)    { Remove-Item $ZIP_FILE }

# Create package directory and copy Lambda source
New-Item -ItemType Directory -Path $PACKAGE_DIR | Out-Null
Copy-Item "$SOURCE_DIR\*" $PACKAGE_DIR -Recurse

# Install beautifulsoup4 into the package directory
Write-Host "Installing dependencies..." -ForegroundColor Cyan
pip install beautifulsoup4 --target $PACKAGE_DIR --quiet

# Zip the package
Write-Host "Creating deployment package..." -ForegroundColor Cyan
Compress-Archive -Path "$PACKAGE_DIR\*" -DestinationPath $ZIP_FILE
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

# Clean up
Remove-Item -Recurse -Force $PACKAGE_DIR
Remove-Item $ZIP_FILE
Write-Host "Cleaned up build artifacts"
