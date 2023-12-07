# DEPLOYMENT NOTES

TBD

# Build distribution
zip -r discograph.zip . -x '.git/*' '.idea/*' '.pytest_cache/*' '.venv/*'  'tests/*' 'discograph/data/*' 'discograph/node_modules/*' '*/__pycache__/*' 'discograph/docs/*' 'discograph/source/*'

# Deploy to Azure WebApp
az webapp deploy --name $APP_SERVICE_NAME --resource-group $RESOURCE_GROUP_NAME --src-path discograph.zip
