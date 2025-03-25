# Domain Properties Housing Analysis 2016-2021
My final project submission for 2025 zoomcamp, the tools used in this project are taught by DataTalksClub. A big thanks to all the coaches, I have learned a lot from scratch in here.
## Problem Description
The goal of this project is to look into historial data of Sydney Housing Price. I know this is a pupolar topic for Sydney residents, the reason I chose this datasets to analyse is for investment decisions to track the house price increasement rate amount different regions in Sydney subburbs. The dashboard can be used to seek potential investment market.

### Kaggle Dataset
https://www.kaggle.com/datasets/alexlau203/sydney-house-prices/data?select=domain_properties.csv

## Technologies


## Step 1：Use Terraform to Create Infrastructure
### Setting up a service account on GCP
* Go to Google cloud - IAM&Admin - Create Service Account - Service Account Name: project2025 - Grant Roles for Service Account (Storage Admin, Bigquery Admin)
### Setting up Credentials and Create bucket in GCP
* Generate Json key - Save as mycreds.json under keys folder - Create main.tf - terrraform fmt - terraform init - terraform plan - terraform apply - create gitignore file ( Ignore Json key )
## Step 2: Use Kestra to Load Data to Google Cloud Platform (GCP) 
#### Using: 
- Google Cloud Storage (GCS) as a data lake 
- BigQuery as a data warehouse

Pull Kestra image from Docker Hub - Start Kestra in Docker container - Forward 8090 - Access Kestra at localhost: 8090 - Download Service Account Json key and Kaggle Json key - Save in Kestra KV store - Creat yaml files to dowmload and load dataset to GCP - Use Kestra to Automate Workflow 


# H1 - Biggest
## H2 - Medium
### H3 - Small
Regular text (smallest)

Bold → **bold text**

Italic → *italic text*

Inline code → `code`