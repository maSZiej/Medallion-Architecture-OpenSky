# open sky pipeline
Project have on purpose to show aircraft traffic on Polish sky
Project uses medallion architecture with 3 layers bronze, silver, gold

### Bronze layer:

Raw data from open sky api
Stored in s3 bucket 

### Silver layer:

Provides sufficient data types and enrichment columns
newest timestamp stored in s3 bucket
previus timestamps stored in history version of table

### Gold layer:

Agreggate Data to bussines goals

### Data ingestion type

Batching

### Data Flow
Here is visualalization of data flow:


![alt text](Images\Data_Flow.jpg)

### Technologies
| Technology | used For|
|----------|----------|
| UV | libraries depedency| 
| Pyspark | disitribiuted processing|
| Minio | data lake - S3 bucket on prem | 
| Delta-spark | transaction and data time travel| 
| Kedro | bulding effective pipeline| 
| Docker| contenerization services | 
| Github | version control| 
<!-- | Row 2 C1 | Row 2 C2 | Row 2 C3 |
| Row 2 C1 | Row 2 C2 | Row 2 C3 |
| Row 2 C1 | Row 2 C2 | Row 2 C3 | -->
<!-- Technologies used: -->

<!-- Airflow for orchestration and automatization

Github-action for CI/CD
Kubernetes for deploy
Terraform for manage kubernetes -->
