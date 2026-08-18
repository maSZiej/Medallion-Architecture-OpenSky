# open sky pipeline

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
| Technology | used For| Column 3 |
|----------|----------|----------|
| UV | libraries depedency| Row 1 C3 |
| Pyspark | disitribiuted processing| Row 2 C3 |
| Minio | data lake - S3 bucket on prem | Row 2 C3 |
| Delta-spark | transaction and data time travel| Row 2 C3 |
| Kedro | bulding effective pipeline| Row 2 C3 |
| Docker| contenerization services | Row 2 C3 |
| Github | version control| Row 2 C3 |
<!-- | Row 2 C1 | Row 2 C2 | Row 2 C3 |
| Row 2 C1 | Row 2 C2 | Row 2 C3 |
| Row 2 C1 | Row 2 C2 | Row 2 C3 | -->
<!-- Technologies used: -->

<!-- Airflow for orchestration and automatization

Github-action for CI/CD
Kubernetes for deploy
Terraform for manage kubernetes -->
