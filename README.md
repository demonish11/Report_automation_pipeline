# Project: Automated Daily Report Generation on AWS
Implemented an automated solution on AWS to generate daily reports crucial for various business teams' key metrics, enabling informed decision-making. This automation significantly optimized resource utilization, resulting in a 40% reduction in operational costs.

## Architecture Overview:
   Data Migration (AWS DMS): 
    Orchestrated migration from RDS (Relational Database Service) to Redshift, ensuring seamless transfer of data for analytics.

  Data Transformation (AWS Glue with PySpark): 
    Leveraged AWS Glue, powered by PySpark, to perform complex transformations on large datasets. This transformation layer prepared the data for reporting and analysis.

 Workflow Automation (AWS Lambda): 
    Utilized AWS Lambda functions to trigger Glue jobs based on predefined schedules or data events, enabling timely execution of data processing tasks.

 Data Storage and Cataloging (Amazon S3 and AWS Glue Catalog):
    Stored processed data efficiently in S3, leveraging Glue Catalog for metadata management and schema inference.

## Key Achievements:
 Resource Optimization:
    Reduced resource time and costs by automating the entire reporting process, eliminating manual intervention.

## Scalability and Performance: 
Leveraged AWS services for scalable and efficient data processing, ensuring high performance even with large datasets.

## Insightful Analytics: 
Enabled business teams to derive insightful Key Performance Indicators (KPIs) using Amazon QuickSight dashboards, leveraging Glue catalog tables for seamless data integration.

This project exemplifies effective utilization of AWS services for data engineering and automation, resulting in tangible benefits for business operations and decision-making processes.

![Report automation](https://github.com/demonish11/Report_automation_pipeline/assets/141517834/5148c4a6-4f3c-428f-ab16-adba441ea8f3)
