# Spotify Data Engineering Project

## Overview
This project is an end-to-end data engineering pipeline that extracts data from the Spotify API, processes and stores it in Amazon S3, transforms the data using AWS Glue and loads the final data into Snowflake using Snowpipe for further analysis. The whole pipeline is automated using various tools.

## Architecture
![Untitled Diagram (1)](https://github.com/user-attachments/assets/14cd8485-2f63-43a1-aa4d-b79f79454fb5)

## Architecture Components
* ### Data Extraction:

1. Spotify API: Data is extracted from the Spotify API. For more info visit : https://developer.spotify.com/documentation/web-api
2. AWS Lambda: A Lambda function, triggered by AWS CloudWatch, runs Python code to fetch data from the Spotify API at regular intervals.
3. Amazon CloudWatch : It is used to trigger lambda function on daily basis.
* ### Data Storage:

Amazon S3: The raw data fetched by the Lambda function and transformed data is stored in an S3 bucket.
* ### Data Transformation:

AWS Glue: The data stored in S3 is processed and transformed using AWS Glue. Spark code is utilized within Glue to perform the necessary data transformations. This process is automated, as the lambda function runs it calls the glue job at the end so we don't have to do anything 
* ### Data Loading:
1. Snowpipe: The transformed data is automatically loaded into Snowflake using Snowpipe.
2. Snowflake: The final processed data is stored in Snowflake, where it can be used for further analysis and reporting. It is a Data Warehouse.


