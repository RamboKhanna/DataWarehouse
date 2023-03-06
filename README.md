DATA WAREHOUSE

SUMMARY
This project combines data which resides in S3, in a directory of JSON logs on user activity, as well as a directory with JSON metadata on the songs in music streaming startup, Sparkify's app.
An ETL pipeline is built that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights.

FILES
1. create_tables.py:     Drop and recreate tables
2. dwh.cfg:              Configure Redshift cluster and data import
3. etl.py:               Copy data to staging tables and insert into star schema fact and dimension tables.
4. sql_queries.py:     
                         Creating and dropping staging and star schema tables
                         Copy JSON data from S3 to Redshift staging tables
                         Insert data from staging tables to star schema fact and dimension tables
5. run.ipynb:            Running the project
6. create_cluster.ipynb: Create redshift cluster

HOW TO RUN PYTHON CODES
Set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
Create IAM role, Redshift cluster, and configure TCP connectivity by running create_cluster.ipynb
Drop and recreate tables by running create_tables.py (second cell in run.py).
Run ETL pipeline etl.py (third cell in etl.py).
Uncomment the last 3 cells in create_tables.py and run them one by one to delete the redshift cluster and de-attach IAM policy.

    
