# asos-movies-app

This is a pyspark based etl app.

### What it does?
* Download movies data zip from grouplens
* Unzip data and stage data in datalake
* Transforms data and gives result in output file

### Pre-requisites :
* Databricks Connect - For running spark. 
* Databricks CLI - For uploading data to databricks file system from local