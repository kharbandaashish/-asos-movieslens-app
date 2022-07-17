# asos-movies-app

This is a pyspark based etl app.

### What it does?
* Download movies data zip from grouplens
* Unzip data and stage data in datalake
* Transforms data and gives result in output file

### Pre-requisites :
* Databricks Connect - For running spark. 
* Databricks CLI - For uploading data to databricks file system from local

### Entry-Point for the app
src/etl/Wrapper.py

### Enhancement scope
* Exception Handling
* Add ability to run in any provided spark env, currently only databricks is supported.
* Add option to used Azure Datalake Storage instead of ddatabricks file system.
