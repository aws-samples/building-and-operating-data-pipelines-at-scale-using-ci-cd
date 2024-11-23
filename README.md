# Building and operating data pipelines at scale using CI/CD

## Caveat

The code shared in this repository is only to provide an overview of the conceptual framework proposed as part of the AWS Blog and by no means should be used in building production systems. 

## Getting started

This repository contains artifacts that are part of an AWS Blog Post on Building and operating data pipelines at scale using CI/CD.

Below is a description of the files stored in this repository. Note this repository does not contain code that can be build and deploy but instead provide reference code and configuration that showcase how one can build an entirely code driven data pipeline using Amazon EMR and Amazon MWAA.

## File Description

### spark.json
Template that Data engineers can leverage to define their Apache Spark data processing job using JSON. 

### dp-logic-example-shareprice-sample.py
Example Direct acyclic graph (DAG) showcasing a simple ETL process.

### deploy.yaml
Template that Data engineers can leverage to build the required data pipeline in simple YAML. 

## License
For open source projects, say how it is licensed.

