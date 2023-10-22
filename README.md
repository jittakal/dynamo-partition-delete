# Utility to Delete DynamoDB Table Partition Data

Deleting DynamoDB Table Partition Data with Go Concurrency.

## Overview

The DynamoDB Delete Table Partition Data Utility is a simple utility built with Golang, Goroutines, Channels, and the AWS SDK v2. Leveraging the concurrency capabilities of Goroutines and Channels, it allows for efficient parallelized deletion of data in a DynamoDB table, making it ideal for handling large datasets. 

The AWS SDK v2 integration ensures secure and efficient communication with AWS services. This utility is designed for streamlined partition data removal when working with Amazon DynamoDB in Golang-based applications.

## Solution

![ddbctl](./docs/images/ddbctl-delete-partition.png "DynamoDB Table Partition Data Deleter")

## Usage 

### Local

```bash
$ ddbctl delete-partition --table-name <<table-name>> --partition-value <<partition-value>> --endpoint-url <<optional-endpoint-url> --region <<optional-aws-region>>

$ # skip confirmation
$ ddbctl delete-partition --table-name <<table-name>> --partition-value <<partition-value>> --endpoint-url <<optional-endpoint-url> --region <<optional-aws-region>> --skip-confirmation
```

### Docker

```bash
$ # build local image
$ # make docker_build
$ docker run go-dynamodb-partition-delete:latest /ddbctl delete-partition -t Orders -p A -e http://192.168.0.139:8080 -r us-east-1 -s
```

or

```bash
$ # pulling docker image from docker.io/jittakal/go-dynamodb-partition-delete:latest
$ docker run jittakal/go-dynamodb-partition-delete:latest /ddbctl delete-partition -t Orders -p A -e http://192.168.0.139:8080 -r us-east-1 -s
```

### Helm - Kubernetes Job

```bash
$ # Modify helm/ddebctl-job/values.yaml and deploy
$ cd helm
$ helm install orders ./ddbctl-job -f ./ddbctl-job/values.yaml

$ # List deployment details
$ helm list

$ # Delete Job
$ helm delete orders
```

## Limitations

- Only Partiton Key of type string is supported as of now
- DynamoDB local connection support as of now
