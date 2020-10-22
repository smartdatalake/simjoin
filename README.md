# SimJoin

## Overview

SimJoin is a Java library providing functions for threshold-based, k-nearest neighbor and top-k (i.e., k-closest pairs) similarity joins for sets and fuzzy sets (see [[Jiang2014](http://www.vldb.org/pvldb/vol7/p625-jiang.pdf), [Mann2016](http://www.vldb.org/pvldb/vol9/p636-mann.pdf), [Xiao2009](https://ieeexplore.ieee.org/document/4812465), [Deng2017](http://www.vldb.org/pvldb/vol10/p1082-deng.pdf)].

Attribute data values may come from diverse data sources:

- CSV files from local or remote paths.
- Tables in a [PostgreSQL](https://www.postgresql.org/) database.

SimJoin can be deployed either as a standalone Java application or as a RESTful web service.

## Documentation

Javadoc is available [here](https://smartdatalake.github.io/simjoin/).

## Usage

**Step 1**. Download or clone the project:
```sh
$ git clone https://github.com/smartdatalake/simjoin.git
```

**Step 2**. Open terminal inside root folder and install by running:
```sh
$ mvn install
```
**Step 3**. Edit the parameters in the `config_datasource_csv.json.example` or `config_datasource_jdbc.json.example` file for each Data Source.

**Step 4**. Edit the parameters in the `config_simjoin.json.example` file for the SimJoin.


## Standalone execution

To invoke SimJoin in standalone mode as a Java application, run the executable:
```sh
$ java -jar target/simjoin-0.0.1-SNAPSHOT.jar --join <config_simjoin> --input <config_input> [OPTIONAL --query <config_query>]
```
or

```sh
$ java -jar target/simjoin-0.0.1-SNAPSHOT.jar -j <config_simjoin> -i <config_input> [OPTIONAL -q <config_query>]
```

## Launching SimJoin as web service

SimJoin also integrates a REST API and can be deployed as a web service application at a specific port (e.g., 8090) as follows:
```sh
$ java -Dserver.port=8090 -jar target/simjoin-0.0.1-SNAPSHOT.jar --service [OPTIONAL --timeout <seconds>]
```

Option `--service` or `-s` signifies that a web application will be deployed using [Spring Boot](https://spring.io/projects/spring-boot). The user can add a new Data Source or use one of the ones already available to perform a Similarity Join provided he/she has an API key.

Optionally, the user can provide a timeout period in seconds `--timeout` or `-t`, after which each job will terminate by force.

Once an instance of the SimJoin service is deployed as above, requests can be formulated according to the API documentation (typically accessible at `http://localhost:8090/swagger-ui.html#`). 

Thus, users are able to issue requests to an instance of the SimJoin service via a client application (e.g., Python scripts), such as:

- [`ADD DATASOURCE request`](scripts/api/add_source.py) -> Adds a new Data Source, from a local CSV file, a remote CSV file or a Postgres Database. In the header of the response, the user will find his/her unique API_key, necessary for each next method. The body of the request must be a JSON array and each JSON represents a DataSource.

- [`APPEND DATASOURCE request`](scripts/api/append_source.py) -> Appends a new Data Source, from a local CSV file, a remote CSV file or a Postgres Database to a pre-existing user.

- [`REMOVE DATASOURCE request`](scripts/api/remove_source.py) -> Removes an existing Data Source.

- [`CATALOG request`](scripts/api/catalog_source.py) -> Returns a JSON listing the available datasets for a specific user.

- [`JOIN request`](scripts/api/start_join.py) -> Starts a thread to perform join on the requested datasets, while returning a unique ID to track the status of the job with the next call. If the request contains only one API_key, then Data Source(s) are searched within that API_key / user. If [two API_keys](scripts/api/start_join2.py) are given, then the query Data Source will be searched in the first API_key / user and the input Data Source in the second API_key / user.

- [`GETSTATUS request`](scripts/api/get_status.py) -> Asking for the status of a previously initiated thread. The resulting JSON will contain a keyword for the status of the thread as well as resulting pairs from the join.

See full example [here](scripts/api/full_example.py)

## Creating and launching a Docker image 

We provide an indicative `Dockerfile` that may be used to create a Docker image (`sdl/simjoin-docker`) from the executable:

```sh
$ docker build -t sdl/simjoin-docker .
```

This docker image can then be used to launch a web service application at a specific port (e.g., 8090) as follows:

```sh
$ docker run -p 8090:8080 sdl/simjoin-docker:latest --service
```

Once the service is launched, requests can be sent as mentioned above in order to create, manage, and query instances of SimJoin against data source(s).

## License

The contents of this project are licensed under the [Apache License 2.0](https://github.com/SLIPO-EU/loci/blob/master/LICENSE).

## Acknowledgement

This software is being developed in the context of the [SmartDataLake](https://smartdatalake.eu/) project. This project has received funding from the European Union’s [Horizon 2020 research and innovation programme](https://ec.europa.eu/programmes/horizon2020/en) under grant agreement No 825041.
