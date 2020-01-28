# SimJoin

## Overview

SimJoin is a Java library providing functions for set similarity joins (see [Jiang2014](http://www.vldb.org/pvldb/vol7/p625-jiang.pdf), [Mann2016](http://www.vldb.org/pvldb/vol9/p636-mann.pdf) and [Xiao2009](https://ieeexplore.ieee.org/document/4812465)) and fuzzy set similarity joins (see [Deng2017](http://www.vldb.org/pvldb/vol10/p1082-deng.pdf)). It currently supports the following functionalities:

- Threshold-based join: find all pairs with similarity score above a given threshold.
- kNN join: for each set, find its k-nearest neighbors.
- Top-k join: find k pairs having the highest similarity score.

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

**Step 3**. Edit the parameters in the `config.json` file.

**Step 4**. Execute by running:
```sh
$ java -jar target/simjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

## License

The contents of this project are licensed under the [Apache License 2.0](https://github.com/SLIPO-EU/loci/blob/master/LICENSE).
