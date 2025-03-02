<img width="64" src="yt/docs/images/logo.png"/><br/>


[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ytsaurus/ytsaurus/blob/main/LICENSE)
[![Telegram](https://img.shields.io/badge/chat-on%20Telegram-2ba2d9.svg)](https://t.me/ytsaurus)

## YTsaurus

[Website](https://ytsaurus.tech) |
[Documentation](https://ytsaurus.tech/docs) |
[YouTube](https://www.youtube.com/@ytsaurus) 

YTsaurus is a distributed storage and processing platform for big data with support for MapReduce model, a distributed file system and a NoSQL key-value database.

You can read [post about YTsaurus](https://medium.com/p/42e7f5fa5fc6) or check video:

[![video about YTsaurus](yt/docs/images/ytsaurus-promo-video.png)](https://youtu.be/4Q2EB_uimLs)

## Advantages of the platform

### Multitenant ecosystem
* A set of interrelated subsystems: MapReduce, an SQL query engine, a job schedule, and a key-value store for OLTP workloads.
* Support for large numbers of users that eliminates multiple installations and streamlines hardware usage
### Reliability and stability
* No single point of failure 
* Automated replication between servers 
* Updates with no loss of computing progress
### Scalability
* Up to 1 million CPU cores and thousands of GPUs 
* Exabytes of data on different media: HDD, SSD, NVME, RAM 
* Tens of thousands of nodes 
* Automated server up and down-scaling
### Rich functionality
* Expansive MapReduce module 
* Distributed ACID transactions
* A variety of SDKs and APIs 
* Secure isolation for compute resources and storage 
* User-friendly and easy-to-use UI
### CHYT powered by ClickHouse®
* A well-known SQL dialect and familiar functionality
* Fast analytic queries 
* Integration with popular BI solutions via JDBC and ODBC
### SPYT powered by Apache Spark
* A set of popular tools for writing ETL processes
* Launch and support for multiple mini SPYT clusters 
* Easy migration for ready-made solutions

## Getting Started

Try YTsaurus cluster [using Kubernetes](https://ytsaurus.tech/docs/en/overview/try-yt#kubernetes) or try our [online demo](https://ytsaurus.tech).

## How to Build from Source Code
* Build [from source code](BUILD.md).

## How to Contribute

We are glad to welcome new contributors!

Please read the [contributor's guide](CONTRIBUTING.md) and the [styleguide](yt/styleguide/styleguide.md) for more details.
