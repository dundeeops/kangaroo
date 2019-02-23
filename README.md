# Kangaroo Framework (NodeJS :heart: Big Data)

<p align="center">
  <img width="300" src="./logo.svg">
</p>

> `The framework is under development`

## Overview

This is is an open-source distributed general-purpose cluster-computing framework (Big Data processing tool).

> `Complex solutions require simple tools`

Kangaroo uses streams, so in order of that you need to manipulate your data only throught ones. You should divide your processings into steps and segregate data by keys (or without them to spread as widely as possible throught the processing servers). To store your data there is a possobility to use tools like HDFS or smth else. To add a reliability and increase processing capacity you can implement accumulator streams.

Before using this tool you need to be shure of understanding Streams in NodeJS [NodeJS Stream Docs](https://nodejs.org/api/stream.html).

## Features

  - MapReduce segregation by [stage, key (null for Map, non null for Reduce)] couples
  - Stream & EventStream
  - Accumulating data
  - Custom Reliability
  - Callbacks of stage endings
  - Binary applications with stream processing (Java, Go, ...)
  - Static & Dynamic clasterization
  - NodeJS

## Todo

> Read readme.todo

## Examples

  - Non implemented yet

## Additional Testing Commands

To run etcd:

```
docker run -d --restart unless-stopped \
    -p 2379:2379 \
    -p 4001:4001 \
    --name etcd \
    127.0.0.1:5000/elcolio/etcd:latest \
    -listen-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001 -advertise-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001
```
