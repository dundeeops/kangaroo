# Kangaroo Framework (NodeJS :heart: Big Data)

<p align="center">
  <img width="300" src="./logo.svg">
</p>

> `The framework is under development`

## Overview

This is is an open-source distributed general-purpose cluster-computing framework (Big Data processing tool).

> `Complex solutions require simple tools`

Kangaroo uses streams, so in order of that you need to manipulate your data only throught ones. You should divide your processings into steps and segregate data by keys (or without them to spread as widely as possible throught the processing servers). To store your data there is a possobility to use tools like HDFS or smth else. To add a reliability and increase processing capacity you can implement accumulator streams.

Before using this tool you need to be shure of understanding Streams in [NodeJS Stream Docs](https://nodejs.org/api/stream.html).

## Features

  - MapReduce segregation by [stage, key (null for Map, non null for Reduce)] couples
  - Stream & EventStream
  - Accumulating data
  - Custom Reliability
  - Callbacks of stage endings
  - Binary applications with stream processing (Java, Go, ...)
  - Static & Dynamic clasterization
  - NodeJS

## To-do

> [TODO List](./readme.todo)

## Examples

  - Non implemented yet

## Documentation

> [Open Docs](./docs/index.md)

To increase memory limit for your node app (8gb):
```
node --max_old_space_size=8000 big-data-worker.js
```
