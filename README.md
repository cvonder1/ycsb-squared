# README
This project's goal is to provide a MongoDB database admin with a customizable benchmark
framework to test different database schemas for the use cases at hand.

## Setup
To get up and start a benchmark, launch the docker-compose stack:
```
podman-compose -f docker-compose.local.yml up --build
```
Running the complete benchmark takes multiple hours.
Access to traces: [Jaeger](http://localhost:16686)\
Access to metrics: [Prometheus](http://localhost:9090)

## Examples
To get a first impression how the framework can be used, have a look at `de.claasklar.benchmark.LogisticBenchmark`.
`SSB` creates a MongoDB benchmark according to the Star Schema Benchmark.
