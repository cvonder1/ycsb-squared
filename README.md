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
Example benchmark configurations can be found in `de.claasklar.benchmark`.