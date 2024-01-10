# Temp-Inc
'Always Right Temp Inc' temperature anomalies solution 

## Recruitment Task Description

Introducing 'Always Right Temp Inc' 🌡️🌞, the masters of temperature sensors! They've entrusted you to create an anomaly detector.
To prove the solution is working end to end, you should create Temperature Measurement Generator, send as events to Kafka stream, implement anomaly detection algorithms, store anomalies in database and provide an API to query for anomaly statistics.
Don't forget to include unit and integration tests to ensure the quality and functionality of the project.

To detect anomalies, we have two algorithms to choose from:

### Anomaly Detection Algorithm ONE:
Label a temperature measurement as an anomaly if, within any 10 consecutive measurements, *one* reading is higher than the average of the *remaining 9* by 5 degrees Celsius.
Example:
```20.1, 21.2, 20.3, 19.1, 20.1, 19.2, 20.1, 18.1, 19.4, 20.1, 27.1, 23.1```

Anomaly detected: 27.1

### Anomaly Detection Algorithm TWO:
Define an anomaly as *any* measurement that differs from the average of *all* readings by 5 degrees Celsius within a 10-second window based on the measurement timestamp.
Example:

```
19.1 1684945005
19.2 1684945006
19.5 1684945007
19.7 1684945008
19.3 1684945009
25.1 1684945010
18.2 1684945011
19.1 1684945012
19.2 1684945013
25.4 1684945015
```
Anomalies detected: 25.1, 25.4


![Solution design overview](doc/image1.png?raw=true "Solution design overview")

## What is expected from you

We provided a skeleton and sample code to allow you to start quickly, feel free to use it or write solution from scratch if you prefer. 
Modules and their description:
* temperature-generator - service responsible for sample data generation. You should implement better [TemperatureGenerator](temperature-generator%2Fsrc%2Fmain%2Fjava%2Fio%2Fkontak%2Fapps%2Ftemperature%2Fgenerator%2FTemperatureGenerator.java) that will allow you to test solution and demo it with realistic data. Integration test framework using TestContainers is provided there.
* temperature-domain - module for kafka event data models and other shared domain objects.
* anomaly-detector - service responsible for processing temperature measurements, it should contain two [AnomalyDetector](anomaly-detector%2Fsrc%2Fmain%2Fjava%2Fio%2Fkontak%2Fapps%2Fanomaly%2Fdetector%2FAnomalyDetector.java) algorithms implementations. Sample code is provided there and integration tests, you need to adapt it.
* anomaly-storage - role of this service is persisting detected anomalies to the database of your choice.
* temperature-analytics-api - please implement REST API here, at least following endpoints: 
  * list of anomalies per thermometer id
  * list of anomalies per room id
  * list of thermometers with amount of anomalies higher that threshold (provided as query param or from properties)

Goal is to implement solution flow end to end, from data generator, to anomaly detection, storing detected anomalies in database and have a REST API to query for anomaly analytics.
  
Repository is configured as template, please do not fork it, you can create a copy of this repo, [docs here](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template#creating-a-repository-from-a-template).