# insight


Detect methane leaks using a large array of gas sensors.

To detect if there are unusually high methane concentrations in real time, identify if there are faulty sensors by comparing concentrations to moving average of nearby sensors, and wind direction and speed of nearest met station.

Technologies used to solve this problem are kafka, flink, spark, cassandra, 

Proposed architecture:
- kafka for ingestion
- cassandra for raw storage
- flink for streaming

