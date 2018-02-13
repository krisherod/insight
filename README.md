# insight

6thScent: Detecting methane alerts in real time


Project motivation:

In California there are over 23 000 idle methane and oil wells. In 2016 one of these wells leaked over 100 000
tons of methane into the atmosphere. This had a climate impact equivalent to 500 000 cars for one whole year.

The aim of my project is to use a large network of methane sensors in order to detect methane leaks as soon as
they occur. Detecting leaks may be difficult because sensors may be faulty and send bad or out of order data.



Use Cases:

1. To detect if there is a methane leak by checking if a sensor exceeds the threshold for a leak multiple times
2. Throw out values if there is no agreement between collocated sensors
3. Identify if sensors are consistently reading faulty values compared to the average value
4. Calibrate methane sensors with a separate temperature stream in real time to generate a stream of calibrated methane data


Processing:

- Complex Event Processing (CEP) engine was used in order to define the patterns for detecting methane leaks in real time
- Windowed Aggregration function was used to calculate the average value of the sensors
- Connected Stream function was used to connect methane and temperature streams in order to calibrate the methane data in real time



Pipeline

- Kafka: for sensor data ingestion
- Flink: for processing a stream of data
- PostgreSQL: for storage of data
- Flask: for visualization of alerts, and faulty sensors


![alt text](https://raw.githubusercontent.com/username/projectname/Methane Leak Detection Network.jpg)



Engineering Challenges

- Processing out of order sensor values
- Connecting multiple streams of data together in real time
