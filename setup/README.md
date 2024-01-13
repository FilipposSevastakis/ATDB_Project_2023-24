# Installations and Setup:
## VMs' Setup:
For the implementation of this project I used "~okeanos", a free-for-academic-purposes IAAS of GRNET (Greek Research & Technology Network). For the access and setup of the VMs, which include a private network and (optional) upgrades of their OS, we were given thorough instructions found in (enter: hyperlink)

The system is comprised of:
- 2 VMs:
  - _okeanos-master_ node: Role of Master and Slave node for Spark and role of Namenode and Datanode for HDFS
  - _okeanos-worker_ node: Role of Slave node for Spark and of Datanode for HDFS
  
  each with 4 CPUs, 8GB RAM and 30GB disk capacity
- Private Network

## Installation and configuration of Spark (over YARN) and HDFS:
The steps followed for the installation of Apache Spark over Hadoop's resource manager, YARN, and of Hadoop's Distributed File System and for the configuration of the necessary environment variables can also be found in the instructions (enter: hyperlink) given.

## Starting HDFS, YARN and Spark's History Server:
If the abovementioned steps have been followed and the setup is successfully completed, one can start the HDFS, YARN and Spark's History Server with the following commands, respectively:
```bash
start-dfs.sh
```
HDFS web interface: http://83.212.73.51:9870
```bash
start-yarn.sh
```
YARN web interface: http://83.212.73.51:8088/cluster
```bash
$SPARK_HOME/sbin/start-history-server.sh
```
Spark History Server web interface: http://83.212.73.51:18080

### Additional Configuration:
Regarding Spark's History Server I noticed that by default there wasn't any "cleaner" activated, meaning that after many Query executions, that were made, the eventLog that is stored in the HDFS took up too much space (as much as the data at some point). To solve that, as well as prevent it from happening again I added the following lines in the spark-defaults.conf file (found in my file system in the ./opt/bin/spark-3.5.0-bin-hadoop3/conf folder):
```bash
spark.history.fs.cleaner.enabled true
spark.history.fs.cleaner.maxAge  12h
spark.history.fs.cleaner.interval 1h
```
