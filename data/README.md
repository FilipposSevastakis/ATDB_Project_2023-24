# Acquiring the Data and loading HDFS:
## Data acquisition:
I initially collected all the data-sets in this (data) directory in okeanos-master node using the following commands:
- For the primary (Los Angeles crimes) data-set:
  ```bash
  wget -O crime_incidents_2010-2019.csv https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD
  ```
  ```bash
  wget -O crime_incidents_2020-.csv https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD
  ```
- For the LAPD Stations (Query 4) I downloaded the csv locally (in my machine) from https://geohub.lacity.org/datasets/lahub::lapd-police-stations/explore and then executed the following from the file's location to send the csv to the master:
  ```bash
  scp ./LAPD_Police_Stations.csv user@snf-39850.ok-kno.grnetcloud.net:./data
  ```
- The Median Household Income by Zip Code and the Reverse Geocoding data-sets were provided in a zip folder in the following link: http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz. For their acquisition I executed the following commands:
  ```bash
  wget http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz
  ```
  ```bash
  tar -xf data.tar.gz
  ```
  ```bash
  sudo rm data.tar.gz
  ```
  ```bash
  mv income/LA_income_2015.csv ./
  ```
  ```bash
  rm -r income
  ```
  The above result in only LA_income_2015.csv and revgecoding.csv	to be added along crime_incidents_2010-2019.csv, crime_incidents_2020-.csv and LAPD_Police_Stations.csv in the data folder.

## Loading the HDFS:
For the insertion of all csv files from the data folder into the HDFS, I created a data folder in the HDFS and then added the files with the following commands, respectively:
```bash
hadoop fs -mkdir hdfs://okeanos-master:54310/data
```
```bash
hadoop fs -put ./* hdfs://okeanos-master:54310/data
```
_At this point, one can remove the data folder or its contents from the okeanos-master node_

### :warning: Important Notice
_A cluster is balanced if there is no under-utilized or above-utilized data node in the cluster.
An under-utilized data node is a node whose utilization is less than (average utilization – threshold).
An over-utilized data node is a node whose utilization is greater than (average utilization + threshold)._
> Reference: https://hadoop.apache.org/docs/r1.2.1/commands_manual.html -> Administration Commands -> balancing -> Rebalancer
		
The threshold is user configurable and can be set or changed from a command line, but has a default value of 10% utilization. Since our data hold less than the 10% of the space of the first datanode (okeanos-master), no rebalancing takes place and it's that node that holds all our data. Therefore, we have to execute rebalancing:
```bash
hdfs balancer -threshold 1		# threshold is in percentage of the clusters' disk capacity (1% is around 300MB)
```
