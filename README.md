# ATDB_Project_2023-24
Advanced Topics in Databases, ECE NTUA - Course Project (2023/24)

## 🎯 Objectives:
1. Successful installation, configuration and management of distributed systems (namely Apache Spark and Apache Hadoop, with Spark being executed on top of Hadoop's resource manager, YARN).
2. Use and comparison of Spark's APIs and of their configurations when querying (large volumes of) data.

## :page_facing_up: Description: Data-sets and Queries:
### Data-sets:
The primary data-set is the Los Angeles Crime Data, sourced from the public repository of the United States' goverment, which can be found in the following links:
- https://catalog.data.gov/dataset/crime-data-from-2010-to-2019
- https://catalog.data.gov/dataset/crime-data-from-2020-to-present

Additional data-sets that are used regard:
- The LA Police Stations:
  - https://geohub.lacity.org/datasets/lahub::lapd-police-stations/explore
- The Median Household Income by Zip Code (Los Angeles County) and a data-set of pre-processed Reverse Geocoding information, both of which can be found in the following zip:
  - http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz
---
### Queries:
**Query 0:** Create a DataFrame that contains the main data-set. Keep the original column names but change the column types as instructed below:
- Date Rptd: date
- DATE OCC: date
- Vict Age: integer
- LAT: double
- LON: double

**Query 1:** Find, for each year, the top-3 months with highest number of recorded crimes committed. You are asked to print the month, year, number of criminal acts recorded, as well as the ranking of the month within the respective year. Results are expected to be sorted in ascending order with respect to the year and descending order with respect to the number of crimes.

**Query 2:** Sort the different parts of the day taking into account crimes that were committed on the (STREET), in descending order. Consider the following pars of the day:
- Morning: 5.00am – 11.59am
- Afternoon: 12.00pm – 4.59pm
- Evening: 5.00pm – 8.59pm
- Night: 9.00pm – 3.59am
Print the total number of rows for the entire data-set and the data type of every column.

**Query 3:** Find the descent of the victims of recorded crimes in Los Angeles for the year 2015 in the 3 ZIP Code areas with the highest and the 3 ZIP Codes with the lowest income per household (that have crimes recorded). Results are expected to be printed from highest to lowest number of victims per ethnic group.

**Query 4:** For the last query, you are asked to examine whether the police stations that respond to recorded crimes in Los Angeles are the closest ones to the crime scene. Towards that end, you are asked to implement and execute two pairs of similar queries and compare results:
1. a) Calculate the number of crimes committed with the use of firearms of any kind and the average distance (in km) of the crime scene to the police station that handled the case. The results should appear ordered by year in ascending order.
  b) Additionally, calculate the same stats (number of crimes committed with the use of firearms of any kind and average distance) per police station. Results should appear ordered by number of incidents, in descending order. 

2. a) Calculate the number of crimes committed with the use of firearms of any kind and the average distance (in km) of the crime scene to the police station that is located closest to the crime scene. The results should appear ordered by year in ascending order.
   b) Additionally, calculate the same stats (number of crimes committed with the use of firearms of any kind and average distance) per police station. Results should appear ordered by number of incidents, in descending order.


For the implementation of this project 





