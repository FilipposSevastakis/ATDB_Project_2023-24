# Queries' Guide
## Execution:
For Queries 0-3 one only has to execute the following shell command:
```bash
spark-execute --num-executors <NUM_EXECUTORS> <YOUR_SPARK_JOB_FILE_PATH>
```
where <NUM_EXECUTORS> an (optional) configuration parameter that defines how many executors will be used (in total amongst the datanodes).


For the 4th Query, for the distance calculation of two (latitude, longitude) coordinations I opted for _geopy_, a Python library that provides tools for geocoding, and in particular it's function geopy.distance.geodesic. As with any other library one might use, it has to be installed in a way to also ensure that it will be distributed to the worker nodes when running the query. To that end, one may execute the following commands to create a python environment, to install geopy and zip it along with all of it's dependencies:
```bash
sudo apt install python3.10-venv
python3 -m venv myenv
source myenv/bin/activate
pip install geopy
cd myenv/lib/python3.10/site-packages
zip -r geopy_files.zip ./*
```
Since I did this outside of my VM's I also had to send the zipped file to the _queries_ directory of the _master_ VM using the command below:
```bash
scp ./geopy_files.zip user@snf-39850.ok-kno.grnetcloud.net:./queries
```
If the aforementioned procedure is successful, one can run the query using the following command:
```bash
spark-execute --num-executors <NUM_EXECUTORS> --py-files <PATH_TO_ZIPPED_FILES> <YOUR_SPARK_JOB_FILE_PATH>
```
where <PATH_TO_ZIPPED_FILES> the path to any zipped file one might want to distribute to the workers to be used in the query execution.

## Comment:
While implementing the 4th Query, the 
