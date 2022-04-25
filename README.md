# Vendor Testing
----------------

## Background
-------------
The testing team has been collaborating with three different vendors to get data of New Yorkers and out of towner at healthcare facilities, testing sites and pop up mobile locations.
This repository houses the pipeline in Python.


## Project goal
---------------------
The main Python script establishes a connection to 2 SFTPs (RRT and Kiteworks), where the vendors upload the CSVs. The files get downloaded in local folders, read, manipulated to massage the data and create additional patient_id and test_id missing, and uploaded to SQL database.
The final output will get connected to Tableau and refreshed weekly through Tableau.


## Libraries Requirements
-------------------------
As of now, the script runs on a virtual machine. Please check if the following Python packages are installed in DAP VM before running the whole script: pyodbc, pandas, numpy, pysftp, paramiko.

## Setup
--------
It's recommended to create a folder for each vendor before running the script.

In the VM:
1. Create Vendor testing main folder where your Python script is saved and vendors subfolders where the local files will be downloaded.
2. Schedule a cron job in task scheduler to run or an orchestrator can be choosen.
