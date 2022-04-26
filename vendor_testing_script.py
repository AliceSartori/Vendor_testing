
'''This data pipeline allows to injest patient level data received by 3 external stakeholders through our SFTP using different tecniques
- upload data in bulk using to_sql() in chuncks to not overload the server
- upload data to sql server creating patient id while doind data maninipations to clean the dataset
- upload data checking for duplicates and error in addresses after doing some other data manipulations

Audit tables on SQL server were created to store information of name of the file and date of processing it
A slack bot was also created to get name of the vendor, name of the file uploaded, and count of the rows uploaded to the database.This solution was preferred to email notifications'''


#import dependeces
import pysftp
import warnings
import datetime
import paramiko
import pathlib
from config import uri_kiteworks,sftp_pwd_kiteworks, uri_rrt,sftp_pwd_rrt, server_info, url_slack,  absolute_path
import datatable as dt
import time
import pandas as pd

# import database libraries
import pyodbc
import urllib
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

#import math and numpy to deal with NaNs
import math
import numpy

# import to create slack bot
import requests
import json

# disable the warnings
warnings.filterwarnings('ignore')


'''Use SQlAlchemy to write code agnostic to SQL syntax by creating Python objects that represents SQL syntax. Map SQL tables
 already created in SQL server database by creating classes for SQlAlchemy. Each class corresponds to a table already.'''

Base = declarative_base()

# Medrite Patient table
class medRitePatient(Base):

	__tablename__ = 'medrite_patient'
	__table_args__ = {"schema": "ven"}
	frst_nm = Column(Integer)
	lst_nm = Column(Integer)
	zipcode=Column(String)
	patient_id=Column(Integer, primary_key=True)
	dob=Column(Integer)
	street=Column(String)
	crtd_dt=Column(String)

# Medrite Test table
class medRiteTest(Base):

	__tablename__ = 'medrite_tst'
	__table_args__ = {"schema": "ven"}
	tst_id = Column(Integer, primary_key=True)
	tst_dt = Column(String)
	tst_time=Column(String)
	patient_id=Column(Integer)
	city=Column(String)
	state=Column(String)
	gndr=Column(String)
	tst_type=Column(String)
	tst_status=Column(String)
	tst_rslt=Column(String)
	crtd_dt=Column(String)

# Audit Tables
class rrtFiles(Base):

	__tablename__ = 'rrt_files'
	__table_args__ = {"schema": "ven"}
	filenm_id = Column(Integer, primary_key=True)
	filenm = Column(String)
	crtd_dt = Column(String)
	vndr_nm = Column(String)

class medriteFiles(Base):

	__tablename__ = 'medrite_files'
	__table_args__ = {"schema": "ven"}
	filenm_id = Column(Integer, primary_key=True)
	filenm = Column(String)
	crtd_dt = Column(String)
	vndr_nm = Column(String)

class premierFiles(Base):

	__tablename__ = 'premier_files'
	__table_args__ = {"schema": "ven"}
	filenm_id = Column(Integer, primary_key=True)
	filenm = Column(String)
	crtd_dt = Column(String)
	vndr_nm = Column(String)

# Premier table
class premier(Base):

	__tablename__ = 'premier'
	__table_args__ = {"schema": "ven"}
	tst_id = Column(String, primary_key=True)
	patient_id = Column(Float)
	gndr=Column(String)
	tst_dt=Column(String)
	tst_type=Column(String)
	tst_status=Column(String)
	tst_rslt=Column(String)
	tst_st_type=Column(String)
	tst_lction=Column(String)
	tst_addrss=Column(String)
	fclty_zipcode=Column(Integer)
	state=Column(String)
	crtd_dt=Column(String)
	city=Column(String)



'''write a function that takes the team slack webhook and a message to post in the designated slack channel'''

def slack_message(url, message):
    payload = {"text": message}
    payload_string = json.dumps(payload)
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    r = requests.post(url, data=payload_string, headers=headers)


''' Write a function that takes files from a target folders in sftp kiteworks and save the list of files uploaded to that target folder in a
designated local folder for each vendor'''

def pull_from_sftp(target_folder, local_folder, vendor_name):

    # today= (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
    today = datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m-%d')
    print(today)

    # opens connection to RRT SFTP using paramiko module
    if(vendor_name == 'rrt'):
	    hostname, port = uri_rrt, 22
	    sftp_uid = 'nychhc'
	    t = paramiko.Transport((hostname, port))
	    t.connect(username=sftp_uid, password=f'{sftp_pwd_rrt}')
	    sftp = paramiko.SFTPClient.from_transport(t)

    else:
	    #opens connection to kiteworks
	    hostname, port = uri_kiteworks, 22
	    sftp_uid = 's-t2dapfiles'
	    t = paramiko.Transport((hostname, port))
	    t.connect(username=sftp_uid, password=f'{sftp_pwd_kiteworks}')
	    sftp = paramiko.SFTPClient.from_transport(t)


    # change directory to enter the targeted folder

    sftp.chdir("/" + target_folder)


   # connect to the SQL database
    quoted = urllib.parse.quote_plus(f'DRIVER={{SQL Server}};SERVER={server_info};DATABASE=T2DAP')
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted), fast_executemany=True)
    conn = engine.connect()
    session = Session(engine)

    found_files = []

 	# return a list of files in each target directory making a check in the database first based on
	# the vendor's name and filename in order to not process files with the same name for each vendor.

    for filename in sftp.listdir():
        print(filename)
        split_name = filename.split('_')

        if(split_name[0].lower() == vendor_name and split_name[1] == today and filename.split('.')[-1] == 'csv'):

            if(vendor_name == 'medrite'):
                result = session.query(medriteFiles).filter(medriteFiles.filenm == filename, medriteFiles.vndr_nm == vendor_name)
                # print(result)
                if(result.count() == 0):
                    found_files.append(filename)
            elif(vendor_name == 'rrt'):
                result = session.query(rrtFiles).filter(rrtFiles.filenm == filename, rrtFiles.vndr_nm == vendor_name)
                if(result.count() == 0):
                    found_files.append(filename)
            else:
                result = session.query(premierFiles).filter(premierFiles.filenm == filename, premierFiles.vndr_nm == vendor_name)
                if(result.count() == 0):
                    found_files.append(filename)


	#	get each file in found files list into a local folder for each vendor
    for file in found_files:
        if file is not None:
            print(f'Downloading file {file}')
            sftp.get(file, (local_folder + "/" + file))

	# Close SFTP connections
    sftp.close()

    return found_files


'''write a function that read the files in each local folder'''

def manipulate_data(local_folder, filename):

	df = dt.fread(f'{local_folder}\{filename}').to_pandas()
	return df
    # local_folder, filename


'''Write a function that takes the new dataframe of each vendor and upload the records for each one to the database'''

def update_database(vendor_name, df_vendor):

    count_rows_inserted = 0
    state_list = [ 'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
           'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
           'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
           'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
           'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

    quoted = urllib.parse.quote_plus(f'DRIVER={{SQL Server}};SERVER={server_info};DATABASE=T2DAP')
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted), fast_executemany=True)
    conn = engine.connect()

	# if the vendor is RRT: upload the dataframe in bulk to the first vendor table after renaming the columns using fast_executemany paramenter and chunks for memory
    ctrl_debug = True
    if(vendor_name =='rrt' and ctrl_debug == True):
        # print(df_vendor)
        session = Session(engine)
        df_vendor.rename(columns={'Collection Date':'tst_dt','Facility Name':'fclty_nm','Site Name':'site_nm','Order Id':'ord_id','flu_a_result':'flu_rslt_a','flu_b_result':'flu_rslt_b','Result':'rslt','Order Type':'ord_type','Patient Zip':'patient_zipcode','Patient Id':'patient_id'},inplace=True)
        df_vendor['crtd_dt']= datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_df = df_vendor
        start = time.time()
        new_df.to_sql('rrt', con=engine, schema='ven', if_exists='append', chunksize=50,index=False, method='multi')

        count_rows_inserted = new_df.shape[0]
        stop = time.time()
        print(f'TIME ELAPSED {stop-start}')


     # upload filename and vendor name to the RRT audit table
        new_row = rrtFiles( filenm = filename,
 								vndr_nm = vendor_name,
 								crtd_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))



        try:
            session.add(new_row)
            session.commit()

        except Exception as e:
            raise e

	# if the vendor is MedRite: remane colum and drop the test and patient id from the dataframe.These value will need to be engineer through queying the sql tables
	# loop over the medrite's dataframe to check:
 		# - if the patient_id doesn't exist in the related table, SQL will create it with the serial identity key. Patient_id newly created is the FK of the test table ans it will be extracted
		#to upload the corresponding matching test data to the test table
	    # - if the patient_id already already exists in the patient table, that patient_id will be matched to the relative test data that will be uploaded to the test table.


    if(vendor_name =='medrite' and ctrl_debug == True):
        df_vendor.rename(columns={'TestDate':'tst_dt','TestTime':'tst_time','FirstName':'frst_nm',	'LastName':'lst_nm','D.O.B':'dob','Patient_ID':'patient_id','Test_ID':'tst_id','Street':'street','City':'city','Zip':'zipcode','State':'state','Gender':'gndr','TestType':'tst_type','TestStatus':'tst_status','TestResult':'tst_rslt'},inplace=True)
        df_vendor['crtd_dt']= datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


        df_test = df_vendor.drop(columns=['tst_id','patient_id'])
        session = Session(engine)

	# loop over the medrite's dataframe.

        for ii,row in df_test.iterrows():

			#check for the zipcode in case it is a NaN value
            zipcode = row['zipcode']
            try:
                zipcode = float(zipcode)
                if(math.isnan(zipcode)):
                    zipcode = 'UNKNOWN 1'
                else:
                    zipcode = row['zipcode']
            except ValueError:
                print ("Not a float")
                zipcode = 'UNKNOWN 2'

			# check for when the Test_dt column has 'ID'values in it
            if(row['tst_dt'] !='ID'):
                tst_dt = row['tst_dt']
            else:
                tst_dt = None
			# check for state acronym in the state_list
            state = row['state']
            if(state not in state_list):
                state = None



            try:
                result = session.query(medRitePatient).filter(medRitePatient.frst_nm == row['frst_nm'],
															  medRitePatient.lst_nm == row['lst_nm'],
															  medRitePatient.zipcode == zipcode ,
															  medRitePatient.dob == row['dob'],
															  medRitePatient.street == row['street'])

            except Exception as e:
                raise e

            try:
                present = result.count()
            except Exception as e:
                print(row)
                present = -1
                pass


			# parameters decided to create a unique patient id= first name, last name, zipcode, dob, street
            if( present == 0):
                print('INSERT PATIENT')

                new_row = medRitePatient(frst_nm = row['frst_nm'],
										lst_nm = row['lst_nm'],
										zipcode = zipcode,
										dob = row['dob'],
										street = row['street'],
										crtd_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


                try:
                    session.add(new_row)
                    #commit immediatelly to refresh the database in case the same patient is present in the same file
                    session.commit()

                except Exception as e:
                    raise e

				# patient id newwly created matched with its correspendent patient tests data
                patient_id = new_row.patient_id


                new_row = medRiteTest(tst_dt = tst_dt ,
 										tst_time = row['tst_time'],
 										patient_id = patient_id,
 										#street = row['street'],
 										city = row['city'],
										state= state,
										gndr=row['gndr'],
										tst_type=row['tst_type'],
										tst_status=row['tst_status'],
										tst_rslt=row['tst_rslt'],
 										crtd_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                try:
                    session.add(new_row)
                    session.commit()
                    count_rows_inserted += 1
                except Exception as e:
                    raise e

			# if the patient is already the table patient, result of the query and associate to the test data
            elif(present == 1):
                patient_id_extracted = result[0].patient_id
                # print(f'Value to use:{patient_id_extracted}')



                # state = row['state']
                # if(state not in state_list):
                #     state = None

                new_row = medRiteTest(tst_dt = tst_dt ,
 										tst_time = row['tst_time'],
 										patient_id = patient_id_extracted,
 										# street = row['street'],
 										city = row['city'],
										state= state,
										gndr=row['gndr'],
										tst_type=row['tst_type'],
										tst_status=row['tst_status'],
										tst_rslt=row['tst_rslt'],
 										crtd_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                try:
                    session.add(new_row)
                    session.commit()
                    count_rows_inserted += 1
                except Exception as e:
                    raise e




        new_row = medriteFiles( filenm = filename,
								vndr_nm = vendor_name,
								crtd_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


        try:
            session.add(new_row)
            session.commit()
        except Exception as e:
            raise e


# if the vendor name of the file is premier, some columns gets renamed and instanciated as empty columns
    ctrl_debug = True
    if(vendor_name =='premier' and ctrl_debug == True):
         # print(df_vendor)
        session = Session(engine)
        df_vendor.rename(columns={'EMR #':'patient_id','Gender':'gndr','Collected':'tst_dt','Requisition #':'tst_id','Status':'tst_status','Result':'tst_rslt','Test Site Type':'tst_st_type','Test Location':'tst_lction','Facility Address':'tst_addrss_tmp','Facility Zip':'fclty_zipcode'},inplace=True)
        df_vendor['tst_type'] = 'PCR'
        df_vendor['crtd_dt']= datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_vendor['city'] = ''
        df_vendor['state'] = ''
        df_vendor['tst_addrss'] = ''
        df_vendor['patient_id']=df_vendor['patient_id'].astype(float)

	# script created to avoid additional empty rows at the end of this vendor's data set
        df_vendor = df_vendor[ df_vendor['tst_id'] != '']

 	# create empty lists for errors and duplicated datapoints
        error_premier_address = []
        duplicated_premier = []

        for ii, row in df_vendor.iterrows():

	# To check for the test_id existence (it is the primary key), we query the table to see if the test_id is already present. If there is no match,
	# the condition is veryfied and we need to insert the record by following one of the patterns designed for the address after the split of the row.
	# If the address doesn't follow any of the pattern, the entire row will be appended in a empty list

           try:
               result = session.query(premier).filter(premier.tst_id == row['tst_id'])
           except Exception as e:

               raise e

		#initialize the variable
           ctrl = False

           if(result.count() == 0):
               split_address = row['tst_addrss_tmp'].split(',')
               if(len(split_address) == 4 or len(split_address) == 3):
                   # df_vendor.at[ii ,'tst_addrss']
                   address = split_address[0]
                   city = split_address[1]
                   state = split_address[2]
				   # set ctrl equal to True as variable to control
                   ctrl = True
               elif(len(split_address) == 5):
                    address =  f'{split_address[0]} {split_address[1]}'
                    city = split_address[2]
                    state = split_address[3]

					# set ctrl equal to True as variable to control
                    ctrl = True
               else:
                   print('ERROR ADDRESS')
                   print(ii)
                   print(row)
                   error_premier_address.append(row)

			   # if the variable ctrl is equal to true, that means that one of the two conditions for splitting rows above was met
			   # and the row will be inserted (as there is no correspondent match in the database)
               if(ctrl == True):
                   new_row = premier(tst_id = row['tst_id'],
                                   patient_id = row['patient_id'],
                                   gndr = row['gndr'],
                                   tst_dt = row['tst_dt'],
                                   tst_type = row['tst_type'],
                                   tst_status = row['tst_status'],
                                   tst_rslt = row['tst_rslt'],
                                   tst_st_type = row['tst_st_type'],
                                   tst_lction = row['tst_lction'],
                                   tst_addrss = address,
                                   fclty_zipcode = row['fclty_zipcode'],
                                   state = state,
								   city = city,
								   crtd_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                   try:
                       session.add(new_row)
                       session.commit()
                       count_rows_inserted += 1
                   except Exception as e:
                   # print(new_row)
                       print('ERROR INSERT')
                       print(row)
                       input('WAIT')
                       raise e

    # if there are already records in database with the same test id, the duplicated rows will be appended to an empty list
           else:
               print('Premier DUPLICATE test')
               print(row)
               duplicated_premier.append(row)

	# the 2 list are saved in a dataframe and saved to csv

        if (error_premier_address):
            address_df = pd.DataFrame(error_premier_address)
			 # del new_df['index']
            # print(address_df)
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            address_df.to_csv(f'premier_errorfile_{today}.csv', index=False)

        if (duplicated_premier):
             duplicated_df = pd.DataFrame(duplicated_premier)
				 # del new_df['index']
             print(duplicated_df)
             today = datetime.datetime.now().strftime('%Y-%m-%d')
             duplicated_df.to_csv(f'premier_duplicatedfile_{today}.csv', index=False)


        new_row = premierFiles(filenm = filename,
  							   vndr_nm = vendor_name,
  							   crtd_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # print(new_row)

        try:
            session.add(new_row)
            session.commit()

        except Exception as e:
            raise e

    return count_rows_inserted


#entrypoint
if __name__ == '__main__':


# the scope is to connect to connect to a target folder on kitework and download a file in a local folder for each vendor
# the key value pair structure of the python dictionary is used  to store target and local folder for each vendor in a list as a value and name of the vendor as key.

    vendors = {'medrite':['MedRite_project', 'Medrite_localfolder'],
               'rrt':['RRT','RRT_localfolder'],
               'premier':[ 'Premier','Premier_localfolder']}
    # vendors = { 'rrt':['RRT','RRT_localfolder']}
    # vendors = { "medrite":['MedRite_project', 'Medrite_localfolder']}
    # vendors = {'premier':[ 'Premier','Premier_localfolder']}

  # create dictionary with key as name of the vendors and values a list with the columns from the data sets
    vendor_columns = {'rrt':['Collection Date','Facility Name','Site Name','Order Id','flu_a_result','flu_b_result','Result','Order Type','Patient Zip','Patient Id'],
                            "medrite":['TestDate','TestTime', 'FirstName','LastName','D.O.B','Patient_ID','Test_ID','Street','City','Zip','State','Gender','TestType','TestStatus','TestResult'],
                            'premier':['EMR #','Gender','Collected', 'Status','Result','Test Site Type', 'Test Location', 'Facility Address','Facility Zip','Requisition #']}


	# use absolute path to allow the scheduler to find the file


	#call the function for slack bot
    message ='start checking new files'
    slack_message(url_slack, message)

	# loop over the dictionary of vendors to extract from each values the first element of the list (the target folder) and the second element (the local folder)
    for vendor_key in vendors:
		# print(today)
        target = vendors[vendor_key][0]

        local = f'{vendors[vendor_key][1]}'

        print(f"FROM MAIN:{target}, {local}")

		# call the pull from sftp function and entered the parameters needed, returning a list of file (the files will always be one per day per construction)
        files2manipulate = pull_from_sftp(target_folder=target, local_folder=local , vendor_name=vendor_key)
        print(files2manipulate)

		# if the condition is verified, there are files in the list, get each of them printed, send the name to slack chat
        if(files2manipulate):
            print(f'VENDOR {target}')

            for filename in files2manipulate:
               print(filename)
               slack_message(url_slack,filename)

			   # read the file using the function manipulate data to get a df
               df_vendor = manipulate_data(local, filename)

			   # to get the column list needed in the dataframe that I am looping on, I extract the values from the vendor_columns dictionary and named a variable
               vendor_columns_list = vendor_columns[vendor_key]
			   # I subset the dataframe that i am reading using the list of
               df2upload = df_vendor[vendor_columns_list]

               print('UPDATE DATABASE PHASE')

			   #upload each dataframe  with the data manipulations in update_database and return the rows_count_database for each vendor
               rows_count_database= update_database(vendor_key, df2upload)
               message=f"The count of new rows uploaded of {vendor_key} in the database is {rows_count_database}"
               slack_message(url_slack,message)


		# if the condition is verified, there areno files in the list, a message of no file uploaded by each vendor gets sent to the slack channel
        else:
            message=f'No file uploaded by {vendor_key}'
            slack_message(url_slack,message)
