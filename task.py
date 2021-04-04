##############
#### Task Runner for Disease ####
#################
## Gets data from amData and then feeds to Airtable ##
##############
## Ticket: https://www.notion.so/automedia/Create-data-service-for-pulling-from-disease-sh-7aded63dac7a47c9b6ca768356e4cb6d 
#############

import os
import json
import uuid
import boto3 #to upload larger files to S3
from botocore.exceptions import ClientError
from airtable import Airtable
from datetime import date, datetime, timedelta
from amData_disease import get_DiseaseData, get_WorldData, get_allCountries, get_allUSStates, get_VaxAllCountries
import asyncio

# Airtable settings 
base_key = os.environ.get("PRIVATE_BASE_KEY")
# table_name_data = os.environ.get("PRIVATE_TABLE_NAME_DATAPAYLOAD") #What to pull
table_name_data = "amPayload_Data" #What to pull
table_name_dump = os.environ.get("PRIVATE_TABLE_NAME_SERVICEDUMP") #Output dump
api_key_airtable = os.environ.get("PRIVATE_API_KEY")
airtable_data = Airtable(base_key, table_name_data, api_key_airtable)
airtable_dump = Airtable(base_key, table_name_dump, api_key_airtable)

## Amazon S3 settings 
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_region='us-west-1' #Manual while creating the bucket 

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

UUID = 'CovidData-'+str(uuid.uuid1())

#Uploads single json, or list to data_output of record ID as given
def uploadData(inputDictList, recToUpdate):
	recID = recToUpdate
	if isinstance(inputDictList, dict):
		fields = {'output': json.dumps(inputDictList)}
		# fields = {'data_output': str(inputDictList)} #Seems if I do str thats same too
	else:
		fields = {'output': str(inputDictList)}
	airtable_data.update(recID, fields)

# Dumping to service Dump after all is run
def dumpData(inputURL):
	time_pulled = str(datetime.now())
	amService = 'amData_CovidData'
	data_output = str(inputURL)
	fields = {'UUID':UUID, 'time_pulled':time_pulled, 'data_output': data_output, 'amService':amService }
	airtable_dump.insert(fields)

# # Dumping to service Dump after all is run
def dumpToS3(file_name, bucket='amnewsbucket', object_name=None):
    # If S3 object_name was not specified, use file_name
    url_s3 = f"https://{bucket}.s3-us-west-2.amazonaws.com/{file_name}" #Manually creating structure
    object_name = file_name
    try:
        response = s3.upload_file(file_name, bucket, object_name, ExtraArgs={'ACL':'public-read'})
        return url_s3
    except ClientError as e:
        return ('🚫Error uploading to S3: '+str(e))
 

# Running through rows of news, calling newsAPI, uploading data back
def updateDataLoop():
	print ('Started loop..') #Extra to keep app going 
	table_output = [] #Final data of entire pull
	allRecords = airtable_data.get_all() #Get all records 
	print ('All records recieved..') #Extra to keep app going 
	for i in allRecords:
		if "Prod_Ready" in i["fields"]: #Only working on prod ready ie checkboxed
			print ('Started row..') #Extra to keep app going 
			if "Service" in i["fields"]:
				# Basic payload, common to all
				payload_native = i["fields"]["payload"]
				payload_json = json.loads(payload_native)
				rec_ofAsked = i["id"]
				query_name = i["fields"]["Name"] #Just to differentiate what is being called
				service_name = i["fields"]["Service"].lower()
				# Calling News service per ask
				if service_name == 'diseasesh': #Only pulling if NewsAPI 	
					print ("Entered loop")

					### Tried repeating what get_DiseaseData does here directly - But same error 
					# endpoint_name = payload_json['endpoint']
					# if endpoint_name == "WorldData":
					# 	data_pulled = asyncio.get_event_loop().run_until_complete(get_WorldData(query_name))
					# elif endpoint_name == "allCountries":
					# 	data_pulled = asyncio.get_event_loop().run_until_complete(get_allCountries(query_name))
					# elif endpoint_name == "allUSStates":
					# 	data_pulled = asyncio.get_event_loop().run_until_complete(get_allUSStates(query_name))
					# elif endpoint_name == "VaxAllCountries":
					# 	data_pulled = asyncio.get_event_loop().run_until_complete(get_VaxAllCountries(query_name))
					# else:
					# 	data_pulled = []
					# row_output = data_pulled

					row_output_unclean = get_DiseaseData(payload_json, query_name) #NewsAPI output for this call
					row_output = row_output_unclean
					# row_output = newsClean(row_output_unclean)
				else:
					row_output = "🚫Query requested is invalid"
				# Appending rest
				print ('Row data done..') #Extra to keep app going 
				table_output.append(row_output) #Adding to all data
				## Running Text Cleaning and Image Cleaning functions 
				data_toUpload = row_output #Uploading clean data
				# data_toUpload = row_output #Uploading clean data
				uploadData(data_toUpload, rec_ofAsked) #Upload back to Airtable 
				print('Row complete..')
	
	# filename = (UUID+'.txt')
	# #Creating a local text file 
	# f = open(filename,"w")
	# f.write( str(table_output) )
	# f.close()
	# url_s3_file = dumpToS3(filename) #uploading to S3 and getting file back
	# dumpData(url_s3_file) #Adding final output to service dump
	# os.remove(filename) #deleting file after upload
	# print('Table complete.')

print ('Entering loop.')

updateDataLoop()
