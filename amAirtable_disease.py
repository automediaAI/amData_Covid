##############
#### Airtable Data Service for Disease ####
#################
## Feed it disease data. Pulls payload from airtable, uploads data in correct format as needed ##
##############
## Ticket: https://www.notion.so/automedia/Create-data-service-for-pulling-from-disease-sh-7aded63dac7a47c9b6ca768356e4cb6d 
#############


## Declarations 
import os
from airtable import Airtable
import json
from amLibrary_ETLFunctions import getSingleByRegion, topListByTitle, dataSingleParse, dataTableParse

# Airtable settings 
base_key = os.environ.get("PRIVATE_BASE_KEY")
table_name = os.environ.get("PRIVATE_TABLE_NAME")
api_key = os.environ.get("PRIVATE_API_KEY")
airtable = Airtable(base_key, table_name, api_key)

### DATA UPLOAD FUNCTIONS
#Uploads single json, or list to data_output of record ID as given
def uploadData(inputDictList, recToUpdate):
	recID = recToUpdate
	if isinstance(inputDictList, dict):
		fields = {'data_output': json.dumps(inputDictList)}
		# fields = {'data_output': str(inputDictList)} #Seems if I do str thats same too
	else:
		fields = {'data_output': str(inputDictList)}
	airtable.update(recID, fields)

## Runner, Loops through the Airtable table
#Goes through all records and updates ones that are in the master dict
def updateLoop(inputMasterDict):
	allRecords = airtable.get_all(view='Service - amData')
	
	for i in allRecords:
		if "Prod_Ready" in i["fields"]: #Only working on prod ready ie checkboxed
			payload_native = i["fields"]["payload"]
			payload_json = json.loads(payload_native)
			type_asked = payload_json["type"] #Single data, or table of data 
			rec_ofAsked = i["id"]

			#Different functions if List or Single data asked for
			if type_asked == "dataSingle":
				region_asked = payload_json["region"] #USA, World etc
				title_asked = payload_json["title"] #Cases, deaths etc
				areaTable = payload_json["areaTable"] #All Countries data, or US State data
				data_asked = getSingleByRegion(inputMasterDict, region_asked, areaTable) #Entire dict is sent
				data_toUpload = dataSingleParse(payload_json, data_asked) #Limited data based on payload ask
				uploadData(data_toUpload, rec_ofAsked) #Just that bit updated 

			elif type_asked == "dataTable":
				filterBy = payload_json["areaTable"] #So only that data goes
				sortBy = payload_json["sortBy"] #Top by Cases, Deaths etc
				listHowMany = int(payload_json["listHowMany"]) #Give back how many records
				data_asked = topListByTitle(inputMasterDict, sortBy, filterBy, listHowMany)
				data_toUpload = dataTableParse(payload_json, data_asked)
				uploadData(data_toUpload, rec_ofAsked)

			else:
				fields = {'data_output': "ERROR - Type incorrect"}
				airtable.update(rec_ofAsked, fields)

