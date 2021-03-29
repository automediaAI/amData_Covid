##############
#### Covid Data Puller Service ####
######## Part of am_Data #########
## Pulls covid data from disease.sh and updates in covid producer ##
##############
## Ticket: https://www.notion.so/automedia/Create-data-service-for-pulling-from-disease-sh-7aded63dac7a47c9b6ca768356e4cb6d 
#############
## Issue of certificates, solved using second bash command here - https://timonweb.com/python/fixing-certificate_verify_failed-error-when-trying-requests-html-out-on-mac/ 


## Declarations 
import diseaseapi
import asyncio

#disease.sh object
client = diseaseapi.Client().covid19

### DATA PULL FUNCTIONS

# Function to get data of all countries in a single call 
# async def get_allCountries(queryName):
def get_allCountries(queryName):
	dataFormatted = []
	# dataList = await client.all_countries() #get for a country
	dataList = client.all_countries() #get for a country
	for x in range(len(dataList)):
		data = dataList[x]
		dataFormatted.append({
			"areaTable":"CountryData",
			"query_name": queryName,   #Name of record in amPayload table
			"region": data.name,
			"Cases":int(data.cases),
			"Deaths":int(data.deaths),
			"Recovered":int(data.recoveries), 
			"Active Cases":int(data.active),
			"Critical":int(data.critical),
			"Tests":int(data.tests),
			"Updated":str(data.updated),
			"Cases Today":int(data.today.cases),
			"Deaths Today":int(data.today.deaths),
			"Recovered Today":int(data.today.recoveries),
			"sourceName": "Source: Worldometer / disease.sh", 
		})
	# await client.request_client.close() #close the ClientSession
	client.request_client.close() #close the ClientSession
	return dataFormatted #List goes out

# Function to get data of all states of USA 
# async def get_allUSStates(queryName):
def get_allUSStates(queryName):
	dataFormatted = []
	# dataList = await client.all_states() #get for a country
	dataList = client.all_states() #get for a country
	for x in range(len(dataList)):
		data = dataList[x]
		dataFormatted.append({
			"areaTable":"StateData_US",
			"query_name": queryName,   #Name of record in amPayload table
			"region": data.name,
			"Cases":int(data.cases),
			"Deaths":int(data.deaths),
			# "Recovered":data.recoveries, #Data not av for states
			"Active Cases":int(data.active),
			# "Critical":data.critical,
			"Tests":int(data.tests),
			# "Updated":data.updated,
			"Cases Today":int(data.today.cases),
			"Deaths Today":int(data.today.deaths),
			# "Recovered Today":data.today.recoveries, #Data not av for states
			"sourceName": "Source: Worldometer / disease.sh", 
		})
	# await client.request_client.close() #close the ClientSession
	client.request_client.close() #close the ClientSession
	return dataFormatted #List goes out

#Function for pulling data for world, for all else use other of all data
# async def get_WorldData(queryName):
def get_WorldData(queryName):
	dataFormatted = {}
	print ("Client", client)
	# data = await client.all() #get global data
	data = client.all() #get global data
	print ("data: ", data)
	dataFormatted.update ({
			"areaTable":"CountryData",
			"query_name": queryName,   #Name of record in amPayload table
			"region": "World",
			"Cases":int(data.cases),
			"Deaths":int(data.deaths),
			"Recovered":int(data.recoveries),
			"Active Cases":int(data.active),
			"Critical":int(data.critical),
			"Tests":int(data.tests),
			"Updated":str(data.updated),
			"Cases Today":int(data.today.cases),
			"Deaths Today":int(data.today.deaths),
			"Recovered Today":int(data.today.recoveries),
			"sourceName": "Source: Worldometer / disease.sh", 
	})
	
	# await client.request_client.close() #close the ClientSession
	client.request_client.close() #close the ClientSession
	dataFormatted_list = [dataFormatted] #JSON to list 
	return dataFormatted_list #Single List goes out


def get_DiseaseData(payload_json, query_name):
	endpoint_name = payload_json['endpoint']
	if endpoint_name == "WorldData":
		data_pulled = get_WorldData(query_name)
		print ("World data", data_pulled)
	elif endpoint_name == "allCountries":
		data_pulled = get_allCountries(query_name)
	elif endpoint_name == "allUSStates":
		data_pulled = get_allUSStates(query_name)
	else:
		data_pulled = []
	return data_pulled

	# dataAll = asyncio.get_event_loop().run_until_complete(get_allCountries()) #All countries
	# dataAll.extend(asyncio.get_event_loop().run_until_complete(get_allUSStates())) #All US States
	# dataAll.append(asyncio.get_event_loop().run_until_complete(get_WorldData())) #Only World Data
	# print ("Data Gathered for: World ..")
	# print ("Data Gathered for all.")
	# #Client connection closing is done only on last API call within the functions
	# return dataAll

