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
import datetime

#disease.sh object
client = diseaseapi.Client().covid19

### DATA PULL FUNCTIONS

# Function to get data of all countries in a single call 
async def get_allCountries(queryName):
	dataFormatted = []
	dataList = await client.all_countries() #get for a country
	vaxlist = await get_VaxAllCountriesLatest(queryName)
	
	for x in range(len(dataList)):
	# for x in range(2):
		data = dataList[x]
		region = data.name
		
		#to get Vaccine data for same region 
		for i in vaxlist:
			vax_list_region = i["region"].lower().strip() #Region in Vax list
			covid_list_region = region.lower().strip() #Region being checked against 
			if covid_list_region == vax_list_region:
				vax_data = i['vax_latest']
		if vax_data:
			vax_data = vax_data
		else:
			vax_data = 0 #just to avoid a key error in future 
		
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
			"Vaccinated":int(vax_data),
			"Updated":str(data.updated),
			"Cases Today":int(data.today.cases),
			"Deaths Today":int(data.today.deaths),
			"Recovered Today":int(data.today.recoveries),
			"sourceName": "Source: Worldometer / disease.sh", 
		})
	await client.request_client.close() #close the ClientSession
	return dataFormatted #List goes out

# Function to get data of all states of USA 
async def get_allUSStates(queryName):
	dataFormatted = []
	dataList = await client.all_states() #get for a country
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
	await client.request_client.close() #close the ClientSession
	return dataFormatted #List goes out


#Function for pulling data for world, for all else use other of all data
async def get_WorldData(queryName):
	dataFormatted = {}
	data = await client.all() #get global data
	vax = await get_VaxWorldLatest(queryName)
	vax_data = vax['vax_latest']
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
			"Vaccinated":int(vax_data),
			"Updated":str(data.updated),
			"Cases Today":int(data.today.cases),
			"Deaths Today":int(data.today.deaths),
			"Recovered Today":int(data.today.recoveries),
			"sourceName": "Source: Worldometer / disease.sh", 
	})
	
	await client.request_client.close() #close the ClientSession
	dataFormatted_list = [dataFormatted] #JSON to list 
	return dataFormatted_list #Single List goes out

#Function for pulling Vaccine data for entire World 
async def get_VaxWorldLatest(queryName):
	dataFormatted = {}
	data = await client.vaccine_coverage(1)
	vax_latest = data[0].value
	dataFormatted.update ({
			"areaTable":"CountryData",
			"query_name": queryName,   #Name of record in amPayload table
			"region": "World",
			"vax_latest":int(vax_latest),
			"sourceName": "Source: Worldometer / disease.sh", 
	})
	await client.request_client.close() #close the ClientSession
	return dataFormatted #List goes out

#Function for pulling Vaccine data for all countries
async def get_VaxAllCountriesLatest(queryName):
	dataFormatted = []
	dataList = await client.vaccine_countries(1) #gets 30 day data by default, Alt 15
	for x in range(len(dataList)):
		data = dataList[x]
		timeline = data.timeline
		timeline_data = []
		for i in timeline:
			timeline_data.append({
				'vax_date': str(i.date.date()), #just has date
				'vax_value': i.value
				})
		dataFormatted.append({
			"areaTable":"Vaccine_AllCountries",
			"query_name": queryName,   #Name of record in amPayload table
			"region": data.country,
			"vax_latest": timeline_data[0]['vax_value'],
		})
	await client.request_client.close() #close the ClientSession
	return dataFormatted #List goes out

#Function for pulling Vaccine data for all countries with a range across 15 days 
async def get_VaxAllCountries(queryName):
	dataFormatted = []
	dataList = await client.vaccine_countries(15) #gets 15 day data by default, Alt 15
	for x in range(len(dataList)):
		data = dataList[x]
		timeline = data.timeline
		timeline_data = []
		for i in timeline:
			timeline_data.append({
				'vax_date': str(i.date.date()), #just has date
				'vax_value': i.value
				})
		dataFormatted.append({
			"areaTable":"Vaccine_AllCountries",
			"query_name": queryName,   #Name of record in amPayload table
			"region": data.country,
			"vax": timeline_data,
		})
	await client.request_client.close() #close the ClientSession
	return dataFormatted #List goes out


def get_DiseaseData(payload_json, query_name):
	endpoint_name = payload_json['endpoint']
	if endpoint_name == "WorldData":
		data_pulled = asyncio.get_event_loop().run_until_complete(get_WorldData(query_name))
	elif endpoint_name == "allCountries":
		data_pulled = asyncio.get_event_loop().run_until_complete(get_allCountries(query_name))
	elif endpoint_name == "allUSStates":
		data_pulled = asyncio.get_event_loop().run_until_complete(get_allUSStates(query_name))
	elif endpoint_name == "VaxAllCountries":
		data_pulled = asyncio.get_event_loop().run_until_complete(get_VaxAllCountries(query_name))
	else:
		data_pulled = []
	return data_pulled

	## OLDER CODE - Keep for now 
	# dataAll = asyncio.get_event_loop().run_until_complete(get_allCountries()) #All countries
	# dataAll.extend(asyncio.get_event_loop().run_until_complete(get_allUSStates())) #All US States
	# dataAll.append(asyncio.get_event_loop().run_until_complete(get_WorldData())) #Only World Data
	# print ("Data Gathered for: World ..")
	# print ("Data Gathered for all.")
	# #Client connection closing is done only on last API call within the functions
	# return dataAll


## Testing
# dataAll = asyncio.get_event_loop().run_until_complete(get_WorldData("test")) #good 
# dataAll = asyncio.get_event_loop().run_until_complete(get_allUSStates("test"))
# dataAll = asyncio.get_event_loop().run_until_complete(get_allCountries("test"))
# dataAll = asyncio.get_event_loop().run_until_complete(get_VaxAllCountriesLatest("test"))
# dataAll = asyncio.get_event_loop().run_until_complete(get_VaxAllCountries("test"))
# dataAll = asyncio.get_event_loop().run_until_complete(get_VaxWorldLatest("test"))
# dataAll = get_DiseaseData({'endpoint':'allCountries'}, "test")
# print(dataAll)

