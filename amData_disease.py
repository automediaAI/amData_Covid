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
async def get_allCountries():
	dataFormatted = []
	dataList = await client.all_countries() #get for a country
	for x in range(len(dataList)):
		data = dataList[x]
		dataFormatted.append({
			"areaTable":"CountryData",
			"region": data.name,
			"Cases":data.cases,
			"Deaths":data.deaths,
			"Recovered":data.recoveries, 
			"Active Cases":data.active,
			"Critical":data.critical,
			"Tests":data.tests,
			"Updated":data.updated,
			"Cases Today":data.today.cases,
			"Deaths Today":data.today.deaths,
			"Recovered Today":data.today.recoveries,
			"sourceName": "Source: Worldometer / disease.sh", 
		})
	# await client.request_client.close() #close the ClientSession
	return dataFormatted #List goes out

# Function to get data of all states of USA 
async def get_allStates():
	dataFormatted = []
	dataList = await client.all_states() #get for a country
	for x in range(len(dataList)):
		data = dataList[x]
		dataFormatted.append({
			"areaTable":"StateData_US",
			"region": data.name,
			"Cases":data.cases,
			"Deaths":data.deaths,
			# "Recovered":data.recoveries, #Data not av for states
			"Active Cases":data.active,
			# "Critical":data.critical,
			"Tests":data.tests,
			# "Updated":data.updated,
			"Cases Today":data.today.cases,
			"Deaths Today":data.today.deaths,
			# "Recovered Today":data.today.recoveries, #Data not av for states
			"sourceName": "Source: Worldometer / disease.sh", 
		})
	# await client.request_client.close() #close the ClientSession
	return dataFormatted #List goes out

#Function for pulling data for world, for all else use other of all data
async def get_WorldData():
	dataFormatted = {}
	data = await client.all() #get global data
	dataFormatted.update ({
			"areaTable":"CountryData",
			"region": "World",
			"Cases":data.cases,
			"Deaths":data.deaths,
			"Recovered":data.recoveries,
			"Active Cases":data.active,
			"Critical":data.critical,
			"Tests":data.tests,
			"Updated":data.updated,
			"Cases Today":data.today.cases,
			"Deaths Today":data.today.deaths,
			"Recovered Today":data.today.recoveries,
			"sourceName": "Source: Worldometer / disease.sh", 
	})
	
	await client.request_client.close() #close the ClientSession
	return dataFormatted #Single JSON goes out


def get_DiseaseData():
	dataAll = asyncio.get_event_loop().run_until_complete(get_allCountries()) #All countries
	dataAll.extend(asyncio.get_event_loop().run_until_complete(get_allStates())) #All US States
	dataAll.append(asyncio.get_event_loop().run_until_complete(get_WorldData())) #Only World Data
	#Client connection closing is done only on last API call within the functions

	return dataAll

