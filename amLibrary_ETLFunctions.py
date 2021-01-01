##############
#### Common Library of ETL Functions ####
#################
## Feed it disease data. Feed it queries. It parses and gives back just format needed ##
##############
## Ticket: https://www.notion.so/automedia/Create-data-service-for-pulling-from-disease-sh-7aded63dac7a47c9b6ca768356e4cb6d 
#############


## Declarations 
import pandas as pd

### EXTRACTION using Pandas - Add more to get more out of data
#Return data by region 
def getSingleByRegion(allData, regionToGet, areaTable):
	for i in allData:
		if (i['region'] == regionToGet) and (i['areaTable'] == areaTable):
			return i 
	return [] #adding to return blank in case region not found, as error check

# Get Top x by Use case
def topListByTitle(allData, sortBy, filterBy, listHowMany):
	df = pd.DataFrame(allData)
	df_filtered = df.loc[df['areaTable'].isin([filterBy])] #Filters by US state or Country data 
	sorted_df = df_filtered.sort_values(by = sortBy , ascending = False)
	if 'World' in sorted_df.values: #To skip first value if World is in list, so hard coding for country vs states list 
		rslt_df = sorted_df.head(listHowMany+1)[1:] 
	else:
		rslt_df = sorted_df.head(listHowMany)
	top_list_values = rslt_df.values.tolist()
	top_list_headers = rslt_df.columns.tolist()

	# There def must be a better pandas way of doing this, but for now couldnt figure out a way to export in format I wanted
	final_list = []
	for i in top_list_values:
		temp_dict = {}
		ind_temp = 0
		for j in i:
			temp_dict[top_list_headers[ind_temp]] = j
			ind_temp += 1
		final_list.append(temp_dict)
	return final_list


### TRANSFORMATION FUNCTIONS
# Matching payload to get only limited items in mapping as wanted
def dataSingleParse(payloadToCheck, dataToFillFrom):
	outputDict = {}
	# In payload the values are keys to data dump dict
	for key, value in payloadToCheck['data_needed'].items():
		if value in dataToFillFrom: 
			if isinstance(dataToFillFrom[value], int):
				outputDict[key] = ("{:,}".format(dataToFillFrom[value])) 
			else:
				outputDict[key] = dataToFillFrom[value] 
	#Adding remaining values manually
	outputDict['type'] = payloadToCheck['type']
	outputDict['title'] = payloadToCheck['title']
	outputDict['region'] = payloadToCheck['region']
	return outputDict

# Matching payload from dataTable
def dataTableParse(payloadToCheck, dataListToFillFrom):
	outputDict = {
		"table_info": {
			"type":payloadToCheck['type'],
			"title":payloadToCheck['title'],
			"sourceName":"",
		},
		"rows" : []
	}
	dataList = []
	keyTemp = 0
	for dataToFillFrom in dataListToFillFrom: 
		tempDict = {}		
		for key, value in payloadToCheck['data_needed'].items():
			if value in dataToFillFrom: 
				if isinstance(dataToFillFrom[value], int):
					tempDict[key] = ("{:,}".format(dataToFillFrom[value])) 
				else:
					tempDict[key] = dataToFillFrom[value]
		tempDict['key'] = str(keyTemp) 
		keyTemp = keyTemp + 1 
		dataList.append(tempDict)
	outputDict["rows"] = dataList
	outputDict["table_info"]["sourceName"] = dataList[0]["sourceName"]
	return outputDict
