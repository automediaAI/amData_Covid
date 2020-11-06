##############
#### Task Runner for Disease ####
#################
## Gets data from amData and then feeds to Airtable ##
##############
## Ticket: https://www.notion.so/automedia/Create-data-service-for-pulling-from-disease-sh-7aded63dac7a47c9b6ca768356e4cb6d 
#############

import os
from amData_disease import get_DiseaseData
from amAirtable_disease import updateLoop
from airtable import Airtable
import uuid
from datetime import datetime

# Airtable settings 
base_key = os.environ.get("PRIVATE_BASE_KEY")
table_name_dump = os.environ.get("PRIVATE_TABLE_NAME_DUMP")
api_key = os.environ.get("PRIVATE_API_KEY")
airtable_2 = Airtable(base_key, table_name_dump, api_key)

## Making API call to get data 
dataPulled = get_DiseaseData()

## Payload for uploading dump to airtable
UUID = 'CovidData-'+str(uuid.uuid1())
time_pulled = str(datetime.now())
input_payload = "All Countries, All States of USA"
amService = 'amData_Covid'
fields = {'UUID':UUID, 'time_pulled':time_pulled, 'data_output': str(dataPulled), 'input_payload':input_payload, 'amService':amService }
# Upload
airtable_2.insert(fields)

## Uploading real data to CMS
updateLoop(dataPulled)


## Testing
# print ('Data Pulled: ', dataPulled)
# print ('Data Pulled Type: ', type(dataPulled))
