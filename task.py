##############
#### Task Runner for Disease ####
#################
## Gets data from amData and then feeds to Airtable ##
##############
## Ticket: https://www.notion.so/automedia/Create-data-service-for-pulling-from-disease-sh-7aded63dac7a47c9b6ca768356e4cb6d 
#############

from amData_disease import get_DiseaseData
from amAirtable_disease import updateLoop

dataPulled = get_DiseaseData()
updateLoop(dataPulled)

# print ('Data Pulled: ', dataPulled)
# print ('Data Pulled Type: ', type(dataPulled))
