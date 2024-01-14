
from kafka import KafkaProducer, KafkaClient
import json
from urllib import request as req
from datetime import date
from time import sleep
import re

################################################################################
# Get Data from EuroMillion
################################################################################
template_url = 'https://www.euro-millions.com/results-history-{YYYY}'

current_year = int(date.today().strftime('%Y'))+1
to_send = {}
try:
    for year in range(2004,current_year):
        '''
            Get the data from the HTML Page
        '''
        url = template_url.replace('{YYYY}',str(year))
        data_bytes = req.urlopen(url).read()
        data_text = data_bytes.decode('utf-8')

        '''
            Select only the rows which are interesting
        '''
        to_send[year] = {}
        new_month = False
        process_row = True
        tmp_month = ''
        for row in data_text.split('\n'):
            
            if process_row and tmp_month != '':
                check = re.sub(r'<(.|[^>]*)>|\s','',row)
                if len(check) > 0 and 'Draw' not in check:
                    data = re.sub(r'<(.|[^>]*)>|\s{2,}','',row)
                    to_send[year][tmp_month].append(data)
                    #print('row %s /data %s'%(row,data))

            if new_month:
                new_month = not new_month
                tmp_month = re.sub(r'<(.|[^>]*|\s{2,}])>','',row)
                to_send[year][tmp_month] = []
                #print('month %s'%tmp_month)

            if 'dateRow' in row: 
                new_month = not new_month
                process_row = not process_row

            if 'resultsRow' in row or 'tbody' in row:
                tmp_month = ''
                process_row = not process_row

        print(to_send)
        break
        #sleep(10)
except Exception as err:
    print('Issue with the url: %s'%err)


################################################################################
# Set up producer
################################################################################
'''
KAFKA = KafkaClient(bootstrap_servers='localhost:9092')
PRODUCER = KafkaProducer(
    bootstrap_servers='localhost:9092',
    client_id='producer-euromil'
)

TOPIC = 'euromil-topic'
'''

################################################################################
# Loop, add to kafka
################################################################################


