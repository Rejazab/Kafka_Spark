
from kafka import KafkaProducer, KafkaClient
import json
from urllib import request as req
from datetime import date
from time import sleep
import re

################################################################################
# Set up producer
################################################################################
try :
    KAFKA = KafkaClient(bootstrap_servers='localhost:9092')
    PRODUCER = KafkaProducer(
        bootstrap_servers='localhost:9092',
        client_id='producer-euromil'
    )

    TOPIC = 'euromil-topic'
except Exception as err:
    print('The following error happended while setting up Kafka: %s'%err)

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
        tmp_month = ''
        tmp_data = ''
        for row in data_text.split('\n'):
            
            #If we are at the end of the resultrow, store the datas and reset the temporary variable
            if '/tr' in row and re.search('[a-zA-Z0-9]',tmp_data):
                #print('month %s / data %s'%(tmp_month,tmp_data[:-2]))
                to_send[year][tmp_month].append(tmp_data[:-2])
                tmp_data = ''

            #Check validity of the data and save in the temporary variable
            if tmp_month != '':
                check = re.sub(r'<(.|[^>]*)>|\s','',row)
                if len(check) > 0 and 'Draw' not in check:
                    data = re.sub(r'<(.|[^>]*)>|\s{2,}','',row).replace('\r','')
                    tmp_data += data + '--'

            #If start of new month, create an entry in the dictionnary
            if new_month:
                new_month = not new_month
                tmp_month = re.sub(r'<(.|[^>]*)>|\s{2,}','',row).replace('\r','')
                to_send[year][tmp_month] = []

            #Check if it's a new month and reset temporary month in this case
            if 'dateRow' in row: 
                new_month = not new_month
                tmp_month = ''


        #print(to_send)
        if year == 2005:
            break
        
        ################################################################################
        # Loop, add to kafka
        ################################################################################
        try:
            #avro_push(rec)
            future = PRODUCER.send(TOPIC, value=dumps(to_send).encode('utf-8'))
            info = future.get(timeout=10)
        except Exception as err:
            info =  str(err)
            pass
        print('pushed: %s / info %s' % (to_send,info))
        sleep(10)
        
except Exception as err:
    print('Met the following error: %s'%err)





