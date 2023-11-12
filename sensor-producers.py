import json
import random
from datetime import datetime
from time import sleep

import boto3

# Create a connection
cliente = boto3.client('kinesis', aws_access_key_id='CHANGEME',
                       aws_secret_access_key='CHANGEME',
                       region_name='CHANGEME')

# setting our time counter
time = 0

# creating dictionaries to save our random data
# these dictionaries will be send to our database in real time
pf_data = dict()
i = 0
bt_data = dict()
j = 0
hp_data = dict()
k = 0

while time >= 0:
    # Give us the current date
    date_now = datetime.now()

    # Give us the current day, month and year
    text_date = date_now.strftime("%d/%m/%Y")

    # Give us the current time ultil seconds
    text_time = date_now.strftime("%H:%M:%S")

    # power factor: value between 0.8 and 1
    pf = random.uniform(0.8, 1)

    # updating pf dictionary
    pf_data['idpf'] = i
    pf_data['pf_data'] = pf
    pf_data['date'] = text_date
    pf_data['time'] = text_time

    # converting pf_data as json
    pf_data_as_json = json.dumps(pf_data)

    # battery temperatura: value between 20ºC and 25ºC
    bt = random.uniform(20.0, 25.0)

    # updating bt dictionary
    bt_data['idbt'] = j
    bt_data['bt_data'] = bt
    bt_data['date'] = text_date
    bt_data['time'] = text_time

    # converting bt_data as json
    bt_data_as_json = json.dumps(bt_data)

    # hydraulic preassure: value between 70 and 80 BAR
    hp = random.uniform(70, 80)

    # updating hp dictionary
    hp_data['idhp'] = k
    hp_data['hp_data'] = hp
    hp_data['date'] = text_date
    hp_data['time'] = text_time

    # converting bt_data as json
    hp_data_as_json = json.dumps(hp_data)

    # Put data into stream
    resposta_pf = cliente.put_record(
                    StreamName='CHANGEME',
                    Data=pf_data_as_json,
                    PartitionKey='CHANGEME'
                    )
    # Put data into stream
    resposta_bt = cliente.put_record(
                    StreamName='CHANGEME',
                    Data=bt_data_as_json,
                    PartitionKey='CHANGEME'
                    )
    # Put data into stream
    resposta_hp = cliente.put_record(
                    StreamName='CHANGEME',
                    Data=hp_data_as_json,
                    PartitionKey='CHANGEME'
                    )

    # increasing values for data ids
    i = i + 1
    j = j + 1
    k = k + 1

    # wating 10 seconds to generate new data
    sleep(10)
