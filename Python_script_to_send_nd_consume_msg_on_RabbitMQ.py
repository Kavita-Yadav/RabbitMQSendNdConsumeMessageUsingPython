# 1. Install RabbitMQ
# 2. Change password and username: guest, guest_pwd
# 3. Add exchange Eg: Name: test_exchange
# 4. Add queue Eg: test_queue
# 5. Run python script

# Consume RabbitMQ queue
import os
import sys
import pika
import logging
import yaml
import time
import datetime
import ujson
import json
import traceback
import pandas as pd
import schedule, threading
from typing import Dict
from clickhouse_driver import Client

logger = logging.getLogger("pyAlarm")

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', pika.PlainCredentials('guest', 'jCq*A5{5EquU]3)/25qD{Gmv')))
channel = connection.channel()

# test data when you are not using clickhouse data
exchange='test_exchange'
routingkey='test_route'
Messagebody='Hello From The Other Side!'

"""
.##......##.####.########.##.....##.....######..##.......####..######..##....##.##.....##..#######..##.....##..######..########
.##..##..##..##.....##....##.....##....##....##.##........##..##....##.##...##..##.....##.##.....##.##.....##.##....##.##......
.##..##..##..##.....##....##.....##....##.......##........##..##.......##..##...##.....##.##.....##.##.....##.##.......##......
.##..##..##..##.....##....#########....##.......##........##..##.......#####....#########.##.....##.##.....##..######..######..
.##..##..##..##.....##....##.....##....##.......##........##..##.......##..##...##.....##.##.....##.##.....##.......##.##......
.##..##..##..##.....##....##.....##....##....##.##........##..##....##.##...##..##.....##.##.....##.##.....##.##....##.##......
..###..###..####....##....##.....##.....######..########.####..######..##....##.##.....##..#######...#######...######..########
"""

# convert clickhouse query data into dataframe
def formatToDataframe(_client, _query):
    result = _client.execute(_query, with_column_types=True)
    header = pd.DataFrame(result[1])
    df = pd.DataFrame(result[0])
    df.columns = header[0].tolist()
    return df  

# get clickhouse data
def getClickhouseData(datamart, data_base,clickhouse_query):
    with Client(host=datamart,database=data_base, compression=True) as client:
        clickhouse_table_data= formatToDataframe(client, clickhouse_query)
        clickhouse_table_data.reset_index()
    
    return clickhouse_table_data

# publish message function when you are using clickhouse data
def sendClickhouseMsg(datamart, data_base, clickhouse_query):

    mlMsg = ""

    clickhouse_data = getClickhouseData(datamart, data_base, clickhouse_query)
    # loop all alarms in the state
    for index in clickhouse_data.index:
        alarmMsg = clickhouse_data["string_col1"][index] + ";" + clickhouse_data["string_col2"][index] + ";" + str(clickhouse_data["int_col3"][index])
        if mlMsg != "":
            mlMsg = mlMsg + ","
        mlMsg = mlMsg + alarmMsg

    sendMsg("test_route", mlMsg)
    logger.debug(f"mlMsg =  {mlMsg}")

# decode the rabbit mq Messagebody 
def decodeBody(config,Messagebody: bytes):
    try:
        bodyStr = Messagebody.decode()
        bodyList = bodyStr.split(';')

        sourceBody = dict()
        i = 0
        for field in config.content["sourceEventMsgField"]:
            sourceBody[field] = bodyList[i]
            i = i + 1

        dateObj = datetime.datetime.strptime(sourceBody["EventDatetime"], "%Y-%m-%d %H:%M:%S") # Event time
        sourceBody["EventTime"] = int(datetime.datetime.timestamp(dateObj))
        sourceBody["AcknowledgeRequired"] = int(sourceBody["AcknowledgeRequired"])
        sourceBody["ValueState"] = int(sourceBody["ValueState"])

        bSuccess = True
        return bSuccess, sourceBody

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        bSuccess = False
        return bSuccess, dict()

# process each rabbit mq Messagebody
def processBlock(ch, method, properties, Messagebody):
    #print("body = ", body.decode())

    bSuccess, sourceBody = decodeBody(Messagebody)

    if bSuccess == False:
        logger.warn(f"Failed to decode message: {Messagebody.decode()}")
        return

    #logger.debug(f"{sourceBody['EquipmentName']} - {sourceBody['PointLabel']} - {sourceBody['PointValueLabel']} - {CUtil.timestampToStr(sourceBody['EventTime'])} - {sourceBody['ValueState']} - {sourceBody['AcknowledgeRequired']}")
    logger.debug(f"{sourceBody['superset']} - {sourceBody['subset']} - {sourceBody['min_sup']}")
    #stateKey = sourceBody['AlarmOnOffKey']

    #processState(stateKey, sourceBody['ValueState'], sourceBody['AcknowledgeRequired'], sourceBody['EventTime'], sourceBody)

    ch.basic_ack(delivery_tag = method.delivery_tag)

"""
.##......##.####.########.##.....##..#######..##.....##.########.....######..##.......####..######..##....##.##.....##..#######..##.....##..######..########
.##..##..##..##.....##....##.....##.##.....##.##.....##....##.......##....##.##........##..##....##.##...##..##.....##.##.....##.##.....##.##....##.##......
.##..##..##..##.....##....##.....##.##.....##.##.....##....##.......##.......##........##..##.......##..##...##.....##.##.....##.##.....##.##.......##......
.##..##..##..##.....##....#########.##.....##.##.....##....##.......##.......##........##..##.......#####....#########.##.....##.##.....##..######..######..
.##..##..##..##.....##....##.....##.##.....##.##.....##....##.......##.......##........##..##.......##..##...##.....##.##.....##.##.....##.......##.##......
.##..##..##..##.....##....##.....##.##.....##.##.....##....##.......##....##.##........##..##....##.##...##..##.....##.##.....##.##.....##.##....##.##......
..###..###..####....##....##.....##..#######...#######.....##........######..########.####..######..##....##.##.....##..#######...#######...######..########
"""

# call back function for basic_consume in connect function when you are not using clickhouse
def callback(ch, method, properties, Messagebody):
    print(f'{Messagebody} is received')

# start consumption of the rabbit mq, blocking call here - Common function for both case clickhouse or without clickhouse
def consumeMsg():
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

# publish message to the rabbit mq by using routing key to get message on rabitmq board - Common function for both case clickhouse or without clickhouse
def sendMsg(routingKey: str, Messagebody: str):
    channel.basic_publish(exchange=exchange,
                            routing_key=routingKey,
                            body=Messagebody)

# load yaml config - Common function for both case clickhouse or without clickhouse
def yamlConfig(configFileName):
        with open(configFileName,'r') as yamlstream:
            content = yaml.safe_load(yamlstream)

# connect to rabbit mq - Common function for both case clickhouse or without clickhouse
def connect(serverName: str, serverPort: int, userName: str, userPassword: str, consumeQueue: str):
    # username, password are first stored in credentials and later pass to connection parameters
    credentials = pika.PlainCredentials(userName, userPassword)
    parameters = pika.ConnectionParameters(serverName,
                                            serverPort,
                                            '/',
                                            credentials, heartbeat=600, blocked_connection_timeout=600)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # bind queue to exchange by declaring routeing key
    channel.queue_bind(exchange=exchange, queue=consumeQueue,
                           routing_key=routingkey)

    # performance improvement
    channel.basic_qos(prefetch_count=10000)

    # declare which queue message to be consumed which is already pubished at GetMessage
    channel.basic_consume(queue=consumeQueue, on_message_callback=callback, auto_ack=False)



if __name__ == '__main__':
    logger = logging.getLogger("pyAlarm")
    # Clickhouse parameters if using clickhouse data
    #datahost= 'localhost'
    #data_base= 'Test_Db'

    #queryFpGrowth = f'''
    #SELECT * FROM Test_db.Test_Table
    #'''
    # end clickhouse parameters
    try:
        logSteamHandler = logging.StreamHandler()
        # logger for file
        logFileHandler = logging.FileHandler("pyscript.log")
        # default logger format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        logSteamHandler.setFormatter(formatter)
        logFileHandler.setFormatter(formatter)
        logger.addHandler(logSteamHandler)
        logger.addHandler(logFileHandler)

        # default log level
        logger.setLevel(logging.DEBUG)

        # if using yaml configurations
        #config = yamlConfig("pyscriptYamlConfig.yaml")

        connect("localhost", 5672, "guest", "guest_pwd", "test_queue")

        logger.debug("CRabbitMq IN")

        #sendClickhouseMsg(datahost, data_base, queryFpGrowth)
        # send manual msg not clickhouse data
        sendMsg(routingkey, Messagebody)

        # rabbitMq.timer(60, rabbitMq.stateCleanup)

        # consume msg
        #consumeMsg()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    except Exception as e:
        logger.error(e)
        # print the stack trace when exception
        logger.error(traceback.format_exc())
