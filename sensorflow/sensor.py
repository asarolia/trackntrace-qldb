#!/usr/bin/python

# Make sure your host and region are correct.

import sys
import ssl
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient, AWSIoTMQTTClient
import json
import time
import random

#Setup our MQTT client and security certificates
#Make sure your certificate names match what you downloaded from AWS IoT

mqttc = AWSIoTMQTTClient("package_temp_sensor")

#Make sure you use the correct region!
mqttc.configureEndpoint("data.iot.us-east-1.amazonaws.com",8883)
mqttc.configureCredentials("./rootCA.pem","./privateKey.pem","./certificate.pem")



#This sends our test message to the iot topic
def send():
    #temp = random.randint(1, 10)
    temp = 12
    print(temp)
    message_data ={
      "temperature": temp,
      "package": "MFGPKG1",
      "batch": "ZAX42H"
    }
    
    mqttc.publish("packagesensortopic", json.dumps(message_data), 0)
    print("Message Published")


#Connect to the gateway
mqttc.connect()
print("Connected")

send()

mqttc.disconnect()
