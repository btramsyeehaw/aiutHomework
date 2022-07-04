import paho.mqtt.client as mqtt
import time
import random
import datetime
import json

mqttBroker = "mqtt.eclipseprojects.io"
client = mqtt.Client("data")
client.connect(mqttBroker)

unit = "V"

while True:
    x = datetime.datetime.now()
    czas = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    value = random.uniform(1900,2100)

    y = {"time": czas, "value": value, "unit": unit}
    z = json.dumps(y)
    client.publish("measurements", z)
    print("published "+z+" to measurements")
    time.sleep(random.random()/20)
