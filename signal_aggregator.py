import paho.mqtt.client as mqtt
import time
import statistics
import sys
import psycopg2

def avg(l):
  return sum(l)/len(l)

try:
  n=int(sys.argv[1])
  k=float(sys.argv[2])
except:
  print("pass in 2 arguments, first N, then T")
  exit()  

#connecting with MQTT brokerem
mqttBroker = "mqtt.eclipseprojects.io"
client = mqtt.Client("measurements")
client.connect(mqttBroker)

client.loop_start()
client.subscribe("measurements")

#connecting with postgreSQL
conn = psycopg2.connect(dbname="aiutzd", user = "postgres", password = "admin123", host = "localhost")

data_buffer = []

#adding data to the buffer if it isn't full, sending data back to the ignored thread if it is
def on_message(client,data,message):
  if len(data_buffer)<n:
    data_buffer.append(message.payload.decode("utf-8"))
  else:
    client.publish("ignored",message.payload)

#main portion of the program
print("aggregating data once received "+str(n)+" inputs or "+str(k)+" seconds have passed")
input("Press enter to start the program")
print("Press ctrl+c to exit the program")
t1 = time.perf_counter()
t2 = time.perf_counter()
try:
  while True:
    #once the difference betweeen the end of previous aggregation and last added datapoint reaches T, the next aggregation commences
    #once the data buffer has n datapoints, the aggregation commences
    while t2-t1<k and len(data_buffer)<n:
      client.on_message = on_message
      t2 = time.perf_counter()
    #empty data is never sent
    if len(data_buffer)>0:
      data_dict = {}
      for x in range(len(data_buffer)):
        data_buffer[x] = eval(data_buffer[x])
        data_buffer[x]["time"] = data_buffer[x]["time"][:-1].replace("T"," ")
        for key, v in data_buffer[x].items():
          data_dict.setdefault(key, []).append(v)
      #aggregating and inserting the data
      cur = conn.cursor()
      cur.execute("INSERT INTO dane VALUES (%s, %s, %s, %s, %s, %s, %s)", (data_dict["time"][0],data_dict["time"][len(data_buffer)-1],len(data_buffer),min(data_dict["value"]),max(data_dict["value"]),statistics.median(data_dict["value"]),avg(data_dict["value"])))
      conn.commit()
      cur.close()
      print("Inserted a datapoint into the database in "+str(t2-t1)+" seconds. 'n' = "+str(len(data_buffer)))
      data_buffer.clear() #clearing the buffer 
      t1 = time.perf_counter()
except KeyboardInterrupt:
  print("Program exited successfully")

