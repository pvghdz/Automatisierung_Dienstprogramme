import json, paho.mqtt.client as mqtt
import time

"""
Functions used to control Shelly devices through MQTT.
There has to exist an MQTT broker to which the shelly devices are connected
You can use the MQTT Explorer App for Mac to monitor the communications and send/receive data
"""

BROKER = "<Broker-IP>"
DEVICE_ID = "<Device-ID>"   # from the device MQTT settings
CLIENT_ID = "<Client-ID>"
INTERVAL_SEC = 3
SRC = "<MQTT Client-ID>"                    # MQTT client_id (also used for replies)
USERNAME = None                             # or "mqttuser"
PASSWORD = None                             # or "mqttpass"

def switch(on: bool, relay_id: int = 0):
    """
    Switches on/off the shelly device
    """
    cmd = {"id": 1, "src": SRC, "method": "Switch.Set", "params": {"id": relay_id, "on": on}}
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=SRC, protocol=mqtt.MQTTv5)
    if USERNAME: c.username_pw_set(USERNAME, PASSWORD)
    c.connect(BROKER, 1883, 60)
    c.publish(f"{DEVICE_ID}/rpc", json.dumps(cmd), qos=1)
    c.disconnect()
    
def rpc(client, method, params=None):
    """
    Used to build and send an MQTT “RPC-style” message
    Asks a device to execute a method, with optional parameters, over MQTT
    """
    payload = {"id": int(time.time()), "src": CLIENT_ID, "method": method}
    if params: payload["params"] = params
    client.publish(f"{DEVICE_ID}/rpc", json.dumps(payload), qos=1)

def on_connect(c, u, f, rc, props=None):
    """
    Subscribes to the specified topic
    """
    c.subscribe(f"{CLIENT_ID}/rpc")  # replies
    
def on_message(c, u, msg):
    """
    Converts the MQTT message payload from:
	bytes -> string -> Python dict (JSON)
    """
    resp = json.loads(msg.payload.decode())
    if resp.get("result"):
        if resp.get("method") == "Switch.GetStatus" or resp["result"].get("id") == 0:
            print("Switch:", "ON" if resp["result"].get("output") else "OFF")
        if "apower" in resp["result"] or "current" in resp["result"]:
            print("Power:", resp["result"].get("apower"), "W",
                  "Current:", resp["result"].get("current"), "A",
                  "Voltage:", resp["result"].get("voltage"), "V")

"""
Example use
"""

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID, protocol=mqtt.MQTTv5)
client.on_connect, client.on_message = on_connect, on_message
client.connect(BROKER, 1883, 60)
client.loop_start()

print("Trying to switch off")
# Turn on, then off:
switch(False)   # off
print("Switched off")

time.sleep(5)
print("Sleep for 5s.")

switch(True)  # on
print("Switched back on")

try:
    while True:
        rpc(client, "Switch.GetStatus", {"id": 0})
        # Some models expose measurements via EM/PM components; if needed:
        # rpc(client, "EM.GetStatus", {"id": 0})
        time.sleep(INTERVAL_SEC)
finally:
    client.loop_stop(); client.disconnect()
        

