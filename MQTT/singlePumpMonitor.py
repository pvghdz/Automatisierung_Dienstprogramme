import json
import time
import requests
import paho.mqtt.client as mqtt
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

"""
Code used to monitor and control a single device connected to a Shelly plug/switch.
It evaluates the power, current, and voltage of the shelly device and takes action according
to specified metrics.

This code was originally written to monitor a water pump (Förderpumpe) to keep them from 
running or stalling perpetually.
"""

#######################################
############ logging setup ############
#######################################
log_dir = Path("<Path>")
log_dir.mkdir(parents=True, exist_ok=True)

# Handler that writes to logs/pump.log and rotates the file at local midnight.
handler = TimedRotatingFileHandler(
    str(log_dir / "pump.log"),
    when="midnight",            # roll at local midnight
    interval=1,                 # Rotate every 1 "when" interval (i.e., every midnight).
    backupCount=30,             # keep last 30 days' worth of files
    encoding="utf-8",           # Write log files as UTF-8 text.
    delay=True                  # create file only on first write
)
#handler.suffix = "%Y-%m-%d"  # rotated files like pump.log.2025-10-31
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s")) # Define how each log line should look: timestamp, level, and message.

logger = logging.getLogger(__name__) # Get (or create) a named logger for your app/module.
logger.setLevel(logging.INFO) # Set the minimum severity level this logger will handle (INFO and above).
logger.addHandler(handler)  # Attach the rotating file handler to the logger so logs go to disk.
logger.propagate = False    # Prevent messages from bubbling up to the root logger (avoids duplicate logs if root has handlers).
    
###################################################
############ Grafana annotations setup ############
###################################################

def sendAnnotation(text, timeStamp):
    GRAFANA_URL = "http://<IP>:<Port>/api/annotations"
    GRAFANA_BEARER_TOKEN = "<Grafana Bearer Token>" 
    
    tags = [
        "tag1",
        "tag2",
        "tag3 ...",
        ]

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(GRAFANA_BEARER_TOKEN)
    }
            
    data = {    
        "dashboardUID": "<Grafana Dashboard ID>",
        "panelId":1,
        "timeStamp": timeStamp,
        "tags": tags,
        "text": text
    }

    try:
        r = requests.post(GRAFANA_URL, headers=headers, data=json.dumps(data), timeout=2.5) # fast timeout so that the 3s poll loop does not block on long network stalls
        r.raise_for_status() # treat 4xx/5xx like “not possible”, but don’t crash
        return {r.text}
    except requests.RequestException:
        return None # Skip silently

########################################
############ Shelly Monitor ############
########################################

class ShellyMonitor:
    """
    Monitor a Shelly via MQTT.
    - polls status
    - keeps latest metrics
    - detects "on for too long"
    - sends ntfy
    - turns device off
    """

    def __init__(
        self,
        broker: str,
        device_id: str,
        client_id: str = "<Client_ID>",
        poll_interval: int = 3,
        on_timeout_sec: int = 135,
        ntfy_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        on_threshold_w: float = 100.0,   # consider ON if apower > this
        off_threshold_w: float = 3.0,    # consider OFF if apower < this
    ):
        # define parameters
        self.broker = broker
        self.device_id = device_id              # e.g. "shellysw-extpump"
        self.client_id = client_id              # e.g. ""
        self.poll_interval = poll_interval      # how often to ask "Switch.GetStatus"
        self.on_timeout_sec = on_timeout_sec    # how long "ON" is allowed
        self.ntfy_url = ntfy_url                # e.g. "https://ntfy.sh/..."
        self.username = username
        self.password = password
        self.on_threshold_w = on_threshold_w
        self.off_threshold_w = off_threshold_w

        # --- runtime state ---

        # latest metrics we got from the shelly switch (filled by on_message)
        self.latest_metrics = {
            "output": None,    # True/False
            "apower": None,
            "current": None,
            "voltage": None,
        }

        # timestamp for when the pump is on. None if currently off / unknown
        # pump on = pulls around 700W from the switch.
        self.on_since = None

        # to avoid spamming alerts, remember if the program already sent one for this on-cycle
        # on-cycle is the interval between turning on the pump and turning it back off
        self.alert_sent_for_cycle = False

        self.timeout_triggered_for_cycle = False  # track that we already enforced a timeout in this cycle

        # create MQTT client
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id, protocol=mqtt.MQTTv5)
        # register callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # credentials if any
        if self.username:
            self.client.username_pw_set(self.username, self.password)

    # ------------- MQTT helper methods -------------

    def connect(self):
        """Connect to broker and start background loop."""
        self.client.connect(self.broker, 1883, 60)
        self.client.loop_start()

    def stop(self):
        """Stop background loop and disconnect cleanly."""
        self.client.loop_stop()
        self.client.disconnect()

    def on_connect(self, client, userdata, flags, rc, props=None):
        """
        Called BY PAHO after the TCP/MQTT connection succeeds.
        This function subscribes to the reply topic so we can see device responses.
        """
        # devices that were sent {"src": CLIENT_ID, ...} will answer to this topic
        topic = f"{self.client_id}/rpc"
        client.subscribe(topic)

    def on_message(self, client, userdata, msg):
        """
        Called BY PAHO whenever a message arrives on a subscribed topic.
        This function decodes the JSON and print the parts we care about.
        """
        # msg.payload is bytes -> turn into Python dict
        try:
            resp = json.loads(msg.payload.decode())
        except json.JSONDecodeError:                    # so that the program doesn't crash if an error is encountered. It tries again instead
            return
        r = resp.get("result")
        if not isinstance(r, dict):
            return

        # update metrics
        self.latest_metrics["output"] = r.get("output")
        self.latest_metrics["apower"] = r.get("apower")
        self.latest_metrics["current"] = r.get("current")
        self.latest_metrics["voltage"] = r.get("voltage")

        # detect ON/OFF transitions
        self._handle_onoff_transition()

    # ------------- RPC helpers -------------

    def _publish_rpc(self, method: str, params: dict | None = None):
        """
        RPC = Remote Procedure Call. It's a message that says "Hey device, please run this 
        method with these parameters, and send me back the result."
        Send an RPC using the already-connected, long-lived client.
        This function uses a timestamp as the request id so we can distinguish replies.
        """
        payload = {
            "id": int(time.time()),  # good-enough correlation id
            "src": self.client_id,   # tell Shelly to answer to "<client_id>/rpc"
            "method": method,
        }
        if params:
            payload["params"] = params
        self.client.publish(f"{self.device_id}/rpc", json.dumps(payload), qos=1)

    def poll_status(self):
        """
        Ask the device for its switch status (and optionally EM status).
        This just SENDS the request; the answer will arrive later in on_message.
        """
        self._publish_rpc("Switch.GetStatus", {"id": 0})
        # if you also want EM metrics, uncomment:
        # self._publish_rpc("EM.GetStatus", {"id": 0})

    def switch_off(self):
        """Ask Shelly to turn relay 0 off."""
        self._publish_rpc("Switch.Set", {"id": 0, "on": False})

    def switch_on(self):
        """Ask Shelly to turn relay 0 on."""
        self._publish_rpc("Switch.Set", {"id": 0, "on": True})

    # ------------- logic helpers -------------

    def _handle_onoff_transition(self):
        """
        Called after every message we get. Looks at self.latest_metrics["output"]
        and updates self.on_since / self.alert_sent_for_cycle.
        """
        power = self.latest_metrics["apower"]
        
        # at the very beginning (we don't know the state of the pump)
        # for the first few cycles, the program only receives "None" MQTT responses 
        if power is None:
            return
        
        now = time.time()

        # we only care when we know the state
        
        # pump is on = it is consuming more than 100W
        if float(power) >= self.on_threshold_w:
            if self.on_since is None:   # check if the pump just turned on (i.e., it was off before)
                self.on_since = now     # flag that stores when the pump turned on
                self.alert_sent_for_cycle = False  # new ON cycle
                self.timeout_triggered_for_cycle = False  # fresh cycle: no timeout yet
                
        # pump is off = it is consuming less than 3W
        elif float(power) < self.off_threshold_w:
            # pump turned off
            self.on_since = None # reset flag that indicates if the pump is on
            #self.alert_sent_for_cycle = False # this prevents sending the Grafana tag for the lights being turned off.
            self.timeout_triggered_for_cycle = False  # reset on off
        # else: still None → we don’t know yet

    def _maybe_trigger_alarm(self):
        """
        Check if the device has been ON for too long.
        If yes, send ntfy (once) and turn it off.
        """
        if self.on_since is None:
            return  # currently off / unknown
        
        # If it is the first time it detects that the pump is on
        now = time.time()
        on_duration = now - self.on_since
        
        # If the pump has been on for too long
        if on_duration >= self.on_timeout_sec and not self.timeout_triggered_for_cycle:  # <<< fire once per cycle
            # figures of merit
            apower = self.latest_metrics["apower"]
            current = self.latest_metrics["current"]
            voltage = self.latest_metrics["voltage"]
            
            # send notification
            timeStamp = time.strftime('%a, %d %b %Y at %H:%M:%S', time.localtime())
            text = f"{timeStamp} Shelly {self.device_id} has been running for {int(on_duration)}s with {apower} W, {current} A, {voltage} V. Turning it off. Human intervention required to check what happened."
            self._send_ntfy(text) # send notification
            logger.warning(text) # Add to log
            sendAnnotation(text, timeStamp)
            #print(text)
            # turn off
            self.switch_off()
            # reset flag to send alerts of when the pump has been turned on for the first time
            self.alert_sent_for_cycle = False
            self.timeout_triggered_for_cycle = True   # remember we already forced off this cycle

    def _send_ntfy(self, message: str):
        """
        Send ntfy message
        """
        if not self.ntfy_url:                     # guard if URL not provided
            print(message)
            logger.info(message)
            return
        try:                                      # avoid crashing on network errors
            requests.post(self.ntfy_url, data=message.encode(), timeout=35)
        except Exception:
            pass

    # ------------- main loop -------------

    def run(self):
        """
        Main loop:
        - poll status every poll_interval
        - after each poll, check timers/alarms
        """
        try:
            print(f"Polling '{self.device_id}' every {self.poll_interval} seconds. \n")
            while True:
                # ask Shelly for current status
                self.poll_status()

                # check if it's been on for too long
                self._maybe_trigger_alarm()

                # figures of merit
                apower = self.latest_metrics["apower"]
                current = self.latest_metrics["current"]
                voltage = self.latest_metrics["voltage"]

                """
                # 3) show what we currently know (debug)
                on_since_str = "-" if self.on_since is None else time.strftime('%a, %d %b %Y at %H:%M:%S', time.localtime(self.on_since))  # convert epoch and guard against None values
        
                print(
                    "output:", self.latest_metrics["output"], "\n",
                    "apower:", self.latest_metrics["apower"], "\n",
                    "current:", self.latest_metrics["current"], "\n",
                    "voltage:", self.latest_metrics["voltage"], "\n",
                    "on_since:", on_since_str, "\n",
                )
                """
                
                # --- notifications based on latest apower ---
                ap = self.latest_metrics["apower"]            # cache once
                if ap is not None:                            # guard early messages
                    # pump turned on and flag = False -> Pump just turned on
                    if float(ap) >= self.on_threshold_w and self.alert_sent_for_cycle is False and not self.timeout_triggered_for_cycle:
                        timeStamp = time.strftime('%a, %d %b %Y at %H:%M:%S', time.localtime())
                        text = f"{timeStamp} Shelly {self.device_id} is now running with {apower} W, {current} A, {voltage} V."
                        sendAnnotation(text, timeStamp)
                        logger.info(text) # Add to log
                        #print(text)
                        self.alert_sent_for_cycle = True
                    
                    # pump turned off and flag = True -> Pump just turned off
                    # (but skip this if we already forced it off this cycle to avoid duplicate)
                    if float(ap) < self.off_threshold_w and self.alert_sent_for_cycle is True and not self.timeout_triggered_for_cycle:
                        timeStamp = time.strftime('%a, %d %b %Y at %H:%M:%S', time.localtime())
                        text = f"{timeStamp} Shelly {self.device_id} stopped running: {apower} W, {current} A, {voltage} V."
                        sendAnnotation(text, timeStamp)
                        logger.info(text) # Add to log
                        #print(f"{time.strftime('%a, %d %b %Y at %H:%M:%S', time.localtime())} External pump is now off.")
                        self.alert_sent_for_cycle = False

                time.sleep(self.poll_interval)
        finally:
            self.stop()

if __name__ == "__main__":
    monitor = ShellyMonitor(
        broker="<IP>",
        device_id="<Device_ID>",
        client_id="<Client_ID>",             # MQTT client_id
        poll_interval=3,
        on_timeout_sec=135,              # 135s (=2 min 15s) limit
        ntfy_url="https://ntfy.sh/url<>", 
        #ntfy_url="https://ntfy.sh/<url>", # for testing
        username=None,
        password=None,
        on_threshold_w = 100.0,   # consider ON if apower > this
        off_threshold_w =  3.0,   # consider OFF if apower < this
    )

    monitor.connect()
    monitor.run()
