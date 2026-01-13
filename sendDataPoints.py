import influxdb_client
from influxdb_client.client.write_api import ASYNCHRONOUS

import influxconfig

"""
This program asynchronously sends information to a specific bucket in influxDB for storage or future display

How to use it:
from sendDataPoints import writeDatapoints

e.g.
datapoints = []
datapoints.append( Point("Temperature-from-a-sensor").tag("Sensor-type", "Sensor-name").field("Temperature", round(temperature, 1)) )
writeDatapoints("Bucket", datapoints)
"""

org = "InfluxDB org"
url = "http://<IP>:<Port>"

write_client = influxdb_client.InfluxDBClient(url=url, token=influxconfig.token, org=org)

def writeDatapoints(bucket, points):

    bucket= bucket
    write_api = write_client.write_api(write_options=ASYNCHRONOUS)
    write_api.write(bucket=bucket, org=org, record=points)