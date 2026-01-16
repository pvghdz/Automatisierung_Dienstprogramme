import paho.mqtt.publish as publish

publish.single("test/topic", "Remote test using python", hostname="192.168.0.154")
