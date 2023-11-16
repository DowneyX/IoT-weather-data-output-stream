from azure.iot.device import IoTHubDeviceClient
from azure.iot.device.exceptions import ConnectionFailedError, ConnectionDroppedError, OperationTimeout, OperationCancelled, NoConnectionError
from azure.iot.device import Message
import requests
import json
import config
import time

def send_data_to_iot_hub(device_client: IoTHubDeviceClient, message):
    telemetry = Message(json.dumps(message))
    telemetry.content_encoding = "utf-8"
    telemetry.content_type = "application/json"
    
    try:
        device_client.send_message(telemetry)
    except (ConnectionFailedError, ConnectionDroppedError, OperationTimeout, OperationCancelled, NoConnectionError):
        return False
    else:
        return True

def run():
    device_client = IoTHubDeviceClient.create_from_connection_string(config.IOT_HUB_CONNECTION_STRING, connection_retry=False)
    device_client.connect()
 
    while True:
        r = requests.get(url="http://downeypi.local:5000/api/weather_data/get/not-send")
        data = r.json()[0]
        print(data)
        is_send_to_IoT_hub = send_data_to_iot_hub(device_client, data)
        
        if is_send_to_IoT_hub:
            print("successfully send data to IoT-hub")
            r = requests.put(url=f"http://downeypi.local:5000/api/weather_data/put/is-send/{data['id']}")
        else:
            print("could not send data to IoT-hub")
        
        time.sleep(1)
run()





