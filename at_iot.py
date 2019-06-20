from paho.mqtt import client as mqtt
import ssl
import random
from time import sleep
import pymongo as pymongo
from datetime import datetime

config = {
    'URI': 'mongodb://blocoiot-db:dsbh5MII4bl4vkDwAcTSLmKpNJv79HW4a4D71ibQtBuqTLowQZtc0MtlYCPNhRjW9VBaBh7ZFan7VIKiKjq1jQ=='
           '@blocoiot-db.documents.azure.com:10255/?ssl=true&replicaSet=globaldb',
    'DATABASE': 'at-iot',
    'CONTAINER': 'sensor'
}

path_to_root_cert = "cert.cer"
device_id = "sensorlocal"
sas_token = "SharedAccessSignature sr=SensorBlocoIoT.azure-devices.net%2Fdevices%2Fsensorlocal&sig=wDL7felH69ztd3DhMxbuwXs" \
            "nFaB9kCBqPEgRcfjsuGA%3D&se=1561719426"
iot_hub_name = "SensorBlocoIoT"
topic = "pedropaiva/producao/mensagem"
message = "{timestamp: %s," \
          " id_sensor: %s," \
          " info: %.2f" \
          "}"

TEMPERATURE = 20.0
INTERVAL = 5


def on_connect(client, userdata, flags, rc):
    print("Device connected with result code: " + str(rc))


def error_str(rc):
    return "Some error occurred. {}: {}".format(rc, mqtt.error_string(rc))


def on_disconnect(unused_client, unused_userdata, rc):
    print("on_disconnect", error_str(rc))


def on_publish(client, userdata, mid):
    print("Device sent message")


def on_message(client, userdata, msg):
    print("Message received at: " + msg.topic + " with payload: " + str(msg.payload))


def client_cloud_mqtt(msg_txt_formatted):
    broker = "postman.cloudmqtt.com"
    port = 18853
    usuario = 'pfylzqnn'
    senha = 'DrH6L-HlwS0J'

    cliente_mqtt = mqtt.Client('python-mqtt')
    cliente_mqtt.username_pw_set(username=usuario, password=senha)
    cliente_mqtt.connect(broker, port)
    cliente_mqtt.loop_start()

    cliente_mqtt.publish(topic, msg_txt_formatted, qos=1, retain=True)
    print(msg_txt_formatted)


def insert_db(timestamp, id_sensor, info):
    mongo_client = pymongo.MongoClient(config['URI'])
    db = mongo_client[config['DATABASE']]
    collection = db[config['CONTAINER']]
    send_object = {"timestamp": timestamp, "id_sensor": id_sensor, "info": info}
    x = collection.insert_one(send_object)
    print("Sent to database/container the object: ", send_object)


def send_iot(client, sensor):
    timestamp = datetime.now().timestamp()
    id_sensor = sensor
    info = TEMPERATURE + (random.random() * 15)
    msg_txt_formatted = message % (timestamp, id_sensor, info)
    msg_full = topic + "/" + msg_txt_formatted

    client.publish("devices/" + device_id + "/messages/events/", msg_full, qos=1)
    client_cloud_mqtt(msg_txt_formatted)
    insert_db(timestamp, id_sensor, info)
    sleep(INTERVAL)


def client_mqtt_iot(num):
    for n in range(0, num):
        client = mqtt.Client(client_id=device_id, protocol=mqtt.MQTTv311)
        client.username_pw_set(username=iot_hub_name + ".azure-devices.net/" + device_id, password=sas_token)
        client.tls_set(ca_certs=path_to_root_cert, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
                       tls_version=ssl.PROTOCOL_TLSv1, ciphers=None)
        client.tls_insecure_set(False)
        client.connect(iot_hub_name + ".azure-devices.net", port=8883)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_publish = on_publish
        client.on_message = on_message

        sensor = "sensor_local"
        send_iot(client, sensor)


if __name__ == '__main__':
    print("AT - Internet of Things")
    number = 0

    try:
        number = int(input("Digite o número de vezes que deseja ver a temperatura (limite 10): "))
    except Exception as ex:
        print(ex)
        print("Digite somente números inteiros")

    if number <= 10:
        client_mqtt_iot(number)
    else:
        print("Número não pode ser maior que 10.")
