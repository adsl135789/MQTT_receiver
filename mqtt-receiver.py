# pip install paho-mqtt
# python mqtt-receiver.py -i 118.163.66.205 -p 1883 -d 60c5a8fffe789a05 -d 60c5a8fffe789a06


import decimal
import getopt
import importlib
import json
import sys
import time
import os
import paho.mqtt.client as mqtt

importlib.reload(sys)
stdi, stdo, stde = sys.stdin, sys.stdout, sys.stderr
sys.stdin, sys.stdout, sys.stderr = stdi, stdo, stde

deveuiList = []
outputfileList = []
deveui = ''
outputfile = ''
HOST = ""
PORT = 1883
is_connected = False


def jsonToDict(msg):
    jsonString = ''
    tab_num = 0
    for i in range(0, len(msg)):
        if msg[i] == '{' or msg[i] == '[':
            jsonString += msg[i]
            tab_num += 1
            jsonString += "\n"
            jsonString += tab_num * '\t'

        elif msg[i] == '}' or msg[i] == ']':
            tab_num -= 1
            jsonString += "\n"
            jsonString += tab_num * '\t'
            jsonString += msg[i]
        elif msg[i] == ",":
            jsonString += msg[i]
            jsonString += "\n"
            jsonString += tab_num * '\t'
        else:
            jsonString += msg[i]

    dic = json.loads(jsonString)
    return dic


def get_results(dic, devEUI):
    data = dic["data"]
    # Byte 1 - 8 NodeID
    nodeID = data[0:16]
    bytes_nodeID = bytes.fromhex(nodeID);
    ascii_nodeID = bytes_nodeID.decode("ASCII")

    # Byte 9 Node Type
    nodeType = data[16:18]
    int_nodeType = int(nodeType, 16)
    str_nodeType = str(decimal.Decimal(int_nodeType))

    # Byte 10 battery voltage
    batteryVolt = data[18:20]
    float_batteryVolt = int(batteryVolt, 16) / 10
    str_batteryVolt = str(float_batteryVolt)

    # Byte 11 temperature
    temp = data[20:22]
    int_temp = int(temp, 16) - 50
    str_temp = str(decimal.Decimal(int_temp))

    # Byte 12 sign1
    sign1 = data[22:24]
    int_sign1 = int(sign1, 16)

    # Byte 13 - 16 value1
    value1 = data[28:32]
    float_value1 = int(value1, 16) / 10000
    if int_sign1 == 0:
        str_value1 = '-' + str(float_value1)[0:6]
    elif int_sign1 == 1:
        str_value1 = str(float_value1)[0:6]

    # Byte 17 sign2
    sign2 = data[32:34]
    int_sign2 = int(sign2, 16)

    # Byte 18 - 21 value2
    value2 = data[38:42]
    float_value2 = int(value2, 16) / 10000
    if int_sign2 == 0:
        str_value2 = '-' + str(float_value2)[0:6]
    elif int_sign2 == 1:
        str_value2 = str(float_value2)[0:6]

    # Byte 22 data serial no
    serialNo = data[42:44]
    int_serialNo = int(serialNo, 16)
    str_serialNo = str(decimal.Decimal(int_serialNo))

    # Byte 23 checksum
    check = int(data[0:2], 16)
    for k in range(2, len(data) - 2, 2):
        check = check ^ int(data[k:k + 2], 16)

    CTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    inf = CTime + "," + ascii_nodeID + "," + str_nodeType + "," + str_nodeType + "," \
          + str_batteryVolt + "," + str_temp + "," + str_value1 + "," + str_value2 + "," + str_serialNo

    if check == int(data[44:46], 16):
        res = open(devEUI + "_results.txt", "a")
        res.write(inf + "\n")
        res.close()
    else:
        print("Data Error\n")
        res = open(devEUI + "_results.txt", "a")
        res.write("Data %d Error\n" % int_serialNo)
        res.close()


def client_loop(HOST, PORT):
    client_id = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    client = mqtt.Client(client_id)
    # client.username_pw_set("loraroot", "62374838")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(HOST, PORT, 60)
    client.loop_forever()


def on_connect(client, userdata, flags, rc):
    global is_connected
    print("Connected with result code " + str(rc))

    if not is_connected:
        for deveui in deveuiList:
            outputfileList.append(deveui + '_data.txt')
            outputfile = deveui + '_data.txt'
            fp = open(outputfile, "a")
            fp.write("Connect to MQTT server {}\n".format(HOST))
            sub_topic = "application/1/device/" + deveui + "/#"
            # sub_topic="application/1/device/#"
            print("mqtt subscribe topic : {}".format(sub_topic))
            fp = open(outputfile, "a")
            fp.write("mqtt subscribe topic : {}\n".format(sub_topic))
            fp.close()
            client.subscribe(sub_topic)
        is_connected = True
    else:
        for deveui in deveuiList:
            sub_topic = "application/1/device/" + deveui + "/#"
            client.subscribe(sub_topic)


def on_message(client, userdata, msg):
    currentTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    msg_dict = jsonToDict(msg.payload.decode("utf-8"))
    print(currentTime)
    print(msg.topic + ": " + msg.payload.decode("utf-8"))
    print('-----------------------------------------------')
    for file in outputfileList:
        if msg_dict["devEUI"] in file:
            break
    get_results(msg_dict, msg_dict["devEUI"])
    fp = open(file, "a")
    fp.write(currentTime + ' ')
    fp.write(msg.topic + ": " + msg.payload.decode("utf-8"))
    fp.write('\n-----------------------------------------------\n')
    fp.close()


def main(argv):
    global HOST, PORT
    try:
        opts, args = getopt.getopt(argv, "hd:f:i:p:", ["help", "deveui=", "file=", "ip=", "port="])
    except getopt.GetoptError:
        print('mqtt-receiver.py -i <host IP> -p <port> -d <deveui> -f <device file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print('mqtt-receiver.py -i <host IP> -p <port> -d <deveui> -f <device file>')
            sys.exit()
        elif opt in ("-i", "--ip"):
            HOST = arg
        elif opt in ("-p", "--port"):
            PORT = int(arg)
        elif opt in ("-d", "--deveui"):
            deveuiList.append(arg)
        elif opt in ("-f", "--file"):           # read all devices in the file
            with open(arg, "r") as deveui_file:
                for line in deveui_file.readlines():
                    deveuiList.append(line.rstrip("\n"))

    print("HOST : {0} \nPORT : {1}".format(HOST, PORT))
    for dev in deveuiList:
        print("device : {0}".format(dev))
    print()
    client_loop(HOST, PORT)


if __name__ == '__main__':
    main(sys.argv[1:])
