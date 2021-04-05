# -*-coding:utf-8 -*-
'''
@file    :   client.py
@time    :   2021/03/07 21:40:01
@author  :   Fu Ming Chen
@version :   1.0
@contact :   wechat18613838275
@license :   GPL-3.0 License
@link    :   https://github.com/miniBamboo/py_dmqtt
'''

import socket
import threading
import sys
import encoders, decoders, definition, time

subscribes = []
tmpSubscribes = []
tmpUnsubscribes = []
tmpPublishAtQos1 = []
tmpPublishAtQos2 = []
tmpReceiveAtQos2 = []
messageReceived = []

SOCKET_DISCONNECTED = 0
SOCKET_CONNECTED = 1
MQTT_CONNECTED = 2

alive = SOCKET_DISCONNECTED
sessionPresent = 0
packetIdentifier = 0
socketLock = threading.Lock()

def generatePacketIdentifier():
    """
        Description:
            生成短期内唯一的报文标识符。
        Args:
            None
        Returns:
            生成的报文标识符。
        Raises:
            None
    """
    global packetIdentifier
    if packetIdentifier < 511:
        packetIdentifier = packetIdentifier+1
    else:
        packetIdentifier = 1
    return packetIdentifier

def getTime():
    """
        Description:
            获取用以在控制台进行输出的当前时间字符串。
        Args:
            None
        Returns:
            string
                当前时间字符串。
        Raises:
            None
    """
    return time.strftime("%H:%M:%S", time.localtime()) 

def receive(socket):
    """
        Description:
            接收消息的主方法，基于剩余长度实现帧定界，并传递给解码方法。
        Args:
            socket(Socket):
                客户端socket对象。
        Returns:
            None
        Raises:
            None
    """
    global alive
    while alive!=SOCKET_DISCONNECTED:
        try:
            oneMessage = b''
            data = socket.recv(2)
            if data != b'':
                oneMessage += data
                remainedLengthBytes = b''
                remainedLengthBytes += int.to_bytes(data[1],1,'big')
                while data !=b'' and (data[data.__len__()-1] & 128 )!=0:
                    data = socket.recv(1)
                    oneMessage += data
                    remainedLengthBytes += data
                data = socket.recv(decoders.remainingBytes_Decoder(remainedLengthBytes,True)[1])
                oneMessage += data
            if oneMessage != b'':
                decode(oneMessage,socket)
        except Exception as e:
            print("["+getTime()+"]"+" [CLIENT/INFO] Socket Disconnected: Connection closed by server.")
            alive = SOCKET_DISCONNECTED
            break

def startClient(serverHost, serverPort, userNameFlag, passwordFlag, willRetain, willQos, willFlag, cleanSession, keepAlive, clientId, willTopic='', willMessage='', userName='', password=''):
    """
        Description:
            启动客户端。
        Args:
            serverHost(string)
                服务器地址。
            serverPort(int)
                服务器端口。
            userNameFlag(int)
                用户名标志，1为存在用户名字段，0为不存在。
            passwordFlag(int)
                密码标志，1为存在密码字段，0为不存在。
            willRetain(int)
                遗嘱保留（willFlag = 0时必须设置为0），1为遗嘱保留，0为遗嘱不保留。
            willQos(int)
                遗嘱消息服务质量（willFlag = 0时必须设置为0）。
            willFlag(int)
                遗嘱标志。
            cleanSession(int)
                清理会话标志，1为清除历史订阅信息，0为不清除。
            keepAlive(int)
                保活时长，单位为s。
            clientId(int)
                ClientID。
            willTopic(string)
                遗嘱消息主题（仅willFlag = 1时可设置）。
            willMessage(string)
                遗嘱消息内容（仅willFlag = 1时可设置）。
            userName(string)
                用户名（仅userNameFlag = 1时可设置）。
            password(string)
                密码（仅password = 1时可设置）。
        Returns:
            Socket:
                客户端socket对象。
        Raises:
            IOException
                socket连接错误。
    """
    print("====Decentralized MQTT-Client v0.1====")
    global alive
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(True)
        print("["+getTime()+"]"+" [CLIENT/INFO] Socket Connecting...")
        sock.connect((serverHost, serverPort))
    except:
        print("["+getTime()+"]"+" [CLIENT/INFO] Socket Disconnected: Can not connect to server.")
        alive = SOCKET_DISCONNECTED
        input("Press ENTER to continue...")
        sys.exit(0)
    alive = SOCKET_CONNECTED
    print("["+getTime()+"]"+" [CLIENT/INFO] Socket Connected.")
    receiveThread = threading.Thread(target = receive, args = (sock,))
    receiveThread.start()
    sendMessage(sock, encoders.CONNECT_Encoder(userNameFlag, passwordFlag, willRetain, willQos, willFlag, cleanSession, keepAlive, clientId, willTopic, willMessage, userName, password))
    print("["+getTime()+"]"+" [CLIENT/INFO] MQTT Connecting...")
    return sock

def sendMessage(sock, message):
    """
        Description:
            在给定的socket连接中发送消息。
        Args:
            sock(Socket):
                客户端socket对象。
            message(byte[]):
                将要发送的字节串。
        Returns:
            None
        Raises:
            IOException
                socket连接错误。
    """
    socketLock.acquire()
    sock.sendall(message)
    socketLock.release()

def removeSubscribe(topic):
    """
        Description:
            从本地存储中移除特定的订阅主题（非向服务器请求取消订阅）。
        Args:
            topic(string)
                移除的主题名。
        Returns:
            None
        Raises:
            None
    """
    for k in range(0,subscribes.__len__()):
        if subscribes[k]['topic'] == topic:
            subscribes.pop(k)
            break

def subscribe(sock, topics):
    """
        Description:
            订阅一组主题消息。
        Args:
            sock(Socket):
                客户端socket对象。
            topics(array[dict]):
                将要订阅的主题数组，数组单元为主题字典，必须包含topic和qos两个字段。
        Returns:
            packetIdentifier(int)
                本次请求使用的报文标识符。
        Raises:
            IOException
                socket连接异常。
    """
    packetIdentifier = generatePacketIdentifier()
    tmpSubscribes.append({'packetIdentifier':packetIdentifier,'topics':topics})
    sendMessage(sock, encoders.SUBSCRIBE_Encoder(packetIdentifier,topics))
    print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": Subscribing...")
    return packetIdentifier

def unsubscribe(sock, topics):
    """
        Description:
            取消订阅一组主题消息。
        Args:
            sock(Socket):
                客户端socket对象。
            topics(array[string]):
                将要取消订阅的主题数组，数组单元为主题字符串。
        Returns:
            packetIdentifier(int)
                本次请求使用的报文标识符。
        Raises:
            IOException
                socket连接异常。
    """
    packetIdentifier = generatePacketIdentifier()
    tmpUnsubscribes.append({'packetIdentifier':packetIdentifier,'topics':topics})
    sendMessage(sock, encoders.UNSUBSCRIBE_Encoder(packetIdentifier,topics))
    print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": Unsubscribing...")
    return packetIdentifier

def disconnect(sock):
    """
        Description:
            主动断开与服务端的连接。
        Args:
            sock(Socket):
                客户端socket对象。
        Returns:
            None
        Raises:
            IOException
                socket连接异常。
    """
    print("["+getTime()+"]"+" [CLIENT/INFO] Sending DISCONNECT message...")
    sendMessage(sock, encoders.DISCONNECT_Encoder())

def ping(sock):
    """
        Description:
            向服务器发送PING请求。
        Args:
            sock(Socket):
                客户端socket对象。
        Returns:
            None
        Raises:
            IOException
                socket连接异常。
    """
    print("["+getTime()+"]"+" [CLIENT/INFO] Sending heartbeat...")
    sendMessage(sock, encoders.PINGREQ_Encoder())
    print("["+getTime()+"]"+" [CLIENT/INFO] Heartbeat sent.")

def publishAtQos0(sock, topic, message, retain):
    """
        Description:
            发送一则服务质量等级为0的消息。
        Args:
            sock(Socket):
                客户端socket对象。
            topic(string):
                消息主题。
            message(string):
                消息内容。
            retain(int):
                消息保留标志。
        Returns:
            None
        Raises:
            IOException
                socket连接异常。
    """
    sendMessage(sock, encoders.PUBLISH_Encoder(0, 0, retain, topic, 0, message))
    print("["+getTime()+"]"+" [CLIENT/INFO] Message published at Qos0" + ".")

def publishAtQos1(sock, topic, message, retain):
    """
        Description:
            发送一则服务质量等级为1的消息。
        Args:
            sock(Socket):
                客户端socket对象。
            topic(string):
                消息主题。
            message(string):
                消息内容。
            retain(int):
                消息保留标志。
        Returns:
            packetIdentifier(int)
                本次请求使用的报文标识符。
        Raises:
            IOException
                socket连接异常。
    """
    packetIdentifier = generatePacketIdentifier()
    sendMessage(sock, encoders.PUBLISH_Encoder(0,1,retain,topic,packetIdentifier,message))
    tmpPublishAtQos1.append({
        "packetIdentifier":packetIdentifier,
        "waitingFor":'PUBACK'
    })
    return packetIdentifier

def publishAtQos2(sock, topic, message, retain):
    """
        Description:
            发送一则服务质量等级为2的消息。
        Args:
            sock(Socket):
                客户端socket对象。
            topic(string):
                消息主题。
            message(string):
                消息内容。
            retain(int):
                消息保留标志。
        Returns:
            packetIdentifier(int)
                本次请求使用的报文标识符。
        Raises:
            IOException
                socket连接异常。
    """
    packetIdentifier = generatePacketIdentifier()
    sendMessage(sock, encoders.PUBLISH_Encoder(0,2,retain,topic,packetIdentifier,message))
    tmpPublishAtQos2.append({
        "packetIdentifier":packetIdentifier,
        "waitingFor":'PUBREC'
    })
    return packetIdentifier

def decode(data,sock):
    """
        Description:
            解码报文，并做出对应的逻辑响应。
        Args:
            data(byte[]):
                待解码的报文字符串。
            sock(Socket):
                客户端socket对象。
        Returns:
            None
        Raises:
            IOException
                socket连接异常。
            decoders.IllegalMessageException
                报文解码异常。
    """
    global alive, sessionPresent,subscribes
    try:
        messageType, results = decoders.message_Decoder(data)
        if messageType == definition.messageType.CONNACK:
            if results['sessionPresent'] == 0:
                subscribes = []
            if results['returnCode'] == definition.ConnackReturnCode.ACCEPTED:
                print("["+getTime()+"]"+" [CLIENT/INFO] MQTT Connected!")
            elif results['returnCode'] == definition.ConnackReturnCode.REFUSED_ILLEGAL_CLIENTID:
                print("["+getTime()+"]"+" [CLIENT/INFO] MQTT Disconnected: Illegal ClientID.")
                alive = SOCKET_DISCONNECTED
                sock.close()
            elif results['returnCode'] == definition.ConnackReturnCode.REFUSED_INVALID_USER:
                print("["+getTime()+"]"+" [CLIENT/INFO] MQTT Disconnected: Invalid User.")
                alive = SOCKET_DISCONNECTED
                sock.close()
            elif results['returnCode'] == definition.ConnackReturnCode.REFUSED_SERVER_UNAVAILABLE:
                print("["+getTime()+"]"+" [CLIENT/INFO] MQTT Disconnected: Server temporarily unavailable.")
                alive = SOCKET_DISCONNECTED
                sock.close()
            elif results['returnCode'] == definition.ConnackReturnCode.REFUSED_UNAUTHORIZED:
                print("["+getTime()+"]"+" [CLIENT/INFO] MQTT Disconnected: Unauthorized.")
                alive = SOCKET_DISCONNECTED
                sock.close()
            elif results['returnCode'] == definition.ConnackReturnCode.REFUSED_UNSUPPORTED_PROTOCOL:
                print("["+getTime()+"]"+" [CLIENT/INFO] MQTT Disconnected: Unsupported protocol.")
                alive = SOCKET_DISCONNECTED
                sock.close()
        elif messageType == definition.messageType.SUBACK:
            packetIdentifier = results['packetIdentifier']
            for i in range(0,tmpSubscribes.__len__()):
                if tmpSubscribes[i]['packetIdentifier'] == packetIdentifier:
                    for j in range(0,tmpSubscribes[i]['topics'].__len__()):
                        if results['returnCodes'][j] == definition.SubackReturnCode.FAILURE:
                            print("["+getTime()+"]"+" [CLIENT/WARN] Subscribe failure at topic "+tmpSubscribes[i]['topics'][j]['topic']+".")
                        elif results['returnCodes'][j] == definition.SubackReturnCode.SUCCESS_MAX_QOS_0:
                            print("["+getTime()+"]"+" [CLIENT/INFO] Subscribe success at topic "+tmpSubscribes[i]['topics'][j]['topic']+", Qos 0 .")
                            removeSubscribe(tmpSubscribes[i]['topics'][j]['topic'])
                            subscribes.append({'topic':tmpSubscribes[i]['topics'][j]['topic'],'qos':0})
                        elif results['returnCodes'][j] == definition.SubackReturnCode.SUCCESS_MAX_QOS_1:
                            print("["+getTime()+"]"+" [CLIENT/INFO] Subscribe success at topic "+tmpSubscribes[i]['topics'][j]['topic']+", Qos 1 .")
                            removeSubscribe(tmpSubscribes[i]['topics'][j]['topic'])
                            subscribes.append({'topic':tmpSubscribes[i]['topics'][j]['topic'],'qos':1})
                        elif results['returnCodes'][j] == definition.SubackReturnCode.SUCCESS_MAX_QOS_2:
                            print("["+getTime()+"]"+" [CLIENT/INFO] Subscribe success at topic "+tmpSubscribes[i]['topics'][j]['topic']+", Qos 2 .")
                            removeSubscribe(tmpSubscribes[i]['topics'][j]['topic'])
                            subscribes.append({'topic':tmpSubscribes[i]['topics'][j]['topic'],'qos':2})
                    tmpSubscribes.pop(i)
                    break
            print("["+getTime()+"]"+" [CLIENT/INFO] Current subscribes: "+str(subscribes)+" .")
        elif messageType == definition.messageType.UNSUBACK:
            packetIdentifier = results['packetIdentifier']
            for i in range(0,tmpUnsubscribes.__len__()):
                if tmpUnsubscribes[i]['packetIdentifier'] == packetIdentifier:
                    for j in range(0,tmpUnsubscribes[i]['topics'].__len__()):
                        removeSubscribe(tmpUnsubscribes[i]['topics'][j])
                    tmpUnsubscribes.pop(i)
                    print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": Unsubscribe success.")
                    break
            print("["+getTime()+"]"+" [CLIENT/INFO] Current subscribes: "+str(subscribes)+" .")
        elif messageType == definition.messageType.PUBACK:
            packetIdentifier = results['packetIdentifier']
            print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": PUBACK received.")
            for i in range (0,tmpPublishAtQos1.__len__()):
                if tmpPublishAtQos1[i]['packetIdentifier'] == packetIdentifier and tmpPublishAtQos1[i]['waitingFor'] == 'PUBACK':
                    tmpPublishAtQos1.pop(i)
                    break
        elif messageType == definition.messageType.PUBREC:
            packetIdentifier = results['packetIdentifier']
            print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": PUBREC received.")
            sendMessage(sock,encoders.PUBREL_Encoder(packetIdentifier))
            print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": PUBREL sent.")
            for i in range (0,tmpPublishAtQos2.__len__()):
                if tmpPublishAtQos2[i]['packetIdentifier'] == packetIdentifier and tmpPublishAtQos2[i]['waitingFor'] == 'PUBREC':
                    tmpPublishAtQos2[i]['waitingFor'] = 'PUBCOMP'
                    break
        elif messageType == definition.messageType.PUBCOMP:
            packetIdentifier = results['packetIdentifier']
            print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": PUBCOMP received.")
            for i in range (0,tmpPublishAtQos2.__len__()):
                if tmpPublishAtQos2[i]['packetIdentifier'] == packetIdentifier and tmpPublishAtQos2[i]['waitingFor'] == 'PUBCOMP':
                    tmpPublishAtQos2.pop(i)
                    break
        elif messageType == definition.messageType.PUBLISH:
            qos = results['qos']
            topic = results['topic']
            message = results['message']
            packetIdentifier = results['packetIdentifier']
            if qos == 0:
                print("["+getTime()+"]"+" [CLIENT/INFO] A message received: topic - "+topic+" , message - "+message+" .")
                messageReceived.append({
                    'topic': topic,
                    'message': message
                })
            elif qos == 1:
                print("["+getTime()+"]"+" [CLIENT/INFO] A message received: topic - "+topic+" , message - "+message+" .")
                messageReceived.append({
                    'topic': topic,
                    'message': message
                })
                sendMessage(sock, encoders.PUBACK_Encoder(packetIdentifier))
                print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": PUBACK sent.")
            elif qos == 2:
                print("["+getTime()+"]"+" [CLIENT/INFO] A message received but not confirmed: topic - "+topic+" , message - "+message+" .")
                tmpReceiveAtQos2.append({
                    'packetIdentifier': packetIdentifier,
                    'topic': topic,
                    'message': message
                })
                sendMessage(sock, encoders.PUBREC_Encoder(packetIdentifier))
                print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": PUBREC sent.")
        elif messageType == definition.messageType.PUBREL:
            packetIdentifier = results['packetIdentifier']
            print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": PUBREL received.")
            for i in range(0,tmpReceiveAtQos2.__len__()):
                if tmpReceiveAtQos2[i]['packetIdentifier'] == packetIdentifier:
                    res = tmpReceiveAtQos2.pop(i)
                    messageReceived.append({
                        'topic': res['topic'],
                        'message': res['message']
                    })
                    print("["+getTime()+"]"+" [CLIENT/INFO] A message received and confirmed: topic - "+res['topic']+" , message - "+res['message']+" .")
                    sendMessage(sock, encoders.PUBCOMP_Encoder(packetIdentifier))
                    print("["+getTime()+"]"+" [CLIENT/INFO] Packet "+str(packetIdentifier)+": PUBCOMP sent.")
                    break
        elif messageType == definition.messageType.PINGRESP:
            print("["+getTime()+"]"+" [CLIENT/INFO] Heartbeat response received.")
    except decoders.IllegalMessageException as e:
        print(data)
        print("["+getTime()+"]"+" [CLIENT/INFO] MQTT Disconnected: Illegal message received.")
        sock.close()

def checkPackageStatus(packetIdenifier):
    """
        Description:
            检查报文标识符对应的报文是否已按对应的服务质量等级完成交互。
        Args:
            packetIdentifier(int)
                待检查报文的报文标识符。
        Returns:
            boolean
                True - 已完成交互；False - 尚未完成交互。
        Raises:
            None
    """
    global tmpSubscribes, tmpUnsubscribes, tmpPublishAtQos1, tmpPublishAtQos2
    for m in tmpSubscribes:
        if m['packetIdentifier'] == packetIdenifier:
            return False
    for m in tmpUnsubscribes:
        if m['packetIdentifier'] == packetIdenifier:
            return False
    for m in tmpPublishAtQos1:
        if m['packetIdentifier'] == packetIdenifier:
            return False
    for m in tmpPublishAtQos2:
        if m['packetIdentifier'] == packetIdenifier:
            return False
    return True



global HOST, MQTT_PORT, BOOTSTRAP_PORT, CONFIG_FILE_NAME, BOOTSTRAP_CONFIG

BOOTSTRAP_CONFIG = 'bootstrap.json'
CONFIG_FILE_NAME = 'config.json'
HOST = ' localhost'
#DHT_PORT = 2961
MQTT_PORT = 2962
BOOTSTRAP_PORT = MQTT_PORT+1

def loadBootStrap():
    global BOOTSTRAP_CONFIG
    import os,json
    if os.path.exists(BOOTSTRAP_CONFIG):
        file = open(BOOTSTRAP_CONFIG, 'r', encoding="utf-8")
        bootstrap_json = json.load(file)
        file.close()
        return bootstrap_json
    else:
        return []


def saveBootStrap(bootstrap_json):
    global BOOTSTRAP_CONFIG
    import os, json
    file = open(BOOTSTRAP_CONFIG, 'w', encoding="utf-8")
    json.dump(bootstrap_json, file)
    file.close()
#only config MQTT Server port, add 1 to MQTT Server port for getting bootstrap server port
def loadConfig():
    global HOST, MQTT_PORT, BOOTSTRAP_PORT, CONFIG_FILE_NAME
    import os,json
    if os.path.exists(CONFIG_FILE_NAME):
        file = open(CONFIG_FILE_NAME, 'r', encoding="utf-8")
        config_json = json.load(file)
        file.close()
        HOST = config_json['HOST']
        MQTT_PORT  = config_json['PORT']
        BOOTSTRAP_PORT = MQTT_PORT+1
    else:
        config_json = {
            'HOST' : ' localhost',
            'PORT' : 2962,
            #'BOOTSTRAP_PORT':2963,
        }
        file = open(CONFIG_FILE_NAME, 'w', encoding="utf-8")
        json.dump(config_json,file)
        file.close()

def getBootStrap(host, port):
    import time,json
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
    except socket.error as msg:
        print(msg)
        return []
    #print(s.recv(1024))  # 目的在于接受：Accept new connection from (...
    #while True:
    #s.recv(1024)
    data = 'get_bootstrap_nodes'.encode()
    s.send(data)
    lg = 0
    try:
        l = s.recv(4)
        lg = int(l)
        print('数据长度'+str(lg))
    except:
        return []
    if lg>0:
        bootstrap = s.recv(lg)
        if bootstrap:
            bootstrap_json = json.loads(bootstrap)
            #saveBootStrap(bootstrap_json)
            s.close()
            return bootstrap_json
    return []
    s.close()
def testHost(host, port):
    start = time.time()
    bootstrap_json = getBootStrap(host, port)
    end = time.time()
    speed = end - start
    return speed,bootstrap_json


def getFastDMQTTServer():
    global HOST, MQTT_PORT, BOOTSTRAP_PORT
    # host,port = 'localhost', 2963
    loadConfig()
    speedhost, speedport = HOST, BOOTSTRAP_PORT
    speed, _ = testHost(HOST, BOOTSTRAP_PORT)
    bootstrap_json = loadBootStrap()  # [(HOST, DHT_PORT),(HOST, DHT_PORT)...]
    if bootstrap_json:
        for host, port in bootstrap_json:
            port += 2  # BOOTSTRAP_PORT:DHT_PORT+2
            times, bootstrap = testHost(host, port)
            if bootstrap:
                print('IP:{},PORT:{},SPEED{}'.format(host, port - 1, times))
            else:
                continue
            if speed < times:
                speed = times
                speedhost, speedport = host, port
    else:
        # loadConfig()
        # host, port = HOST, BOOTSTRAP_PORT
        bootstrap = getBootStrap(speedhost, speedport)
        if bootstrap:
            saveBootStrap(bootstrap)
    return speedhost, speedport-1


def startDMQTTClient():
    speedhost, speedport = getFastDMQTTServer()
    startClient(speedhost, speedport,False,False,False,False,False)
    

if __name__ == '__main__':
    startDMQTTClient()