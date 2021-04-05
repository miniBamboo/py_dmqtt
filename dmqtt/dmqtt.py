# -*-coding:utf-8 -*-
'''
@file    :   dmqtt.py
@time    :   2021/02/27 07:25:33
@author  :   Fu Ming Chen
@version :   0.1
@contact :   wechat:18613838275
@license :   GPL-3.0 License
@link    :   https://github.com/MiniBamboo/python_dmqtt
'''

import socket
import threading
import time
import sqlite3
from queue import Queue
import decoders, definition, encoders
import pydht #hashing
import hashlib
import socketserver

global dht
global HOST, DHT_PORT, MQTT_PORT, BOOTSTRAP_PORT, BOOT_HOST, BOOT_PORT, CONFIG_FILE_NAME, BOOTSTRAP_CONFIG
global SUBSCRIPTION
#global NODES_ADDRESS

BOOTSTRAP_CONFIG = 'bootstrap.json'
CONFIG_FILE_NAME = 'config.json'
HOST = ' localhost'
DHT_PORT = 2961
MQTT_PORT = DHT_PORT+1
BOOTSTRAP_PORT = DHT_PORT+2
BOOT_HOST = None
BOOT_PORT = None
SUBSCRIPTION = 'subscription' #hashlib.md5('subscription').hexdigest() #hashing.hash_function('subscription')
# NODES_ADDRESS = []

ACK_TIMEOUT = 5

connections = []
connectionsLock = threading.Lock()
messageQueue = Queue(0)
timeoutTimers = []
retainedMessages = []
packetIdentifier = 513
packetIdentifierLock = threading.Lock()


def checkFatherSon(fatherTopic, sonTopic):
    """
        Description:
            检验两个主题之间是否存在父-子层级关系。
        Args:
            fatherTopic(string)
                待检验的父主题。
            sonTopic(string)
                待检验的子主题。
        Returns:
            boolean
                True-存在父子关系，False-不存在父子关系。
        Raises:
            None
    """
    if (fatherTopic.__len__() - 2) < sonTopic.__len__():
        try:
            sonTopic.index(fatherTopic[0:fatherTopic.__len__() - 2])
            if sonTopic[fatherTopic.__len__() - 2] == '/':
                return True
            else:
                return False
        except:
            return False
    else:
        return False


def generatePacketIdentifier():
    """
        Description:
            生成短期内唯一的报文标识符。为保证唯一性，此方法申请了线程锁，因此是线程阻塞的。
        Args:
            None
        Returns:
            string
                报文标识符。
        Raises:
            None
    """
    global packetIdentifier
    packetIdentifierLock.acquire()
    if packetIdentifier < 1024:
        packetIdentifier = packetIdentifier + 1
    else:
        packetIdentifier = 513
    packetIdentifierLock.release()
    return packetIdentifier


def install():
    """
        Description:
            初始化服务器所需的数据库。
        Args:
            None
        Returns:
            None
        Raises:
            Exception
                参见sqlite3的异常抛出规则。
    """
    pass
    '''conn = sqlite3.connect('serverDB.db')
    cursor = conn.cursor()
    cursor.execute('create table if not exists subscription (clientId varchar(100), topic varchar(100), qos int(2))')
    cursor.close()
    conn.commit()
    conn.close()'''


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


def addRetain(messageItem):
    """
        Description:
            向运行存储中添加保留（RETAIN）消息。
        Args:
            messageItem(Dict)
                保留消息字典。必须包含'topic'和'message'字段。
        Returns:
            None
        Raises:
            None
    """
    for i in range(0, retainedMessages.__len__()):
        if retainedMessages[i]['topic'] == messageItem['topic']:
            retainedMessages.pop(i)
            break
    retainedMessages.append(messageItem)


def checkUser(userName, password):
    """
        Description:
            用户验证方法（可自定义，默认全部放通）。
        Args:
            userName(string)
                待验证的用户名。
            password(string)
                待验证的密码。
        Returns:
            int
                0: 用户名或密码格式无效；
                1: 用户名或密码不正确；
                2: 用户验证成功。
        Raises:
            None
    """
    # 0 = INVALID
    # 1 = UNAUTHORIZED
    # 2 = OK
    return 2


def getAllSubscribe():
    """
        Description:
            获取当前服务端上全部的订阅信息。
        Args:
            None
        Returns:
            Array[Tuple(3)]
                订阅信息数组。数组由订阅元组构成，元组0位为clientId，元组1位为topic，元组2位为QoS。
        Raises:
            Exception
                参见sqlite3的异常抛出规则。
    """
    res = []
    supscription = dht[SUBSCRIPTION]
    if supscription:
        for client in supscription:
            records = dht[client]
            if records:
                for hashrecord in records:
                    #record = dht[hashrecord]
                    record = eval(hashrecord)
                    res.append(record)
    return res


def getSubscribe(clientId):
    """
        Description:
            获取指定clientID的全部订阅信息。
        Args:
            clientId(string)
                指定的clientID。
        Returns:
            Array[Tuple(3)]
                订阅信息数组。数组由订阅元组构成，元组0位为clientId，元组1位为topic，元组2位为QoS。
        Raises:
            Exception
                参见sqlite3的异常抛出规则。
    """
    res = []
    records = dht[clientId]
    if records:
        for hashrecord in records:
            #record = dht[hashrecord]
            record = eval(hashrecord)
            res.append(record)
    return res



def getSubscribers(topic):
    """
        Description:
            获取指定主题的全部订阅信息。
        Args:
            topic(string)
                指定的主题。
        Returns:
            Array[Tuple(3)]
                订阅信息数组。数组由订阅元组构成，元组0位为clientId，元组1位为topic，元组2位为QoS。
        Raises:
            Exception
                参见sqlite3的异常抛出规则。
    """
    topicStages = topic.split('/')
    topics = [topic]
    allRes = []
    for i in range(0, topicStages.__len__() - 1):
        ntopic = '/'.join(topicStages[0:topicStages.__len__() - 1 - i])
        ntopic = ntopic + '/#'
        topics.append(ntopic)
    for qtopic in topics:
        res = []
        #topic_hash = hashlib.md5(str(qtopic)).hexdigest()
        topic_records = dht[qtopic]
        if topic_records:
            for hashrecord in topic_records:
                #record = dht[hashrecord]
                record = eval(hashrecord)
                res.append(record)
            if res:
                allRes.extend(res)
    return allRes


def addSubscribe(clientId, topic, qos):
    """
        Description:
            向数据库内写入新的订阅信息。
        Args:
            clientId(string)
                订阅者的clientId。
            topic(string)
                订阅主题。
            qos(int)
                订阅服务质量等级。
        Returns:
            None
        Raises:
            Exception
                参见sqlite3的异常抛出规则。
    """
    #save clientId to supscription key
    #supscription = []
    supscription = dht[SUBSCRIPTION]
    if clientId not in supscription:
        supscription.append(clientId)
        dht[SUBSCRIPTION] = supscription
    # save  record hash to clientId key and  save  data to hashrecord
    record = [clientId, topic, qos]
    hashrecord = str(record)  # str(clientId)+str(topic)+str(qos) #hashlib.md5().hexdigest()
    clients = dht[clientId]
    if clients:
        if hashrecord not in clients:
            clients.append(hashrecord)
            dht[clientId] = clients
            # record = [clientId, topic, qos]
            # dht[hashrecord] = record
        else:
            print('clientId{},Topic{} exists.'.format(clientId, topic))
    else:
        clients = [hashrecord]
        dht[clientId] = clients
        # record = [clientId, topic, qos]
        # dht[hashrecord] = record
    # save  topic records to topic hash
    # topic_hash = hashlib.md5( str(topic) ).hexdigest()
    # topic_records = dht[topic_hash]
    topic_records = dht[topic]
    if topic_records:
        if hashrecord not in topic_records:
            topic_records.append(hashrecord)
            #dht[topic_hash] = topic_records
            dht[topic] = topic_records
        else:
            print('record:[{},{},{}] exists.'.format(clientId, topic.qos))
    else:
        topic_records = [hashrecord]
        #dht[topic_hash] = topic_records
        dht[topic] = topic_records


    '''
    conn = sqlite3.connect('serverDB.db')
    cursor = conn.cursor()
    cursor.execute(
        'insert into subscription(clientId, topic, qos) values (\'' + clientId + '\',\'' + topic + '\',' + str(
            qos) + ')')
    cursor.close()
    conn.commit()
    conn.close()
    '''

def removeSubscribe(clientId, topic=''):
    """
        Description:
            移除对应用户、主题的订阅信息；当主题参数为空时，移除对应用户的全部订阅信息。
        Args:
            clientId(string)
                移除订阅信息的用户clientID。
            topic(string, DEFAULT='')
                移除订阅信息的主题。
        Returns:
            None
        Raises:
            Exception
                参见sqlite3的异常抛出规则。
    """
    if topic == '':
        records = dht[clientId]
        for hashrecord in records:
            record = eval(hashrecord)
            dht_topic = record[1]
            topic_records = dht[dht_topic]
            if topic_records:
                if hashrecord in topic_records:
                    topic_records.remove(hashrecord)
                    dht[dht_topic] = topic_records
                    #dht.delete(dht_topic)
        dht.delete(clientId) #dht[clientId] = []
    else:
        records = dht[clientId]
        for hashrecord in records:
            record = eval(hashrecord)
            if clientId == record[0] and topic == record[1]:
                records.remove(hashrecord)
                dht[clientId] = records
                #dht.delete(hashrecord)
                topic_records = dht[topic]
                if topic_records:
                    if hashrecord in topic_records:
                        topic_records.remove(hashrecord)
                        dht[topic] = topic_records
            break #only one?





def pubackNotReceived(connection):
    """
        Description:
            未收到PUBACK响应的callback方法，用以与收到QoS1级别PUBLISH报文后启动的timeout线程绑定。
        Args:
            connection(Server.Connection):
                未收到响应的连接单元。
        Returns:
            None
        Raises:
            IOException
                socket连接异常。
    """
    print(
        "[" + getTime() + "]" + " [SERVER/INFO] Client " + connection.getAddress() + " has disconnected: Puback timeout.")
    connection.onDisconnect()


def pubrecNotReceived(connection):
    """
        Description:
            未收到PUBREC响应的callback方法，用以与收到QoS2级别PUBLISH报文后启动的timeout线程绑定。
        Args:
            connection(Server.Connection)
                未收到响应的连接单元。
        Returns:
            None
        Raises:
            IOException
                socket连接异常。
    """
    print(
        "[" + getTime() + "]" + " [SERVER/INFO] Client " + connection.getAddress() + " has disconnected: Pubrec timeout.")
    connection.onDisconnect()


def publishFromQueue():
    """
        Description:
            从线程安全的messageQueue中按队列顺序发送消息。
        Args:
            None
        Returns:
            None
        Raises:
            IOException
                socket连接异常。
    """
    while True:
        if messageQueue.qsize() != 0:
            messageItem = messageQueue.get()
            subscribers = getSubscribers(messageItem['topic'])
            for i in range(0, subscribers.__len__()):
                subscriber = subscribers[i][0]
                qos = subscribers[i][2]
                connectionsLock.acquire()
                if qos == 0:
                    for j in range(0, connections.__len__()):
                        if connections[j].getClientId() == subscriber:
                            connections[j].send(
                                encoders.PUBLISH_Encoder(0, qos, 0, messageItem['topic'], 0, messageItem['message']))
                elif qos == 1:
                    packetIdentifier = generatePacketIdentifier()
                    for j in range(0, connections.__len__()):
                        if connections[j].getClientId() == subscriber:
                            connections[j].send(
                                encoders.PUBLISH_Encoder(0, qos, 0, messageItem['topic'], packetIdentifier,
                                                         messageItem['message']))
                            timer = threading.Timer(ACK_TIMEOUT, pubackNotReceived, [connections[j], ])
                            timeoutTimers.append({
                                'clientId': connections[j].getClientId(),
                                'packetIdentifier': packetIdentifier,
                                'waitingFor': 'PUBACK',
                                'timer': timer
                            })
                            timer.start()
                elif qos == 2:
                    packetIdentifier = generatePacketIdentifier()
                    for j in range(0, connections.__len__()):
                        if connections[j].getClientId() == subscriber:
                            connections[j].send(
                                encoders.PUBLISH_Encoder(0, qos, 0, messageItem['topic'], packetIdentifier,
                                                         messageItem['message']))
                            timer = threading.Timer(ACK_TIMEOUT, pubrecNotReceived, [connections[j], ])
                            timeoutTimers.append({
                                'clientId': connections[j].getClientId(),
                                'packetIdentifier': packetIdentifier,
                                'waitingFor': 'PUBREC',
                                'timer': timer
                            })
                            timer.start()
                connectionsLock.release()
            if messageItem['retain'] == 1:
                addRetain(messageItem)
            print("[" + getTime() + "]" + " [SERVER/INFO] Queue processed a message." + str(
                messageQueue.qsize()) + " message(s) in queue, " + str(
                retainedMessages.__len__()) + " message(s) retained.")


class Connection(threading.Thread):
    '''
        Description:
            与客户端进行信息交互与处理的类。继承自Thread。
        Args:
            socket(Socket)
                与客户端建立连接的套接字对象。
            address(tuple)
                客户端的套接字地址。
    '''

    SOCKET_DISCONNECTED = 0
    '''
        状态标识符：未连接。
    '''

    SOCKET_CONNECTED = 1
    '''
        状态标识符：socket已连接。
    '''

    MQTT_CONNECTED = 2
    '''
        状态标识符：MQTT已连接。
    '''

    socketLock = threading.Lock()
    '''
        socket线程锁。为确保消息发送顺序的正确性，所有有关当前connection中“发送消息”的操作，都必须先申请此线程锁，并在操作完成后进行释放。
    '''

    def getClientId(self):
        """
            Description:
                获取当前连接的clientID。
            Args:
                None
            Returns:
                string
                    当前连接的clientID。
            Raises:
                None
        """
        return self.clientId

    def getAddress(self):
        """
            Description:
                获取当前连接的socket套接字字符串。
            Args:
                None
            Returns:
                string
                    当前连接的socket套接字字符串。
            Raises:
                None
        """
        return str(self.address)

    def getSock(self):
        """
            Description:
                获取当前连接的套接字对象。
            Args:
                None
            Returns:
                string
                    当前连接的套接字对象。
            Raises:
                None
        """
        return self.socket

    def __init__(self, socket, address):
        threading.Thread.__init__(self)
        self.socket = socket
        self.address = address
        self.alive = 1
        self.clientId = ''
        self.keepAlive = 0
        self.count = 0
        self.willFlag = 0
        self.willTopic = ''
        self.willMessage = ''
        self.willQos = 0
        self.willRetain = 0

    def __str__(self):
        return str(self.address)

    def publishWill(self):
        """
            Description:
                代替客户端发布遗嘱消息。
            Args:
                None
            Returns:
                None
            Raises:
                None
        """
        if self.willTopic != '':
            messageQueue.put({
                'retain': self.willRetain,
                'topic': self.willTopic,
                'message': self.willMessage
            })

    def onDisconnect(self):
        """
            Description:
                客户端断开连接、或断开客户端连接时进行的系列操作，包含停止timeout计时器、发布遗嘱、关闭套接字三部分，发布遗嘱和关闭套接字申请了线程锁，因此整个方法是线程阻塞的。
            Args:
                None
            Returns:
                None
            Raises:
                IOException
                    socket连接异常。
        """
        self.alive = self.SOCKET_DISCONNECTED
        try:
            delIndexs = []
            for i in range(0, timeoutTimers.__len__()):
                if timeoutTimers[i]['clientId'] == self.clientId:
                    timeoutTimers[i]['timer'].cancel()
                    delIndexs.append(i)
            while delIndexs.__len__() != 0:
                timeoutTimers.pop(delIndexs.pop())
            connectionsLock.acquire()
            connections.remove(self)
            connectionsLock.release()
        except:
            pass
        self.socketLock.acquire()
        self.publishWill()
        self.socket.close()
        self.socketLock.release()

    def counter(self):
        """
            Description:
                保活（keepAlive）计时器，用以作为保活线程的主方法。
            Args:
                None
            Returns:
                None
            Raises:
                None
        """
        while self.alive != self.SOCKET_DISCONNECTED:
            time.sleep(1)
            self.count = self.count + 1
            if self.count >= self.keepAlive:
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(
                    self.address) + " has disconnected: Connection time out.")
                self.onDisconnect()
                break

    def run(self):
        """
            Description:
                接收消息的线程主方法，基于剩余长度实现帧定界，并传递给解码方法。
            Args:
                None
            Returns:
                None
            Raises:
                None
        """
        while self.alive != self.SOCKET_DISCONNECTED:
            try:
                oneMessage = b''
                data = self.socket.recv(2)
                if data != b'':
                    oneMessage += data
                    remainedLengthBytes = b''
                    remainedLengthBytes += int.to_bytes(data[1], 1, 'big')
                    while data != b'' and (data[data.__len__() - 1] & 128) != 0:
                        self.count = 0
                        data = self.socket.recv(1)
                        oneMessage += data
                        remainedLengthBytes += data
                    data = self.socket.recv(decoders.remainingBytes_Decoder(remainedLengthBytes, True)[1])
                    oneMessage += data
            except Exception as e:
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " has disconnected.")
                self.onDisconnect()
                break
            if oneMessage != b'':
                self.decode(oneMessage)

    def pubcompNotReceived(self):
        """
            Description:
                未收到PUBCOMP响应的callback方法，用以与收到PUBREL报文后启动的timeout线程绑定。
            Args:
                None
            Returns:
                None
            Raises:
                None
        """
        print(
            "[" + getTime() + "]" + " [SERVER/INFO] Client " + self.getAddress() + " has disconnected: Pubcomp timeout.")
        self.onDisconnect()

    def decode(self, data):
        """
            Description:
                对于给定的报文字节串进行解码，并实现对应的逻辑响应。
            Args:
                data(byte[])
                    待解码的报文字节串。
            Returns:
                None
            Raises:
                decoders.IllegalMessageException
                    消息解码错误。
                IOException
                    socket连接错误。
        """
        if data == b'':
            return
        try:
            messageType, results = decoders.message_Decoder(data)
            if messageType == definition.messageType.CONNECT:
                if results['clientId'].isalnum() and results['clientId'].__len__() >= 1 and results[
                    'clientId'].__len__() <= 23:
                    self.clientId = results['clientId']
                    if checkUser(results['userName'], results['password']) == 2:
                        if results['cleanSession'] == 1:
                            removeSubscribe(results['clientId'])
                        self.keepAlive = results['keepAlive']
                        self.willFlag = results['willFlag']
                        if self.willFlag == 1:
                            self.willTopic = results['willTopic']
                            self.willMessage = results['willMessage']
                            self.willQos = results['willQos']
                            self.willRetain = results['willRetain']
                        print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " has connected.")
                        sessionPresent = getSubscribe(self.clientId).__len__()
                        if sessionPresent > 0:
                            sessionPresent = 1
                        self.send(encoders.CONNACK_Encoder(sessionPresent, definition.ConnackReturnCode.ACCEPTED))
                        scs = getSubscribe(self.clientId)
                        for sc in scs:
                            for retainedMessage in retainedMessages:
                                if checkFatherSon(sc[1], retainedMessage['topic']) or retainedMessage['topic'] == sc[1]:
                                    packetIdentifier = generatePacketIdentifier()
                                    print(111)
                                    self.send(encoders.PUBLISH_Encoder(0, sc[2], 0, retainedMessage['topic'],
                                                                       packetIdentifier, retainedMessage['message']))
                                    if sc[2] == 2:
                                        timer = threading.Timer(ACK_TIMEOUT, pubrecNotReceived, [self, ])
                                        timeoutTimers.append({
                                            'clientId': self.getClientId(),
                                            'packetIdentifier': packetIdentifier,
                                            'waitingFor': 'PUBREC',
                                            'timer': timer
                                        })
                                        timer.start()
                                    elif sc[2] == 1:
                                        timer = threading.Timer(ACK_TIMEOUT, pubackNotReceived, [self, ])
                                        timeoutTimers.append({
                                            'clientId': self.getClientId(),
                                            'packetIdentifier': packetIdentifier,
                                            'waitingFor': 'PUBACK',
                                            'timer': timer
                                        })
                                        timer.start()
                                    print(
                                        "[" + getTime() + "]" + " [SERVER/INFO] A retained message sent to Client " + str(
                                            self.address) + " at packet " + str(packetIdentifier) + " .")
                                    retainedMessages.remove(retainedMessage)
                                    break
                        keepAliveThread = threading.Thread(target=self.counter)
                        keepAliveThread.start()
                    elif checkUser(results['userName'], results['password']) == 0:
                        self.send(encoders.CONNACK_Encoder(0, definition.ConnackReturnCode.REFUSED_INVALID_USER))
                        print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(
                            self.address) + " has disconnected: Illegal User or Password.")
                        self.onDisconnect()
                    else:
                        self.send(encoders.CONNACK_Encoder(0, definition.ConnackReturnCode.REFUSED_UNAUTHORIZED))
                        print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(
                            self.address) + " has disconnected: Unauthorized user.")
                        self.onDisconnect()
                else:
                    self.send(encoders.CONNACK_Encoder(0, definition.ConnackReturnCode.REFUSED_ILLEGAL_CLIENTID))
                    print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(
                        self.address) + " has disconnected: Illegal ClientId.")
                    self.onDisconnect()
            elif messageType == definition.messageType.SUBSCRIBE:
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " subscribing...")
                packetIdentifier = results['packetIdentifier']
                topics = results['topics']
                returnCodes = []
                for i in range(0, topics.__len__()):
                    removeSubscribe(self.clientId, topics[i]['topic'])
                    addSubscribe(self.clientId, topics[i]['topic'], topics[i]['qos'])
                    returnCodes.append(topics[i]['qos'])
                self.send(encoders.SUBACK_Encoder(packetIdentifier, returnCodes))
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " subscribed.")
                print("[" + getTime() + "]" + " [SERVER/INFO] Current subscirbes: " + str(getAllSubscribe()) + " .")
            elif messageType == definition.messageType.UNSUBSCRIBE:
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " unsubscribing...")
                packetIdentifier = results['packetIdentifier']
                topics = results['topics']
                for i in range(0, topics.__len__()):
                    removeSubscribe(self.clientId, topics[i])
                self.send(encoders.UNSUBACK_Encoder(packetIdentifier))
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " unsubscribed.")
                print("[" + getTime() + "]" + " [SERVER/INFO] Current subscirbes: " + str(getAllSubscribe()) + " .")
            elif messageType == definition.messageType.PINGREQ:
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " sent a heartbeat.")
                self.send(encoders.PINGRESP_Encoder())
            elif messageType == definition.messageType.PUBLISH:
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " sent a message.")
                messageQueue.put({
                    'retain': results['retain'],
                    'topic': results['topic'],
                    'message': results['message']
                })
                if results['qos'] == 1:
                    self.send(encoders.PUBACK_Encoder(results['packetIdentifier']))
                    print("[" + getTime() + "]" + " [SERVER/INFO] PUBACK responded to Client " + str(
                        self.address) + " at packet " + str(results['packetIdentifier']) + " .")
                elif results['qos'] == 2:
                    self.send(encoders.PUBREC_Encoder(results['packetIdentifier']))
                    print("[" + getTime() + "]" + " [SERVER/INFO] PUBREC responded to Client " + str(
                        self.address) + " at packet " + str(results['packetIdentifier']) + " .")
            elif messageType == definition.messageType.PUBREL:
                self.send(encoders.PUBCOMP_Encoder(results['packetIdentifier']))
                print("[" + getTime() + "]" + " [SERVER/INFO] PUBCOMP responded to Client " + str(
                    self.address) + " at packet " + str(results['packetIdentifier']) + " .")
            elif messageType == definition.messageType.PUBACK:
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " responded a PUBACK.")
                clientId = self.clientId
                packetIdentifier = results['packetIdentifier']
                try:
                    for i in range(0, timeoutTimers.__len__()):
                        if timeoutTimers[i]['packetIdentifier'] == packetIdentifier and timeoutTimers[i][
                            'clientId'] == clientId and timeoutTimers[i]['waitingFor'] == 'PUBACK':
                            timeoutTimers[i]['timer'].cancel()
                except:
                    pass
            elif messageType == definition.messageType.PUBREC:
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " responded a PUBREC.")
                clientId = self.clientId
                packetIdentifier = results['packetIdentifier']
                self.send(encoders.PUBREL_Encoder(packetIdentifier))
                print("[" + getTime() + "]" + " [SERVER/INFO] PUBREL responded to Client " + str(
                    self.address) + " at packet " + str(packetIdentifier) + " .")
                try:
                    for i in range(0, timeoutTimers.__len__()):
                        if timeoutTimers[i]['packetIdentifier'] == packetIdentifier and timeoutTimers[i][
                            'clientId'] == clientId and timeoutTimers[i]['waitingFor'] == 'PUBREC':
                            timeoutTimers[i]['timer'].cancel()
                            break
                    timer = threading.Timer(ACK_TIMEOUT, self.pubcompNotReceived)
                    timeoutTimers.append({
                        'clientId': self.getClientId(),
                        'packetIdentifier': packetIdentifier,
                        'waitingFor': 'PUBCOMP',
                        'timer': timer
                    })
                    timer.start()
                except:
                    pass
            elif messageType == definition.messageType.PUBCOMP:
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " responded a PUBCOMP.")
                clientId = self.clientId
                packetIdentifier = results['packetIdentifier']
                try:
                    for i in range(0, timeoutTimers.__len__()):
                        if timeoutTimers[i]['packetIdentifier'] == packetIdentifier and timeoutTimers[i][
                            'clientId'] == clientId and timeoutTimers[i]['waitingFor'] == 'PUBCOMP':
                            timeoutTimers[i]['timer'].cancel()
                            break
                except:
                    pass
            elif messageType == definition.messageType.DISCONNECT:
                self.onDisconnect()
                print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(self.address) + " has disconnected.")
        except decoders.IllegalMessageException:
            print(data)
            print("[" + getTime() + "]" + " [SERVER/INFO] Client " + str(
                self.address) + " has disconnected: Illegal Message Received.")
            self.onDisconnect()

    def send(self, data):
        """
            Description:
                向客户端发送字节串。
            Args:
                data(bytes[])
                    待发送的字节串。
            Returns:
                None
            Raises:
                IOException
                    socket连接错误。
        """
        self.socketLock.acquire()
        try:
            self.socket.sendall(data)
        except:
            pass
        self.socketLock.release()


def newConnections(socket):
    """
        Description:
            监听指向当前服务端的连接，并为新的连接创建Connection对象。
        Args:
            socket(Socket)
                当前服务端套接字对象。
        Returns:
            None
        Raises:
            IOException
                socket连接异常。
    """
    while True:
        sock, address = socket.accept()
        connectionsLock.acquire()
        connections.append(Connection(sock, address))
        connections[len(connections) - 1].start()
        connectionsLock.release()
        print("[" + getTime() + "]" + " [SERVER/INFO] New connection at " + str(connections[len(connections) - 1]))


def startServer(host, port):
    """
        Description:
            启动服务端。
        Args:
            host(string)
                服务端地址。这一地址通常需要设置为内网私有IP（即便目的是在公网上运行）。
            port(int)
                服务端端口号。
        Returns:
            None
        Raises:
            IOException
                socket连接异常。
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(128)
    newConnectionsThread = threading.Thread(target=newConnections, args=(sock,))
    newConnectionsThread.start()
    messageQueueThread = threading.Thread(target=publishFromQueue)
    messageQueueThread.start()

    print("====Decentarlized MQTT-Server v0.1====")
    print("[" + getTime() + "]" + " [SERVER/INFO] Successfully started!")
    print("[" + getTime() + "]" + " [SERVER/INFO] running on " + host + ":" + str(port))# -*-coding:utf-8 -*-


def bootstrapServer(host, port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 防止socket server重启后端口被占用（socket.error: [Errno 98] Address already in use）
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(128)
    except socket.error as msg:
        print(msg)
        #sys.exit(1)
    print('BootstrapServer waiting connection...')
    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=bootstrapData, args=(conn, addr))
        t.start()
def bootstrapData(conn, addr):
    import json
    global dht
    print('Accept new connection from {0}'.format(addr))
    #conn.send(('Hi, Welcome to the server!').encode())
    while True:
        data = conn.recv(1024)
        print('{0} client send data is {1}'.format(addr, data.decode()))#b'\xe8\xbf\x99\xe6\xac\xa1\xe5\x8f\xaf\xe4\xbb\xa5\xe4\xba\x86'
        #time.sleep(1)
        if data == 'get_bootstrap_nodes':  #or not data:
            print('{0} connection close'.format(addr))
            nodes_address = json.dumps(dht.get_nodes(dht.peer.id))
            if nodes_address:
                length = '{:0>4d}'.format(len(nodes_address))
                conn.send(bytes(length), 'UTF-8')
                conn.send(bytes(nodes_address),'UTF-8')
            else:
                nodes_address = json.dumps([])
                length ='{:0>4d}'.format(len(nodes_address))
                conn.send(bytes(length), 'UTF-8')
                conn.send(bytes(nodes_address), 'UTF-8')
            break
        else:
           pass
        #conn.send(bytes('Hello, {0}'.format(data),"UTF-8"))#TypeError: a bytes-like object is required, not 'str'
    conn.close()
# DMQTT Server config port only gived DHT port，
# add 1 and 2 to DHT port for getting MQTT Server port and bootstrap server port .
def loadConfig():
    global HOST, MQTT_PORT, DHT_PORT, BOOTSTRAP_PORT, BOOT_HOST, BOOT_PORT, CONFIG_FILE_NAME
    import os,json
    if os.path.exists(CONFIG_FILE_NAME):
        file = open(CONFIG_FILE_NAME, 'r', encoding="utf-8")
        config_json = json.load(file)
        file.close()
        HOST = config_json['HOST']
        DHT_PORT = config_json['PORT']
        MQTT_PORT = DHT_PORT+1
        BOOTSTRAP_PORT = DHT_PORT+2
        BOOT_HOST = config_json['BOOT_HOST']
        BOOT_PORT = config_json['BOOT_PORT']
    else:
        config_json = {
            'HOST' : ' localhost',
            'PORT' : 2961,
            #'DHT_PORT' : 2962,
            #'BOOTSTRAP_PORT':2963,
            'BOOT_HOST' : None,
            'BOOT_PORT' : None
        }
        file = open(CONFIG_FILE_NAME, 'w', encoding="utf-8")
        json.dump(config_json,file)
        file.close()


def startDMQTTServer():
    global dht
    global HOST, DHT_PORT, MQTT_PORT, BOOTSTRAP_PORT, BOOT_HOST, BOOT_PORT, CONFIG_FILE_NAME, BOOTSTRAP_CONFIG

    loadConfig()
    dht = pydht.DHT(HOST, DHT_PORT, boot_host=BOOT_HOST, boot_port=BOOT_PORT)

    #supscription = []
    supscription = dht[SUBSCRIPTION]
    if supscription:
        pass
    else:
        supscription = []
        dht[SUBSCRIPTION] = supscription
    #NODES_ADDRESS = dht.get_nodes(dht.peer.id)
    #dht['server_nodes_address'] = NODES_ADDRESS
    startServer(HOST, MQTT_PORT)
    bootstrapServer(HOST, BOOTSTRAP_PORT)


if __name__ == '__main__':
    startDMQTTServer()
