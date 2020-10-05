from enum import Enum
import socket
import selectors
import json
from broker import Broker


class MiddlewareType(Enum):
    CONSUMER = 1
    PRODUCER = 2


class Queue:
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        self.topic = topic
        self.HOST = 'localhost'  # Adresse du courtier de messages
        self.PORT = 8000  # Le même port que celui utilisé par le courtier de messages
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.HOST, self.PORT))
        self.type = type
        serialization = str(self.__class__.__name__).encode(
            'utf-8')  # Obtenez le nom de la classe, car c'est le numéro de série. mécanisme
        msgsize = '{:>5}'.format(str(len(serialization))).encode()
        self.s.send(msgsize)
        self.s.send(serialization)
        if self.type == MiddlewareType.CONSUMER:
            # s'abonner au sujet passé comme argument de ligne de commande, par exemple: --type weather ou --type /
            self.subscribe(self.topic)

    def push(self, value):
        print(value)
        self.sendMsg('PUBLISH', value)

    def pull(self):
        nBytes2 = self.s.recv(5)
        nBytes2 = int(nBytes2.decode('utf-8'))
        data = self.s.recv(nBytes2)
        # reçoit des informations du courtier
        if data:
            method, topic, msg = self.decode(data)
            if method == "LIST_ACK" or method == "LIST_NACK":
                msg = msg.replace("\\n", "\n").replace("\\t", "\t")
                topic = topic.replace("\\n", "\n")
            return topic, msg
        else:
            return topic, "No data :("

    def subscribe(self, topic):
        self.sendMsg('SUBSCRIBE', topic)

    def sendMsg(self, method, data):
        # envoyer un message au Broker en encodant la méthode (opération à effectuer), le sujet à traiter et le message
        data = self.encode(method, self.topic, data)
        # envoyer la taille du message avant le message lui-même.
        msgsize = str(len(data))
        msgsize = "{:>5}".format(msgsize)
        msgsize = msgsize.encode('utf-8')
        self.s.send(msgsize)
        self.s.send(data)

    def cancelSub(self, topic):
        self.sendMsg('CANCEL_SUB', topic)

    def listTopics(self):
        self.sendMsg('LIST', '')


class JSONQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)

    def decode(self, data):
        data = data.decode('utf-8')
        msg = json.loads(data)
        op = msg['method']
        topic = msg['topic']
        msg = msg['msg']
        return op, topic, msg

    def encode(self, method, topic, msg):
        init = {'method': method, 'topic': topic, 'msg': msg}
        init = json.dumps(init)
        init = init.encode('utf-8')
        return init