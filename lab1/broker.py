import socket
import json
import selectors


class Broker:
    def __init__(self):
        self.HOST = ''
        self.PORT = 8000
        self.sock = socket.socket()
        self.sock.bind((self.HOST, self.PORT))
        self.sock.listen(100)
        self.sel = selectors.DefaultSelector()
        self.usersdict = {}  # chaque utilisateur et son mécanisme de sérialisation
        self.topicmsg = {}  # chaque sujet et comme valeur: les messages publiés pour ce sujet et ses sous-sujets
        self.run()

    def accept(self, sock, mask):
        conn, addr = self.sock.accept()
        print('accepted', conn, 'from', addr)
        #après avoir établi la connexion avec le socket de la file d'attente, le premier message envoyé est le mécanisme de sérialisation de cette file d'attente
        nBytes = conn.recv(5)
        if nBytes:
            nBytes = int(nBytes.decode())
            data = conn.recv(nBytes)
            if data:
                if data.decode('utf-8') == 'JSONQueue':
                    self.usersdict[conn] = 'JSON'
        else:
            # si aucune donnée reçue, ferme la connexion
            print('closing', conn)
            self.sel.unregister(conn)
            conn.close()
        # conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)

    def sendMsg(self, sock, method, topic, msg):
        if self.usersdict[sock] == 'JSON':
            # encode in JSON
            sendmsg = self.encodeJSON(method, topic, msg)
        # Avant d'envoyer le message lui-même, envoyez d'abord la taille de celui-ci avec
        msgsize = str(len(sendmsg))
        msgsize = "{:>5}".format(msgsize)
        msgsize = msgsize.encode('utf-8')
        sock.send(msgsize)
        # maintenant nous pouvons envoyer le message
        sock.send(sendmsg)

    def read(self, conn, mask):
        # recevoir le reste des informations du middleware
        nBytes = conn.recv(5)
        if nBytes:
            nBytes = int(nBytes.decode('utf-8'))
            data = conn.recv(nBytes)
            if data:
                if conn in self.usersdict:
                    # vérifiez d'abord comment décoder le message
                    if self.usersdict[conn] == 'JSON':
                        method, topic, msg = self.decodeJSON(data)
                    # vérifier la méthode associée au message
                    if method == 'PUBLISH':
                        self.readPubSub(conn, method, topic, msg)
                    elif method == 'SUBSCRIBE':
                        self.readPubSub(conn, method, topic)
                    elif method == 'CANCEL_SUB':
                        self.readCancelSub(conn, msg)
                    elif method == 'LIST':
                        self.listTopics(True, conn, "JustConn")
        else:

            print('closing', conn)
            # même si aucun message cancel_sub n'a été envoyé
            # il est toujours nécessaire de supprimer le socket des structures de données
            self.readCancelSub(conn)
            self.sel.unregister(conn)
            conn.close()

    def readPubSub(self, conn, method, topic, msg=None):
        # utiliser regex pour effectuer les opérations de publication / abonnement
        newTopic = False
        topics = topic.split("/")
        topics[0] = "root"
        if topics[1] == "":
            topics = ["root"]
        users = []
        topic_name = ""
        for i in range(len(topics)):
            topic_name += "/" + str(topics[i])
            if topic_name not in self.topicmsg:
                self.topicmsg[topic_name] = {}
                self.topicmsg[topic_name]["messages"] = []
                self.topicmsg[topic_name]["users"] = []
                if i != 0:
                    newTopic = True
                    newTopic = self.listTopics(newTopic,
                                               conn)  # chaque fois qu'un sujet est créé, nous envoyons la liste des sujets à l'utilisateur avant de publier le message
            self.topicmsg[topic_name]["users"] = self.topicmsg[topic_name]["users"] + list(
                set(users) - set(self.topicmsg[topic_name]["users"]))
            users = users + list(set(self.topicmsg[topic_name]["users"]) - set(
                users))  # transmettre tous les utilisateurs d'un topic à subtopic, en supprimant les doublons
            # publier un msg
            if msg != None:
                # print(topic_name)
                # nous sauvegardons simplement le dernier message de chaque sujet
                self.topicmsg[topic_name]["messages"].append(str(msg))
                if len(self.topicmsg[topic_name]["messages"]) > 1:
                    self.topicmsg[topic_name]["messages"].pop(0)

        # maintenant nous pouvons envoyer le message ...
        if msg != None:
            self.sendtoTopic(topic_name)
        # subscribe topic
        else:
            # recherche d'un sujet d'abonnement (sous-sujets également)
            for topic in self.topicmsg.keys():
                if topic_name in topic:
                    self.topicmsg[topic]["users"].append(conn)
                    if topic != "/root":
                        msg_to_send = self.topicmsg[topic]["messages"]
                        if len(msg_to_send) > 0:
                            # envoyer dernier message enregistré, lorsque vous êtes abonné à un topic avec des messages enregistrés
                            self.sendMsg(conn, "LAST_MSG", topic[5:len(topic)], msg_to_send[len(msg_to_send) - 1])
        newTopic = self.listTopics(newTopic,
                                   conn)  # chaque fois qu'un topic est créé, nous envoyons la liste des sujets à l'utilisateur

    def sendtoTopic(self, topic_name):
        # Publier un message pour tous les utilisateurs du sujet et ses sous-sujets
        for user in self.topicmsg[topic_name]["users"]:
            msg = self.topicmsg[topic_name]["messages"][len(self.topicmspicg[topic_name]["messages"]) - 1]
            self.sendMsg(user, "PUBLISH", topic_name[5:len(topic_name)], msg)

    def readCancelSub(self, conn, canceltopic=None):
        for topic in self.topicmsg:
            # s'il a été envoyé un message d'annulation d'abonnement uniquement supprimer
            # l'abonnement à ce sujet et sous-sujets (s'ils existent)
            if (canceltopic != None):
                if conn in self.topicmsg[topic]["users"] and ("/root" + canceltopic) in topic:
                    self.topicmsg[topic]["users"].remove(conn)
            else:
                # si non, lors de la fermeture de la connexion, nous devons quand même supprimer le socket du dictionnaire
                # qui évite à un éditeur d'envoyer un message à un socket fermé
                if conn in self.topicmsg[topic]["users"]:
                    self.topicmsg[topic]["users"].remove(conn)
        if conn in self.usersdict:
            del self.usersdict[conn]

    def listTopics(self, newTopic, conn, conn_Spec=None):
        # lister tous les topics dans Broker après un abonnement et après la création d'un nouveau sujet
        # informer tous les utilisateurs en ligne
        if newTopic == True:
            users = [conn]
            lst = ""
            for key, value in self.topicmsg.items():
                if key != "/root":
                    lst += "Topic: " + str(key[5:len(key)]) + "\\n"
                    users = users + list(set(value["users"]) - set(users))
            if conn_Spec == None:
                users.remove(conn)
            else:
                users = [conn]
            if len(users) > 0:
                for user in users:
                    if (len(lst) > 0):
                        self.sendMsg(user, 'LIST_ACK', "\\nList of Topics:", "\\n" + str(lst))
                    else:
                        # abonnement à la racine ne compte pas comme sujet
                        self.sendMsg(user, 'LIST_NACK', "\\nList of Topics:", "\\nNo topics created yet.\\n")
        return False

    def decodeJSON(self, data):
        data = data.decode('utf-8')
        msg = json.loads(data)
        op = msg['method']
        topic = msg['topic']
        msg = msg['msg']
        return op, topic, msg

    def encodeJSON(self, method, topic, msg):
        init = {'method': method, 'topic': topic, 'msg': msg}
        init = json.dumps(init)
        init = init.encode('utf-8')
        return init

    def run(self):
        # utiliser des sélecteurs pour enregistrer les événements sur Broker
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)
        while True:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)


if __name__ == "__main__":
    br = Broker()