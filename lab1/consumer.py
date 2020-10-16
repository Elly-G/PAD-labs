import sys
import argparse
import middleware
import time


class Consumer:
    def __init__(self, datatype):
        self.type = datatype
        self.queue = middleware.JSONQueue(f"/{self.type}")

    @classmethod
    def datatypes(self):
        return ["currency", "msg", "telephony","/"]

    def run(self, length=10):
        try:
            self.queue.listTopics()
            while True:
                topic, data = self.queue.pull()
                print(topic,data)
        except KeyboardInterrupt:
                self.queue.cancelSub(self.queue.topic)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [currency, msg, telephony,/]", default="currency")
    args = parser.parse_args()

    if args.type not in Consumer.datatypes():
        print("Error: not a valid producer type")
        sys.exit(1)

    p = Consumer(args.type)

    p.run()
