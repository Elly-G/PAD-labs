import sys
import argparse
import middleware
import random
import time

text = ["Lorem ipsum dolor sit amet, consectetur adipiscing elit, ",
"sed do eiusmod tempor incididunt ut labore et dolore ",
"Ut enim ad minim veniam, quis nostrud ",
"exercitation ullamco laboris nisi ut aliquip ex ea",
"reprehenderit in voluptate velit esse cillum dolore eu ",]


class Producer:
    def __init__(self, datatype):
        self.type = datatype
        self.queue = [middleware.JSONQueue(f"/{self.type}", middleware.MiddlewareType.PRODUCER)]
        if datatype == "currency":
            self.gen = self._currency
        elif datatype == "msg":
            self.gen = self._msg
        elif datatype == "telephony":
            self.queue = [middleware.JSONQueue(f"/{self.type}/orange", middleware.MiddlewareType.PRODUCER),
                          middleware.JSONQueue(f"/{self.type}/unite", middleware.MiddlewareType.PRODUCER),
                          middleware.JSONQueue(f"/{self.type}/moldcell", middleware.MiddlewareType.PRODUCER)]
            self.gen = self._telephony

    @classmethod
    def datatypes(self):
        return ["currency", "msg", "telephony"]

    def _currency(self):
        time.sleep(0.1)
        yield random.randint(0,40)

    def _msg(self):
        time.sleep(0.2)
        yield random.choice(text)

    def _telephony(self):
        time.sleep(0.1)
        yield random.randint(0,40)
        time.sleep(0.1)
        yield random.randint(0,100)
        time.sleep(0.1)
        yield random.randint(10000,11000)

    def run(self, length=10):
        for _ in range(length):
            for queue, value in zip(self.queue, self.gen()):
                queue.push(value)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [currency, msg, telephony]", default="currency")
    parser.add_argument("--length", help="number of messages to be sent", default=10)
    args = parser.parse_args()

    if args.type not in Producer.datatypes():
        print("Error: not a valid producer type")
        sys.exit(1)

    p = Producer(args.type)

    p.run(int(args.length))