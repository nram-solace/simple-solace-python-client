#----------------------------------------------------------------------------
# simple-subscriber
#   A simple solace subscriber
#
#
import sys; sys.path.insert(0, "/root/nram/python/virtualenv/solace-python/lib/python3.8/site-packages")

import sys, os
import argparse
import pprint
import json
import yaml
import requests
import inspect
import urllib
from urllib.parse import unquote
import pathlib
import time, datetime
import threading
import random
import traceback
import string

# Import Solace Python  API modules from the solace package
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage


# Globals
Cfg = {}    # cfg dict
Verbose = 0
pp = pprint.PrettyPrinter(indent=4)

Verbose = 0

class T:
    ''' return current timestamp '''
    def __str__(self):
        return f'{datetime.datetime.now()}'

#----------------------------------------------------------------------------
# SolaceBroker Class
#


class SolaceBroker:

    ''' implements solace broker connection handling '''

    #----------------------------------------------------------------------------
    # Inner classes for message, event and error handling
    #
    class MessageHandlerImpl(MessageHandler):
        ''' async message handler callback '''

        def __init__ (self, _name):
            self.name = _name

        def on_message(self, message: InboundMessage):
            topic = message.get_destination_name()
            if Verbose > 0 :
               print (f'{T()}: [{self.name}] <- {topic}')
            if Verbose > 2:
                payload_str = message.get_payload_as_string
                print("\n" + f"Message Payload String: {payload_str} \n")
                print("\n" + f"Message Topic: {topic} \n")
                print("\n" + f"Message dump: {message} \n")

    class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
        ''' solace event handlers '''

        def on_reconnected(self, e: ServiceEvent):
            print("\non_reconnected")
            print(f"Error cause: {e.get_cause()}")
            print(f"Message: {e.get_message()}")
        
        def on_reconnecting(self, e: "ServiceEvent"):
            print("\non_reconnecting")
            print(f"Error cause: {e.get_cause()}")
            print(f"Message: {e.get_message()}")

        def on_service_interrupted(self, e: "ServiceEvent"):
            print("\non_service_interrupted")
            print(f"Error cause: {e.get_cause()}")
            print(f"Message: {e.get_message()}")

    class PublisherErrorHandling(PublishFailureListener):
        ''' solace event handler '''

        def on_failed_publish(self, e: 'FailedPublishEvent'):
            print("on_failed_publish")

        
    def __init__(self, _smfurl, _vpn, _clientusername, _clientpasswd, 
                       _name = "default", _verbose = 0):
        global Verbose
        self.broker_props = {
            "solace.messaging.transport.host": _smfurl,
            "solace.messaging.service.vpn-name": _vpn,
            "solace.messaging.authentication.scheme.basic.username": _clientusername,
            "solace.messaging.authentication.scheme.basic.password": _clientpasswd
        }
        self.name = _name
        Verbose = _verbose # store globally for other classes to use
        if Verbose > 2:
            print (f'broker_props: {self.broker_props}')

    def connect (self):
        print (f'{T()}: {self.name} Connecting to {self.broker_props["solace.messaging.transport.host"]} ({self.broker_props["solace.messaging.service.vpn-name"]})')
        self.messaging_service = MessagingService.builder().from_properties(self.broker_props)\
                    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                    .build()
        # Event Handeling for the messaging service
        self.service_handler = self.ServiceEventHandler()
        self.messaging_service.add_reconnection_listener(self.service_handler)
        self.messaging_service.add_reconnection_attempt_listener(self.service_handler)
        self.messaging_service.add_service_interruption_listener(self.service_handler)

        # Blocking connect thread
        self.messaging_service.connect()
        if not self.messaging_service.is_connected:
            raise Exception (f'Messaging Service not connected')

    def topic_subscriber(self, _topics):
        print (f'{T()}: {self.name} Starting topic subscriber with {len(_topics)} topics')
        if Verbose > 1:
            print (f'Topics: {_topics})')

        # Define a Topic subscriptions 
        topics_sub = []
        for t in _topics:
            topics_sub.append(TopicSubscription.of(t))

        # Build a Receiver with the given topics and start it
        self.direct_receiver = self.messaging_service.create_direct_message_receiver_builder()\
                                .with_subscriptions(topics_sub)\
                                .build()

        self.direct_receiver.start()
        if not self.direct_receiver.is_running():
            raise Exception (f'Topic Subscriber not running.')
        try:
            # Callback for received messages
            self.direct_receiver.receive_async(self.MessageHandlerImpl(self.name))
            while True:
               time.sleep(1)

        finally:
            print('{TS()}: {self.name} Terminating receiver')
            self.direct_receiver.terminate()
            print('Disconnecting Messaging Service')
            self.messaging_service.disconnect()

    def close(self):
        if Verbose > 0 :
            print('Disconnecting Messaging Service')
        self.messaging_service.disconnect()


class TopicSubscriber (threading.Thread) :
    ''' Solace topic subscriber implementation '''

    def __init__(self):
        ''' Constructor. '''
        #threading.Thread.__init__(self)
        super(TopicSubscriber, self).__init__()
        #super().__init__()
        self._stop = threading.Event()


    def run(self):
        if Verbose > 0:
           print ( f'{T()}: {self.getName()} starting thread')
        cfg_b = Cfg['broker']
        cfg_s = Cfg['subscriber']
        self.sol = SolaceBroker (cfg_b['url'], cfg_b['vpn'],
                                cfg_b['client']['username'], 
                                cfg_b['client']['password'],
                                self.getName())
        try :
            self.sol.connect()
            self.sol.topic_subscriber(cfg_s['topics'])
        except Exception as e:
            print(f'Unexpected error in TopicSubscriber\n{e} ({sys.exc_info()[0]}')
            print(traceback.format_exc())
            self.stop() 

    def stopped(self):
            return self._stop.isSet()

    def stop(self):
        if self.stopped() :
            print (f'Subcriber thread {self.getName()} is not running')
            return
        print (f'{T()}: Stoping subscriber thread {self.getName()}')
        if Verbose > 0 :
            print('Terminating subscriber')
        self.sol.direct_receiver.terminate()
        self.sol.close()
        self._stop.set()
        #os.kill(os.getpid(), signal.SIGINT)

class Main:
    def __init__(self):
        self.threads = []

    def start_subscribers(self, b) :
        print (f"{T()}: Starting subscribers")
        cfg_s = Cfg['subscriber']

        # start another subscriber with some topics
        for i in range (cfg_s['num-clients']):
            sub1 = TopicSubscriber()
            sub1.setName('Subscriber-{}'.format(i+1))
            sub1.start()
            self.threads.append(sub1)
            time.sleep(cfg_s['delay'])

    def stop_threads(self) :
        print (f"{T()}: Stoping all threads")
        for t in self.threads:
            t.stop()
            #t.join()

class YamlHandler():
    """ YAML handling functions """

    def __init__(self):
        if Verbose :
            print ('Entering {}::{}'.format(__class__.__name__, inspect.stack()[0][3]))

    def read_file(self, file):
        """ read yaml file and return data """
        if Verbose > 0 :
            print ("Entering {}::{}  file: {}".format(__class__.__name__, inspect.stack()[0][3], file))

        with open(file, "r") as fp:
            data = yaml.safe_load(fp)
        return data

def main(argv):
    global Cfg, SysCfg, Verbose

    p = argparse.ArgumentParser()
    p.add_argument('--configfile', dest="config_file", required=True, help='config file')
    p.add_argument( '--verbose', '-v', action="count",  required=False, default=0,
                help='Turn Verbose. Use -vvv to be very verbose')
    r = p.parse_args()
    Verbose = r.verbose

    print (f"Reading user config {r.config_file}")

    yaml_h = YamlHandler()
    Cfg = yaml_h.read_file(r.config_file)
    if Verbose > 2:
        print ('CONFIG'); pp.pprint (Cfg)

    m = Main()
    m.start_subscribers(Cfg)

if __name__ == "__main__":
   main(sys.argv[1:])
