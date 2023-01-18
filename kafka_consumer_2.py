import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


API_KEY = '33W4NMD6546SH5KA'
ENDPOINT_SCHEMA_URL  = 'https://psrc-68gz8.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'mdzp5Oujrood53ruVLUxiEDy1ifUpwMGn6JYieZu+v5H4fk6GwydN1GoaNrHN0TP'
BOOTSTRAP_SERVER = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'TZE4I6CLZXTGI7QC'
SCHEMA_REGISTRY_API_SECRET = 'x0k3m3uxp0jN5Vfp9tsm2ysxeJ1SaFNoSObD6rRWeV5qqtvPj5J8lel4ZSJq2z8z'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Car:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_car(data:dict,ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_str = """
     {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "Order_Date": {
      "description": "The type(v) type is used.",
      "type": "string",
      "format":"date"
    },
    "Order_Number": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Item_Name": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Quantity": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Product_Price": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Total_products": {
      "description": "The type(v) type is used.",
      "type": "number"
    }  
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Car.dict_to_car)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "latest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    count=0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if car is not None:
                count=count+1
                print("Record count: ",count)
                print("User record {}: car: {}\n"
                      .format(msg.key(), car))
        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurent-take-away-data")