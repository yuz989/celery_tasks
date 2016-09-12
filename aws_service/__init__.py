# -.- coding: utf-8 -.-

import json


class AWSService:

    def __init__(self, access_key, secret_key, region='ap-southeast-1'):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region     = region

        self.connection = AWSService._connect(access_key, secret_key, region)


    @staticmethod
    def _connect(access_key, secret_key, region):
        from boto.sns import connect_to_region

        try:
            connection = connect_to_region(
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key)
        except:
            raise Exception('AWS auth failed')

        return connection

    def send_sns(self, message, cat, arn, topic=''):
        message = {
            "default": message,
            "GCM": json.dumps({
                "data": {
                    "alert": message
                }
            })
        }

        if cat == 'endpoint':
            publication = self.connection.publish(message=json.dumps(message), target_arn=arn, message_structure='json')
        else:
            publication = self.connection.publish(message=json.dumps(message), topic=topic,message_structure='json')

        return publication

