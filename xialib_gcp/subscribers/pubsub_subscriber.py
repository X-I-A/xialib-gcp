import os
import json
import datetime
import asyncio
from functools import partial
from google.cloud import pubsub_v1
from xialib.subscriber import Subscriber
from typing import Callable


class PubsubSubscriber(Subscriber):
    """Google Pubsub based subscriber

    """
    def __init__(self, sub_client: pubsub_v1.SubscriberClient):
        super().__init__()
        if not isinstance(sub_client, pubsub_v1.SubscriberClient):
            self.logger.error("sub_client must be type of Pubsub Subscriber Client")
            raise TypeError("XIA-010002")
        else:
            self.subscriber = sub_client

    def pull(self, project_id: str, subscription_id: str):
        subscription_path = self.subscriber.subscription_path(project_id, subscription_id)
        while True:
            counter = 0
            response = self.subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 8}
            )
            for received_message in response.received_messages:
                counter += 1
                return_message = {'header': received_message.message.attributes,
                                  'data': received_message.message.data,
                                  'id': received_message.ack_id}
                yield return_message
            if counter < 8:
                self.subscriber.close()
                break

    async def stream(self, project_id: str, subscription_id: str, callback: Callable, timeout: int = None):
        def pubsub_callback(message, subscriber: Subscriber, project_id: str, subscription_id: str,
                            custom_callback: Callable):
            return_message = {'header': message.attributes,
                              'data': message.data,
                              'id': message.ack_id}
            custom_callback(subscriber, return_message, project_id, subscription_id)
        subscription_path = self.subscriber.subscription_path(project_id, subscription_id)
        prepared_callback = partial(pubsub_callback,
                                    subscriber=self,
                                    project_id=project_id,
                                    subscription_id=subscription_id,
                                    custom_callback=callback)
        streaming_pull_future = self.subscriber.subscribe(subscription_path, callback=prepared_callback)
        self.logger.warning("Listening on {}".format(subscription_path))
        total_time = 0
        while timeout is None or total_time < timeout:
            await asyncio.sleep(1)
            total_time += 1
        self.subscriber.close()
        streaming_pull_future.cancel()

    def ack(self, project_id: str, subscription_id: str, message_id: str):
        subscription_path = self.subscriber.subscription_path(project_id, subscription_id)
        self.subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": [message_id]}
        )
