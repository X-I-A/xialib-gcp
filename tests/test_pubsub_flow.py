import os
import json
import base64
import gzip
import asyncio
import pytest
from google.cloud import pubsub_v1
from xialib_gcp import PubsubSubscriber, PubsubPublisher


project_id = 'x-i-a-test'
topic1 = 'xialib-topic-01'
topic2 = 'xialib-topic-02'
subscription1 = 'xialib-sub-01'
subscription2 = 'xialib-sub-02'
header_1 = {'age': 2, 'data_format': 'record', 'data_spec': 'x-i-a', 'data_encode': 'gzip', 'data_store': 'body'}


def callback(s: PubsubSubscriber, message: dict, source, subscription_id):
    header, data, id = s.unpack_message(message)
    assert int(header['age']) == 2


@pytest.fixture(scope='module')
def publisher():
    pub_client = pubsub_v1.PublisherClient()
    publisher = PubsubPublisher(pub_client=pub_client)
    yield publisher


def test_publish_and_pull(publisher: PubsubPublisher):
    publisher.publish(project_id, topic1, header_1, gzip.compress(b'[]'))
    subscriber = PubsubSubscriber(sub_client=pubsub_v1.SubscriberClient())
    for message in subscriber.pull(project_id, subscription1):
        header, data, id = subscriber.unpack_message(message)
        assert int(header['age']) == 2
        subscriber.ack(project_id, subscription1, id)

def test_check_destination(publisher: PubsubPublisher):
    assert publisher.check_destination(project_id, topic1)
    assert not publisher.check_destination(project_id, 'error_topic')

def test_publish_and_streaming_pull(publisher: PubsubPublisher):
    publisher.publish(project_id, topic1, header_1, gzip.compress(b'[]'))
    publisher.publish(project_id, topic2, header_1, gzip.compress(b'[]'))
    loop = asyncio.get_event_loop()
    subscriber1 = PubsubSubscriber(sub_client=pubsub_v1.SubscriberClient())
    subscriber2 = PubsubSubscriber(sub_client=pubsub_v1.SubscriberClient())
    task1 = subscriber1.stream(project_id, subscription1, callback=callback, timeout=2)
    task2 = subscriber2.stream(project_id, subscription2, callback=callback, timeout=2)
    loop.run_until_complete(asyncio.wait([task1, task2]))
    loop.close()


def test_exceptions():
    with pytest.raises(TypeError):
        sub = PubsubSubscriber(sub_client=object())
    with pytest.raises(TypeError):
        pub = PubsubPublisher(pub_client=object())
