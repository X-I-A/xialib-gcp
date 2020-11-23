from google.cloud import pubsub_v1
from xialib.publisher import Publisher

class PubsubPublisher(Publisher):
    """A local file system based publisher

    Data is saved to path of (destination->topic_id->current_timestamp ).
    all content is dumped to a json file. Because the json do not support blob, the data part is base64 encoded
    """
    blob_support = True

    def __init__(self, pub_client: pubsub_v1.PublisherClient):
        super().__init__()
        if not isinstance(pub_client, pubsub_v1.PublisherClient):
            self.logger.error("pub_client must be type of Pubsub Publisher Client")
            raise TypeError("XIA-010001")
        else:
            self.publisher = pub_client

    def _send(self, project_id: str, topic_id: str, header: dict, data):
        topic_path = self.publisher.topic_path(project_id, topic_id)
        try:
            data = data.encode()
        except (UnicodeEncodeError, AttributeError):
            pass
        future = self.publisher.publish(topic_path, data, **header)
        try:
            message_no = future.result(30)
            self.logger.info("Sent to {}-{}: {}".format(project_id, topic_id, header))
            return message_no
        except TimeoutError as e:  # pragma: no cover
            self.logger.error("Publish Timeout {}-{}: {}".format(project_id, topic_id, header))  # pragma: no cover
