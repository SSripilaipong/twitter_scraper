from tweepy.streaming import StreamListener
from extract import create_tagged_data, create_object_data, create_social_data


class MyStreamListener(StreamListener):
    def __init__(self, queue_export, queue_handle_reply, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.queue_export = queue_export
        self.queue_handle_reply = queue_handle_reply

    def on_status(self, status):
        if hasattr(status, 'retweeted_status'):
            pass

        elif status.is_quote_status:
            pass

        elif status.in_reply_to_status_id_str:
            print('reply', status.id_str)
            self.queue_handle_reply.put(status)

        else:
            object_data = create_object_data(status.author)
            self.queue_export.put(('object', object_data))

            social_data = create_social_data(status,
                                             topic_id=status.id_str,
                                             comment_id=None, reply_id=None)
            self.queue_export.put(('social', social_data))

            tagged_data = create_tagged_data(status,
                                             topic_id=status.id_str,
                                             comment_id=None, reply_id=None)
            for tagged_datum in tagged_data:
                self.queue_export.put(('tagged', tagged_datum))

    def on_error(self, status):
        print(status)
