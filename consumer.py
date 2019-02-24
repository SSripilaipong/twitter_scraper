import tweepy as twp

from connection import get_mongo_db, get_tweepy_auth
from extract import create_object_data, create_social_data, create_tagged_data


def export_to_db(data, db, table_name):
    if table_name == 'object':
        condition = {'object_id': data['object_id'],
                     'community': data['community']}

    elif table_name == 'social':
        condition = {'topic_id': data['topic_id'],
                     'comment_id': data['comment_id'],
                     'reply_id': data['reply_id']}

    elif table_name == 'tagged':
        condition = {'topic_id': data['topic_id'],
                     'comment_id': data['comment_id'],
                     'reply_id': data['reply_id'],
                     'tag_number': data['tag_number']}
    else:
        raise NotImplementedError('table name is not recognised')

    collection = db.get_collection(table_name)
    query = collection.find_one(condition)

    if query is None:
        collection.insert_one(data)
    else:
        collection.replace_one(condition, data)


def export_process_function(queue_export):
    db = get_mongo_db()

    while True:
        table_name, data = queue_export.get()
        export_to_db(data, db, table_name)


def handle_reply(db, status, queue_export):
    social = db.get_collection('social')

    if status.in_reply_to_status_id_str:
        query = social.find_one({'$or': [
            {'topic_id': status.in_reply_to_status_id_str},
            {'comment_id': status.in_reply_to_status_id_str},
            {'reply_id': status.in_reply_to_status_id_str},
        ]})

        if query is not None:
            topic_id = query['topic_id']
            comment_id = query['reply_id'] or query['comment_id']
            reply_id = status.id_str

        else:
            auth = get_tweepy_auth()
            api = twp.API(auth)

            parent = api.get_status(status.in_reply_to_status_id_str, tweet_mode='extended')
            topic_id, comment_id = handle_reply(db, parent, queue_export)
            reply_id = status.id_str

        if comment_id is None:
            comment_id = status.id_str
            reply_id = None

    else:
        topic_id = status.id_str
        comment_id, reply_id = None, None

    object_data = create_object_data(status.author)
    queue_export.put(('object', object_data))

    social_data = create_social_data(status,
                                     topic_id=topic_id,
                                     comment_id=comment_id, reply_id=reply_id)
    queue_export.put(('social', social_data))

    tagged_data = create_tagged_data(status,
                                     topic_id=topic_id,
                                     comment_id=comment_id, reply_id=reply_id)
    for tagged_datum in tagged_data:
        queue_export.put(('tagged', tagged_datum))

    return topic_id, status.id_str if status.in_reply_to_status_id_str else None


def handle_reply_process_function(queue_export, queue_handle_reply):
    db = get_mongo_db()

    while True:
        status = queue_handle_reply.get()
        handle_reply(db, status, queue_export)
