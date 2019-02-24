import datetime


def create_object_data(user, **kwargs):
    object_data = {
        'object_id': user.id_str,
        'full_name': user.name,
        'community': 'Twitter',
    }

    return dict(**object_data, **kwargs)


def create_social_data(status, include_id=False, **kwargs):
    if hasattr(status, 'extended_tweet'):
        message = status.extended_tweet['full_text']
    elif hasattr(status, 'full_text'):
        message = status.full_text
    elif hasattr(status, 'text'):
        message = status.text
    else:
        raise Exception('no message field found')

    social_data = {
        'status_id': status.id_str,
        'like': status.favorite_count,
        'retweet': status.retweet_count,
        'message': message,
        'create_date': status.created_at,
        'scrape_date': datetime.datetime.now(),
        'user_id': status.author.id_str,
        'link': f'https://twitter.com/statuses/{status.id_str}',
    }

    if include_id:
        social_data['status_id'] = status.id_str

    return dict(**social_data, **kwargs)


def create_tagged_data(status, include_id=False, **kwargs):
    tagged_data = []

    for tag_number, mention in enumerate(status.entities.get('user_mentions', [])):
        tagged_datum = {
            'tag_number': tag_number,
            'offset': mention['indices'][0],
            'length': len(mention['name'])+1,
            'type': 'user',
            'tagger_id': status.author.id_str,
            'tagged_id': mention['id_str'],
        }

        if include_id:
            tagged_datum['status_id'] = status.id_str

        tagged_datum.update(kwargs)

        tagged_data.append(tagged_datum)

    return tagged_data


def create_retweet_data(status, **kwargs):
    retweet_data = {
        'retweeted_status_id': status.retweeted_status.id_str,
        'status_id': status.retweeted_status.id_str,
    }

    return dict(**retweet_data, **kwargs)
