import multiprocessing as mp
import tweepy as twp

from connection import get_tweepy_auth
from listener import MyStreamListener
from consumer import export_process_function, handle_reply_process_function


def main():
    queue_export, queue_handle_reply = mp.Queue(), mp.Queue()
    export_process = mp.Process(target=export_process_function, args=(queue_export,))
    export_process.start()

    handle_reply_process = mp.Process(target=handle_reply_process_function, args=(queue_export, queue_handle_reply,))
    handle_reply_process.start()

    auth = get_tweepy_auth()

    listener = MyStreamListener(queue_export, queue_handle_reply)

    stream = twp.Stream(auth, listener)
    stream.filter(track=['bnk48'])


if __name__ == '__main__':
    main()
