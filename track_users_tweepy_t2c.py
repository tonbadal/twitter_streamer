#!/usr/bin/env python3

import os
import json
from datetime import date
import gzip
import shutil
from queue import Queue
import smtplib
from email.mime.text import MIMEText
from threading import Event, Thread
import logging

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream, API, TweepError


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


NUM_THREADS = 10

class UserTracker:

    def __init__(self):
        # Copy here the keys
        CONSUMER_KEY = ''
        CONSUMER_SECRET = ''
        ACCESS_TOKEN = ''
        ACCESS_TOKEN_SECRET = ''
        
        self.twitter_auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        self.twitter_auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

        self.api = API(self.twitter_auth)
        self.my_listener = None

    def get_users(self, users_file):
        """Read the users names in the given file, and returns an array of strings with the IDs of these users."""
        f = open(users_file, 'r')
        users = []
        for l in f.readlines():
            if l[0] == '@':
                print(l)
                try:
                    user = self.api.get_user(screen_name = l)
                    users.append(str(user.id))
                except TweepError:
                    print('ERROR : User ' + l + ' not found')
        f.close()
        return users

    def start_stream(self, users, callback):
        """Track the users and store all their tweets into output_{date}.txt
        """

        self.my_listener = MyListener(callback=callback)
        stream = Stream(auth=self.twitter_auth, listener=self.my_listener)
        stream.filter(follow=users, stall_warnings=True)

        if self.my_listener and self.my_listener.get_error_status():
            raise Exception("Twitter API error: %s" % self.my_listener.get_error_status())


    def stop_stream(self):
        """Stops the current stream."""

        if not self.my_listener:
            logging.warning("No stream to stop.")
            return

        logging.debug("Stopping stream.")
        self.my_listener.stop_queue()
        self.my_listener = None


class MyListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
        This is a basic listener that just prints received tweets to stdout.
        """
    def __init__(self, callback):
        self.callback = callback
        self.error_status = None
        self.start_queue()

        # Call superclass constructor
        super(StreamListener, self).__init__()

    def start_queue(self):
        """Creates a queue and starts the worker threads."""
        self.queue = Queue()
        self.stop_event = Event()
        logging.debug("Starting %s worker threads." % NUM_THREADS)
        self.workers = []
        for worker_id in range(NUM_THREADS):
            worker = Thread(target=self.process_queue, args=[worker_id])
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def stop_queue(self):
        """Shuts down the worker threads."""
        if not self.workers:
            logging.warning("No worker threads to stop.")
            return

        self.stop_event.set()
        for worker in self.workers:
            # Terminate the thread immediately.
            worker.join(0)

    def on_data(self, data):
        """Puts a task to process the new data on the queue."""

        if self.stop_event.is_set():
            return False

        # Put the task on the queue and keep streaming.
        self.queue.put(data)
        return True

    def process_queue(self, worker_id):
        """Continuously processes tasks on the queue."""

        # Create a new logs instance (with its own httplib2 instance) so that
        # there is a separate one for each thread.

        logging.debug("Started worker thread: %s" % worker_id)
        while not self.stop_event.is_set():
            # The main loop doesn't catch and report exceptions from background
            # threads, so do that here.
            try:
                size = self.queue.qsize()
                logging.debug("Processing queue of size: %s" % size)
                data = self.queue.get(block=True)
                self.handle_data(data)
                self.queue.task_done()
            except Exception:
                pass
                logging.error('EXCEPTION', exc_info=True)
        logging.debug("Stopped worker thread: %s" % worker_id)

    def handle_data(self, data):
        """Sanity-checks and extracts the data before sending it to the
        callback.
        """

        try:
            data = json.dumps(data)
            tweet = json.loads(data)
        except ValueError:
            logging.error("Failed to decode JSON data: %s" % data)
            return

        self.callback(tweet)

    def on_error(self, status):
        print(status)


class MyCallback:
    """ A Callback function that gets the data and writes it to file.
        """
    def __init__(self, output_path=None):
        """ Initialize variables and open file to write. Name format:

            <output_path>/output_<date>.txt

        """
        self.today = date.today()
        self.file_name = output_path + 'output_' + self.today.isoformat() + '.txt'
        self.f = open(self.file_name, 'w')
        logging.info('File ' + self.file_name + ' created.')

    def tweet2file(self, line):
        """ Write the tweet to the file, and if the date changes compress old file,
        delete uncompressed file and open a new one with current date"""

        if line:
            self.f.write(line.encode('utf-8'))

        if self.today != date.today():
            logging.info('Compressing file ' + self.file_name + '...')
            with open(self.file_name, 'r') as self.f_in, gzip.open(self.file_name + '.gz', 'w') as self.f_out:
                shutil.copyfileobj(self.f_in, self.f_out)
            logging.info('Deleting file ' + self.file_name + '...')
            os.remove(self.file_name)

            self.__init__()

class EmailSender:
    """ Email Sender. In case that the script crashes, it sends an email.
        """
    def __init__(self):
        email = 'example@example.com'
        self.send_from = email
        self.send_to = email
        
    def sendWarning(self):
        content = 'Check track_users_tweepy_t2c.py execution. It may have failed.'
        msg = MIMEText(content)

        # me == the sender's email address
        # you == the recipient's email address
        msg['Subject'] = 'Python execution quited'
        msg['From'] = self.send_from
        msg['To'] = self.send_to

        # This will work only if you have a configured email service in your computer
        s = smtplib.SMTP('localhost')
        s.sendmail(self.send_from, [self.send_to], msg.as_string())
        s.quit()

if __name__ == '__main__':

    while True:
        logging.info("Starting new session.")

        # Get users operation
        tracker = UserTracker()
        users_file_path = 'users.txt'
        users = tracker.get_users(users_file=users_file_path)
        logging.info("GetUsers operation completed.")

        # Initialize Callback with the output path where the data will be stored
        output_path = 'data/'
        my_callback = MyCallback(output_path)

        try:
            # Start Streaming users
            tracker.start_stream(users, callback=my_callback.tweet2file)
        except Exception:
            logging.error('EXCEPTION', exc_info=True)
        finally:
            # Send warning by e-mail
            emailSender = EmailSender()
            emailSender.sendWarning()
            logging.info("Warning e-mail sent.")
            
            # Close the files
            logging.info("Ending session.")
            tracker.stop_stream()
            logging.info("Closing files.")
            tracker.f.close()
            tracker.f_in.close()
            tracker.f_out.close()
            

