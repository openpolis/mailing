from datetime import datetime
import json
import time

__author__ = 'guglielmo'

"""
Validate email addresses in a csv file.
The field is ``email`` by default, but it can be specified as an option in the command invocation.

Validation has 3 levels:
* RFC - the form of the address, no connection required
* DOMAIN - an SMTP connection is made, the server exists
* USER - user exists on the SMTP server

Only CSV rows containing valid email addresse are passed.

Usage examples::

     python validate.py --log=logfile data/subscribers.csv > data/clean_subscribers.csv
     python validate.py --email-field=email_address --log=logfile data/subscribers.csv > data/clean_subscribers.csv
"""

import sys
import csv
import zmq
import argparse
import utils
from multiprocessing import Process
from validate_email import validate_email


#
# define parser for arguments and options
#
parser = argparse.ArgumentParser(description='validate email addresses from a csv file')
parser.add_argument("csv")
parser.add_argument(
    '--field', default='email',
    help='the name of the email field in the csv header')
parser.add_argument(
    '--encoding', default='utf8',
    help='the encoding of the input file')
parser.add_argument(
    '--nprocs', default=1, type=int,
    help='the number of parallel minions')
parser.add_argument(
    '--log', default=sys.stdout, type=argparse.FileType('w'),
    help='the file where the log should be written; defaults to stdout')
parser.add_argument(
    '--output', default=sys.stdout, type=argparse.FileType('w'),
    help='the file where the clean csv file should be written; defaults to stdout')
args = parser.parse_args()



def producer(args):

    encoding = args.encoding
    field = args.field
    logger = args.log
    csvfile = open(args.csv, 'rb')

    # zmq context
    context = zmq.Context()

    # binding to PUSH socket
    zmq_socket = context.socket(zmq.PUSH)
    zmq_socket.bind("tcp://127.0.0.1:5557")


    #
    # csv file reader definition
    #
    try:
        csv_reader = utils.UnicodeDictReader(csvfile, delimiter=';', encoding=encoding)
    except IOError:
        logger.write("It was impossible to open file %s\n" % csvfile)
        exit(1)
    except csv.Error, e:
        logger.write("CSV error while reading %s: %s\n" % (csvfile, e.message))


    counter = 0

    #
    # main loop
    #
    for row in csv_reader:
        #
        # increment counter
        #
        counter = csv_reader.reader.line_num - 1
        if counter % 10:
            logger.write(".")
        else:
            logger.write(":")

        if field not in row:
            logger.write("\nInvalid field name: exiting\n")
        logger.flush()

        zmq_socket.send_json(row)

    logger.write("\n======\nAll sent!\n======\n")
    logger.flush()



def collector(args):
    encoding = args.encoding
    outfile = args.output
    logger = args.log
    csvfile = open(args.csv, 'rb')

    # zmq context
    context = zmq.Context()

    # binding to PULL socket
    results_receiver = context.socket(zmq.PULL)
    results_receiver.bind("tcp://127.0.0.1:5558")

    #
    # csv file reader definition
    #
    try:
        csv_reader = utils.UnicodeDictReader(csvfile, delimiter=';', encoding=encoding)
    except IOError:
        logger.write("It was impossible to open file %s\n" % csvfile)
        exit(1)
    except csv.Error, e:
        logger.write("CSV error while reading %s: %s\n" % (csvfile, e.message))

    #
    # csv file writer definition: uses modified dialect in utils.py
    #
    try:
        csv_writer = utils.UnicodeDictWriter(
            outfile,
            fieldnames=csv_reader.reader.fieldnames, dialect= utils.excel_semicolon
        )
    except IOError:
        logger.write("It was impossible to open file %s\n" % outfile)
        exit(1)
    except csv.Error, e:
        logger.write("CSV error while writing to %s: %s\n" % (outfile, e.message))

    logger.write("Starting with %s\n" % csvfile.name)
    logger.flush()

    csv_writer.writeheader()  # first line contains csv header

    while True:
        result = results_receiver.recv_json()
        if result['is_valid']:
            csv_writer.writerow( result['row'] )
            csv_writer.stream.flush()
        else:
            logger.write("{0} \n".format(result))
            logger.flush()


def minion(id=0, field='email'):

    # zmq context and
    context = zmq.Context()

    # receive work
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect("tcp://127.0.0.1:5557")

    # send work
    consumer_sender = context.socket(zmq.PUSH)
    consumer_sender.connect("tcp://127.0.0.1:5558")

    # logger
    logger = open("log/{0}".format(id), 'w')

    while True:
        row = consumer_receiver.recv_json()
        email = row[field]
        if validate_email(email):
            if validate_email(email, check_mx=True):
                if validate_email(email, verify=True):
                    res = {'row': row, 'is_valid': True}
                else:
                    res = {'row': row, 'is_valid': False, 'err': 'KO-USER'}
            else:
                res = {'row': row, 'is_valid': False, 'err': 'KO-MX'}
        else:
            res = {'row': row, 'is_valid': False, 'err': 'KO-RFC'}

        consumer_sender.send_json(res)
        logger.write("{0} - {1}\n".format(datetime.now(), res))
        logger.flush()


if __name__ == "__main__":

    if args.nprocs > 42:
        print "Please!"
        exit()

    Process(target=collector, args=(args,)).start()
    time.sleep(1)

    for i in range(args.nprocs):
        Process(target=minion, args=(i,)).start()

    time.sleep(1)
    Process(target=producer, args=(args,)).start()


