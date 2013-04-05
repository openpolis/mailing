__author__ = 'guglielmo'

"""
Open a csv file, loop through the records and
spits records whose emails are not found within the blacklist
out to an output stream.

Input and ouptut csv streams are stdin and stdout by default,
they can be overriden using the right options

Usage example:
 cat data/iscritti.csv | \
     python remove_blacklist.py --blacklist data/black_list_utenti_openpolis.txt --log=log/logfile \
     > data/clean_iscritti.csv
"""

import sys
import csv
import argparse
import utils


#
# define parser for arguments and options
#
parser = argparse.ArgumentParser(
    description='remove records matching blacklisted email addresses from a csv file')
parser.add_argument(
    '--csv', default=sys.stdin, type=argparse.FileType('r'),
    help='the csv file containing the records with the email')
parser.add_argument(
    '--blacklist', type=argparse.FileType('r'), required=True,
    help='the file containing the list of blacklisted emails, should be encoded as utf8')
parser.add_argument(
    '--encoding', default='utf8',
    help='the encoding of the input file')
parser.add_argument(
    '--log', default=sys.stdout, type=argparse.FileType('w'),
    help='the file where the log should be written; defaults to stdout')
parser.add_argument(
    '--output', default=sys.stdout, type=argparse.FileType('w'),
    help='the file where the clean csv file should be written; defaults to stdout')
args = parser.parse_args()

encoding = args.encoding
outfile = args.output
logger = args.log
csvfile = args.csv
blacklistfile = args.blacklist

#
# read blacklisted email addresses from file
#
blacklisted_emails = blacklistfile.read().splitlines()

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
# csv file writer definition: uses modified dialec in utils.py
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

csv_writer.writeheader() # first line contains csv header

#
# main loop
#
for row in csv_reader:
    #
    # increment counter
    #
    c = csv_reader.reader.line_num - 1

    #
    # log every 1000 lines
    #
    if c % 1000 == 0:
        logger.write( ".. read line %d ..\n" % c )

    #
    # check if email in current row is among blacklisted
    # only write row to ouput csv if not true
    #
    if row['email'] not in blacklisted_emails:
        csv_writer.writerow( row )
    else:
        logger.write("Excluding %s\n" % row['email'])

#
# close all streams
#
logger.close()

