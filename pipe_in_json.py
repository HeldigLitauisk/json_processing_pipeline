#!/usr/bin/python3

import gzip
import json
import time
import requests
import uuid
import jsonlines
import argparse

SAMPLE_URL = 'https://raw.githubusercontent.com' \
              '/GetLinkfire/python_engineer_task/master/data.json.gz'
EXCHANGE_RATES = 'https://raw.githubusercontent.com' \
                 '/GetLinkfire/python_engineer_task/master/rates.json'


def get_gzip(url):
    content = requests.get(url).content
    return gzip.decompress(content).splitlines()


def process_url(url):
    """

    :param url: url of zipped json file
    :return: yields json object
    """
    for obj in get_gzip(url):
        for line in jsonlines.Reader(obj.splitlines()):
            yield line


def process_json(path):
    """

    :param path: path to extracted json file
    :return: yields json object
    """
    with jsonlines.open(path) as reader:
        for line in reader:
            yield line


class Processor:
    """
    Abstract processor for future processors
    Each object which inherits from this class must implement process method
    """
    def process(self, *args, **kwargs):
        raise NotImplementedError

    def close(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def is_valid_uuid(uuid_to_test):
        try:
            return uuid.UUID(uuid_to_test)
        except ValueError:
            return False


class ExchangeProcessor(Processor):
    """
    Exchange processor to convert currencies into given currencies
    """
    def __init__(self, currency='USD'):
        """

        :param currency: currently valid options USD, JPY, DKK, EUR
        """
        self.ex_rates = self.get_rates()
        self.default_currency = currency
        self.currency_key = 'conv{}value'.format(currency.lower())

    @staticmethod
    def get_rates(url=EXCHANGE_RATES):
        """ Exchange rates dict relative to default currency. Currently: USD

        :param url: json url to load currency rates from
        :return: currency dict object
        """
        response = requests.get(url)
        return json.loads(response.content)

    def process(self, data):
        if data.get('convvalue', None) and data.get('convvalueunit', None) != self.default_currency:
            data[self.currency_key] = self.ex_rates[data['convvalueunit']] * data['convvalue']

    def close(self):
        pass


class ValidationProcessor(Processor):
    """
    Processor for outputting json rows with invalid linkid
    """

    def __init__(self, errors_output="./deadletters.json"):
        self.errors_output = errors_output
        self.errors_logger = jsonlines.open(errors_output, mode='a')

    def process(self, data):
        if not data.get('linkid', None) or not self.is_valid_uuid(data['linkid']):
            self.errors_logger.write(data)

    def close(self):
        self.errors_logger.close()


class SplittingProcessor(Processor):
    """
    Processor to split data into group based on type and output json lines
    """

    def __init__(self, output_path="./{type}.json", strict_uuid=True):
        """

        :param output_path: path to to output json. Must keep {type} to work.
        :param strict_uuid: if True only rows with valid uuid will be written
        """
        self.output_path = output_path
        self._cached_writers = {}
        self.strict_uuid = strict_uuid

    def get_writer(self, writer_type):
        """ Stores json writer object for given type into dict

        :param writer_type: any type for generating json file paths
        :return: gets or creates jsonlines writer object for given type
        """
        if writer_type in self._cached_writers:
            return self._cached_writers[writer_type]
        writer = jsonlines.open(
            self.output_path.format(type=writer_type), mode='a')
        self._cached_writers[writer_type] = writer
        return self._cached_writers[writer_type]

    def close(self):
        for writer in self._cached_writers:
            self._cached_writers[writer].close()

    def process(self, data):
        if data.get('type', None):
            if not self.strict_uuid or data.get('linkid', None) and self.is_valid_uuid(data['linkid']):
                self.get_writer(data['type']).write(data)


if __name__ == "__main__":
    """
    If neither path nor url is provided will use SAMPLE_URL by default
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', help='path to extracted json lines file')
    parser.add_argument('--url', default=SAMPLE_URL, help='gzip json file url')
    args = parser.parse_args()

    if args.path:
        stream = process_json(path=args.path)
    else:
        stream = process_url(url=args.url)

    # Creates processors and adds them to pipeline.
    exchanger = ExchangeProcessor()
    validator = ValidationProcessor()
    splitter = SplittingProcessor()
    pipelines = [exchanger, validator, splitter]

    rows_counter = 0
    start = time.time()
    while True:
        try:
            raw_data = stream.__next__()
            for pipe in pipelines:
                pipe.process(raw_data)
            rows_counter += 1
        except StopIteration:
            for pipe in pipelines:
                pipe.close()
            break
    end = time.time()
    print("Processed {} rows in {} secs".format(rows_counter, (end - start)))

