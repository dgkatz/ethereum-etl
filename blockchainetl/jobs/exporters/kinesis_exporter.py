import collections
import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter


class KinesisItemExporter:

    def __init__(self, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.kinesis_client = boto3.client("kinesis")

    def open(self):
        pass

    def export_items(self, items):
        with ThreadPoolExecutor() as pool:
            for kinesis_response in pool.map(self.export_item, items):
                logging.debug(f"Kinesis export item response: {kinesis_response}")

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            data = json.dumps(item).encode('utf-8')
            return self.kinesis_client.put_record(
                StreamName=self.item_type_to_topic_mapping[item_type],
                Data=data,
                PartitionKey=str(uuid.uuid4()),
            )
        else:
            logging.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        pass


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
