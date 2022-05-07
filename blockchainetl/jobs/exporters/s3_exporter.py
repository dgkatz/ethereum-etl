import os

import boto3

from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter
from ethereumetl.enumeration.entity_type import EntityType


class S3ItemExporter:
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = normalize_path(prefix)
        self.s3_client = boto3.resource('s3')

    def open(self):
        pass

    def export_items(self, items, start_block: int, end_block: int):
        file_suffix = f'{start_block}_{end_block}.json'
        file_exporter = CompositeItemExporter(
            filename_mapping={
                entity_type: f'{entity_type}_{file_suffix}'
                for entity_type in EntityType.ALL_FOR_STREAMING
            }
        )
        with file_exporter:
            file_exporter.export_items(items=items, start_block=start_block, end_block=end_block)
        for item_type, file_name in file_exporter.filename_mapping.items():
            counter = file_exporter.counter_mapping[item_type]
            if counter.count > 0:
                destination_blob_name = f'{self.prefix}/{item_type}/{file_suffix}'
                self.s3_client.Object(self.bucket, destination_blob_name).upload_file(file_name)
            os.remove(file_name)

    def close(self):
        pass


def normalize_path(p):
    if p is None:
        p = ''
    p = p.lstrip('/')
    p = p.rstrip("/")
    return p
