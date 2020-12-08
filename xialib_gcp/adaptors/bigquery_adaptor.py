import os
import json
import sqlite3
from typing import List
from google.api_core.exceptions import Conflict, BadRequest
from google.cloud import bigquery
from xialib.adaptor import Adaptor

class BigQueryAdaptor(Adaptor):

    _age_field = {'field_name': '_AGE', 'key_flag': False, 'type_chain': ['int', 'ui_8'],
                  'format': None, 'encode': None, 'default': 0}
    _seq_field = {'field_name': '_SEQ', 'key_flag': False, 'type_chain': ['char', 'c_20'],
                  'format': None, 'encode': None, 'default': '0'*20}
    _no_field = {'field_name': '_NO', 'key_flag': False, 'type_chain': ['int', 'ui_8'],
                 'format': None, 'encode': None, 'default': 0}
    _op_field = {'field_name': '_OP', 'key_flag': False, 'type_chain': ['char', 'c_1'],
                 'format': None, 'encode': None, 'default': ''}

    type_dict = {
        'NULL': ['null'],
        'INT64': ['int'],
        'FLOAT64': ['real'],
        'STRING': ['char'],
        'BYTES': ['blob']
    }

    def __init__(self, connection: bigquery.Client, project_id: str, location='EU', **kwargs):
        super().__init__(**kwargs)
        if not isinstance(connection, bigquery.Client):
            self.logger.error("connection must a big-query client", extra=self.log_context)
            raise TypeError("XIA-010005")
        else:
            self.connection = connection

        self.project_id = project_id
        self.location = location


    def _get_field_type(self, type_chain: list):
        for type in reversed(type_chain):
            for key, value in self.type_dict.items():
                if type in value:
                    return key
        self.logger.error("{} Not supported".format(json.dumps(type_chain)), extra=self.log_context)  # pragma: no cover
        raise TypeError("XIA-000020")  # pragma: no cover

    def _get_table_schema(self, field_data: List[dict]) -> List[dict]:
        schema = list()
        for field in field_data:
            schema_field = {'name': field['field_name'], 'description': field.get('description', '')}
            if field.get('key_flag', False):
                schema_field['mode'] = 'REQUIRED'
            schema_field['type'] = self._get_field_type(field['type_chain'])
            schema.append(schema_field.copy())
        return schema

    def _get_dataset_id(self, table_id) -> str:
        dataset_name = table_id.split('.')[2] if table_id.split('.')[2] else 'xia_default'
        dataset_id = '.'.join([self.project_id, dataset_name])
        return dataset_id

    def _get_table_id(self, table_id) -> str:
        dataset_id = self._get_dataset_id(table_id)
        bq_table_id = '.'.join([dataset_id, table_id.split('.')[-1]])
        return bq_table_id

    def create_table(self, table_id: str, meta_data: dict, field_data: List[dict], raw_flag: bool = False):
        dataset = bigquery.Dataset(self._get_dataset_id(table_id))
        dataset.location = self.location
        try:
            dataset = self.connection.create_dataset(dataset, timeout=30)
        except Conflict as e:
            self.logger.info("Dataset already exists, donothing", extra=self.log_context)

        field_list = field_data.copy()
        field_list.append(self._age_field)
        field_list.append(self._seq_field)
        field_list.append(self._no_field)
        field_list.append(self._op_field)
        schema = self._get_table_schema(field_list)
        table = bigquery.Table(self._get_table_id(table_id), schema=schema)
        table = self.connection.create_table(table, True, timeout=30)
        return True if table else False

    def drop_table(self, table_id: str):
        try:
            self.connection.delete_table(self._get_table_id(table_id), not_found_ok=True, timeout=30)
        except Exception as e:  # pragma: no cover
            return False  # pragma: no cover

    def rename_table(self, old_table_id: str, new_table_id: str):
        self.drop_table(new_table_id)
        job = self.connection.copy_table(self._get_table_id(old_table_id),
                                         self._get_table_id(new_table_id),
                                         timeout=60)
        job.result()
        # self.drop_table(old_table_id)
        return True

    def get_ctrl_info(self, table_id):
        # Bigquery doesn't need ctrl info to build operation queries, neither for sequence control
        return {'TABLE_ID': table_id, 'META_DATA': dict(), 'FIELD_LIST': [{}]}

    def set_ctrl_info(self, table_id, **kwargs):
        return True

    def insert_raw_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs) -> bool:
        try:
            errors = self.connection.insert_rows_json(self._get_table_id(table_id), data)
        except BadRequest as e:
            return False
        if errors == []:
            return True
        else:  # pragma: no cover
            self.logger.error("Insert {} Error: {}".format(table_id, errors), extra=self.log_context)
            return False

    def load_raw_data(self, raw_table_id: str, tar_table_id: str, field_data: List[dict]):
        return self.rename_table(raw_table_id, tar_table_id)

    def upsert_data(self,
                    table_id: str,
                    field_data: List[dict],
                    data: List[dict],
                    replay_safe: bool = False,
                    **kwargs):
        return self.insert_raw_data(table_id, field_data, data)

    def alter_column(self, table_id: str, field_line: dict) -> bool:
        raise NotImplementedError  # pragma: no cover