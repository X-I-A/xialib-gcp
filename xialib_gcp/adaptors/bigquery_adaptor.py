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

    def _escape_column_name(self, old_name: str) -> str:
        """A column name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_),
        and it must start with a letter or underscore. The maximum column name length is 128 characters.
        A column name cannot use any of the following prefixes: _TABLE_, _FILE_, _PARTITION
        """
        better_name = old_name.translate({ord(c): "_" for c in r"!@#$%^&*()[]{};:,./<>?\|`~-=+"})
        if better_name[0].isdigit():
            better_name = '_' + better_name
        if better_name.upper().startswith('_TABLE_') or \
            better_name.upper().startswith('_FILE_') or \
            better_name.upper().startswith('_PARTITION'):
            better_name = '_' + better_name
        if len(better_name) > 128:
            better_name = better_name[:128]
        return better_name

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
            schema_field = {'name': self._escape_column_name(field['field_name']),
                            'description': field.get('description', '')}
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


    def create_table(self, source_id: str, meta_data: dict, field_data: List[dict],
                     raw_flag: bool = False, table_id: str = None):
        if table_id is None:
            table_id = source_id
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

    def drop_table(self, source_id: str):
        table_id = source_id
        try:
            self.connection.delete_table(self._get_table_id(table_id), not_found_ok=True, timeout=30)
        except Exception as e:  # pragma: no cover
            return False  # pragma: no cover

    def rename_table(self, source_id: str, new_table_id: str):
        old_table_id = source_id
        self.drop_table(new_table_id)
        job = self.connection.copy_table(self._get_table_id(old_table_id),
                                         self._get_table_id(new_table_id),
                                         timeout=60)
        job.result()
        # self.drop_table(old_table_id)
        return True

    def get_ctrl_info(self, source_id):
        table_id = source_id
        # Bigquery doesn't need ctrl info to build operation queries, neither for sequence control
        return {'SOURCE_ID': source_id, 'TABLE_ID': table_id, 'META_DATA': dict(), 'FIELD_LIST': [{}]}

    def set_ctrl_info(self, source_id: str, **kwargs):
        return True

    def insert_raw_data(self, log_table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        table_id = log_table_id
        new_data = [{self._escape_column_name(k): v for k, v in line.items()} for line in data]
        try:
            errors = self.connection.insert_rows_json(self._get_table_id(table_id), new_data)
        except BadRequest as e:  # pragma: no cover
            return False  # pragma: no cover
        if errors == []:
            return True
        else:  # pragma: no cover
            self.logger.error("Insert {} Error: {}".format(table_id, errors), extra=self.log_context)
            return False

    def load_log_data(self, source_id: str, start_age: int = None, end_age: int = None):
        return True

    def upsert_data(self,
                    table_id: str,
                    field_data: List[dict],
                    data: List[dict],
                    replay_safe: bool = False,
                    **kwargs):
        return self.insert_raw_data(table_id, field_data, data)

    def alter_column(self, table_id: str, field_line: dict) -> bool:
        return True  # pragma: no cover
