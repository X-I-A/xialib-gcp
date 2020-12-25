import os
import json
import base64
import gzip
from typing import List, Dict, Any, Union, Generator
from google.cloud import firestore
from xialib.depositor import Depositor


class FirestoreDepositor(Depositor):
    data_encode = 'gzip'
    size_limit = 2 ** 20

    def __init__(self, db: firestore.Client):
        super().__init__()
        if not isinstance(db, firestore.Client):
            self.logger.error("FirestoreDepositor db must be type of Firestore Client", extra=self.log_context)
            raise TypeError("XIA-010003")
        else:
            self.db = db

    def _get_filter_key(self, merge_status, merge_level):
        """Get Filter Key for Firestore

        Firestore accept an IN statement of 10. We must cut the request:
            9: Header
            8 Packaged
            7: merged : merge_level >= 3
            4-6: merged: merge level 0-2
            3: initial : merge_level >= 3
            0-2: initial : merge_level 0-2
        """
        if merge_status == 'header':
            return 9
        elif merge_status == 'packaged':
            return 8
        elif merge_status == 'merged':
            return 4 + min(3, merge_level)
        elif merge_status == 'initial':
            return min(3, merge_level)

    def _set_current_topic_table(self, topic_id: str, table_id: str):
        self.topic_id = topic_id
        self.table_id = table_id
        self.topic_object = self.db.collection(topic_id)

    def _add_document(self, header: dict, data: bytes) -> dict:
        content = header.copy()
        content['data'] = data
        content['data_size'] = len(content['data'])
        content['filter_key'] = self._get_filter_key(content['merge_status'], content['merge_level'])
        self.topic_object.add(content)
        return content

    def _update_document(self, ref: firestore.DocumentSnapshot, header: dict, data: bytes):
        content = header.copy()
        content['data'] = data
        content['data_size'] = len(content['data'])
        content['filter_key'] = self._get_filter_key(content['merge_status'], content['merge_level'])
        ref.reference.set(content)
        return content

    def _update_header(self, ref: firestore.DocumentSnapshot, header: dict):
        content = header.copy()
        if content.get('merge_status', None) == 'packaged':
            content['filter_key'] = 8
        doc_content = ref.to_dict()
        for key, value in header.items():
            if key not in doc_content and value != self.DELETE:
                doc_content[key] = value
            elif value == self.DELETE:
                doc_content.pop(key, None)
                content[key] = firestore.DELETE_FIELD
            else:
                doc_content[key] = value
        ref.reference.update(content)
        return doc_content

    def delete_documents(self, ref_list: List[firestore.DocumentSnapshot]):
        for ref in ref_list:
            ref.reference.delete()
        return True

    def get_header_from_ref(self, doc_ref: firestore.DocumentSnapshot) -> dict:
        return doc_ref.to_dict()

    def get_data_from_header(self, header: dict) -> List[dict]:
        return json.loads(gzip.decompress(header['data']).decode())

    def get_ref_by_merge_key(self, merge_key) -> firestore.DocumentSnapshot:
        q = self.topic_object.where('table_id', '==', self.table_id).where('merge_key', '==', merge_key).limit(1)
        for ref in q.stream():
            return ref

    def get_stream_by_sort_key(self,
                               status_list: List[str] = None,
                               le_ge_key: str = None,
                               reverse: bool = False,
                               min_merge_level: int = 0,
                               equal: bool = True):
        white_list = list()
        initial_merge_level_dict = {x: [i for i in range(4) if i >= x] for x in range(4)}
        merged_merge_level_dict = {x + 4: [i + 4 for i in range(4) if i >= x] for x in range(4)}
        if not status_list:
            status_list = ['header', 'initial', 'merged', 'packaged']
        if 'initial' in status_list:
            white_list.extend(initial_merge_level_dict.get(min_merge_level, [3]))
        if 'merged' in status_list:
            white_list.extend(merged_merge_level_dict.get(min_merge_level + 4, [7]))
        if 'packaged' in status_list:
            white_list.append(8)
        if 'header' in status_list:
            white_list.append(9)
        if reverse:
            if not le_ge_key:
                q = self.topic_object.where('table_id', '==', self.table_id) \
                                     .where('filter_key', 'in', white_list) \
                                     .order_by('sort_key', direction=firestore.Query.DESCENDING)
            elif equal:
                q = self.topic_object.where('table_id', '==', self.table_id) \
                    .where('filter_key', 'in', white_list) \
                    .where('sort_key', '<=', le_ge_key).order_by('sort_key', direction=firestore.Query.DESCENDING)
            else:
                q = self.topic_object.where('table_id', '==', self.table_id) \
                    .where('filter_key', 'in', white_list) \
                    .where('sort_key', '<', le_ge_key).order_by('sort_key', direction=firestore.Query.DESCENDING)
        else:
            if not le_ge_key:
                q = self.topic_object.where('table_id', '==', self.table_id) \
                                     .where('filter_key', 'in', white_list) \
                                     .order_by('sort_key')
            elif equal:
                q = self.topic_object.where('table_id', '==', self.table_id) \
                    .where('filter_key', 'in', white_list) \
                    .where('sort_key', '>=', le_ge_key).order_by('sort_key')
            else:
                q = self.topic_object.where('table_id', '==', self.table_id) \
                    .where('filter_key', 'in', white_list) \
                    .where('sort_key', '>', le_ge_key).order_by('sort_key')
        for ref in q.stream():
            # Min_merge_level >= 3 means the furthur filter is necessairy
            if min_merge_level >= 3:
                doc_dict = self.get_header_from_ref(ref)
                if doc_dict['merge_level'] < min_merge_level:
                    continue
            yield ref

    def get_table_header(self) -> firestore.DocumentSnapshot:
        for ref in self.get_stream_by_sort_key(['header'], reverse=True):
            return ref

    def inc_table_header(self, **kwargs):
        header_ref = self.get_table_header()
        header_dict = header_ref.to_dict()
        content = kwargs.copy()
        for key, value in content.items():
            content[key] = firestore.Increment(value)
            header_dict[key] = header_dict.get(key, 0) + value
        self.update_document(header_ref, content)
        return header_dict
