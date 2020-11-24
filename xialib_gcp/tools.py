import logging
from google.cloud import firestore_admin_v1


def init_firestore_topic(project_id, topic_id):
    """
    Admin Client Reference:
    API: https://googleapis.dev/python/firestore/latest/admin_client.html
    Protocol: https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.admin.v1
    """
    dbadmin = firestore_admin_v1.FirestoreAdminClient()
    parent = '/'.join(['projects', project_id, 'databases', '(default)', 'collectionGroups', topic_id])
    data_path = '/'.join([parent, 'fields', 'data'])
    asc_index = {"name": topic_id + "-idx-asc", "query_scope": "COLLECTION",
                 "fields": [{"field_path": "table_id", "order": "ASCENDING"},
                            {"field_path": "filter_key", "order": "ASCENDING"},
                            {"field_path": "sort_key", "order": "ASCENDING"}]}
    desc_index = {"name": topic_id + "idx-desc", "query_scope": "COLLECTION",
                  "fields": [{"field_path": "table_id", "order": "ASCENDING"},
                             {"field_path": "filter_key", "order": "ASCENDING"},
                             {"field_path": "sort_key", "order": "DESCENDING"}]}
    data_field = {"name": data_path, "index_config": {"indexes": []}}
    create_asc_index = dbadmin.create_index(parent=parent, index=asc_index)
    logging.info("{}: Ascending Index Creation".format(topic_id))
    create_desc_index = dbadmin.create_index(parent=parent, index=desc_index)
    logging.info("{}: Descending Index Creation".format(topic_id))
    update_field = dbadmin.update_field(field=data_field)
    logging.info("{}: Removing Index of field Data".format(topic_id))
