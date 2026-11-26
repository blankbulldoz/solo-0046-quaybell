import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'
# 批次引用既可能是服务端数字编号，也可能是调用方提供的幂等字符串键。
REF = r'([A-Za-z0-9_.:-]{1,96})'
BATCH_COLL = CHANNEL + r'/(?:batches|batch|intakes|uploads|multipart-uploads|lots|shipments|collections|manifests)'
BATCH_INIT = BATCH_COLL + r'/(?:initiate|init|start|begin|open|create|new)'
BATCH_ITEM = BATCH_COLL + r'/' + REF
BATCH_PARTS = BATCH_ITEM + r'/(?:parts|part|shards|shard|chunks|chunk|segments|segment|pieces|piece|upload|uploads)'
BATCH_PART_AT = BATCH_PARTS + r'/' + REF
BATCH_ACTION = BATCH_ITEM + r'/(?:seal|sealed|complete|completion|commit|finalize|finish|close)'
BATCH_CANCEL = BATCH_ITEM + r'/(?:cancel|cancellation|abort|discard|revoke)'
BATCH_RECEIPT = BATCH_ITEM + r'/(?:receipt|receipts|status|state)'
GLOBAL_BATCH = r'/v1/(?:batches|intakes|uploads)/' + REF
GLOBAL_BATCH_PARTS = GLOBAL_BATCH + r'/(?:parts|part|shards|shard|chunks|chunk|segments|segment|pieces|piece)'
GLOBAL_BATCH_PART_AT = GLOBAL_BATCH_PARTS + r'/' + REF
GLOBAL_BATCH_ACTION = GLOBAL_BATCH + r'/(?:seal|complete|commit|finish|close|finalize)'
GLOBAL_BATCH_CANCEL = GLOBAL_BATCH + r'/(?:cancel|abort|discard|revoke)'
GLOBAL_BATCH_RECEIPT = GLOBAL_BATCH + r'/(?:receipt|receipts|status|state)'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
        (BATCH_COLL, 'batch_list'), (BATCH_RECEIPT, 'batch_receipt'),
        (BATCH_PART_AT, 'batch_part_get'), (BATCH_PARTS, 'batch_parts'),
        (BATCH_ITEM, 'batch_get'),
        (GLOBAL_BATCH_RECEIPT, 'batch_receipt_global'),
        (GLOBAL_BATCH_PART_AT, 'batch_part_get_global'), (GLOBAL_BATCH_PARTS, 'batch_parts_global'),
        (GLOBAL_BATCH, 'batch_get_global'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP + '/pause', 'pause'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
        ('/v1/sweep', 'sweep'), (DELIVERY + '/redrive', 'redrive'), (CHANNEL + '/retention', 'prune'),
        (BATCH_COLL, 'batch_create'), (BATCH_INIT, 'batch_create'),
        (BATCH_PART_AT, 'batch_part_at'),
        (BATCH_PARTS, 'batch_part'), (BATCH_ACTION, 'batch_seal'),
        (BATCH_CANCEL, 'batch_cancel'), (BATCH_RECEIPT, 'batch_receipt_write'),
        (BATCH_ITEM, 'batch_item_op'),
        (GLOBAL_BATCH_PART_AT, 'batch_part_at_global'), (GLOBAL_BATCH_PARTS, 'batch_part_global'),
        (GLOBAL_BATCH_ACTION, 'batch_seal_global'), (GLOBAL_BATCH_CANCEL, 'batch_cancel_global'),
        (GLOBAL_BATCH, 'batch_item_op_global'),
    ),
    'PUT': (
        (BATCH_PART_AT, 'batch_part_at'), (BATCH_PARTS, 'batch_part'),
        (BATCH_ITEM, 'batch_put'),
        (GLOBAL_BATCH_PART_AT, 'batch_part_at_global'), (GLOBAL_BATCH_PARTS, 'batch_part_global'),
        (GLOBAL_BATCH, 'batch_put_global'),
    ),
    'PATCH': (
        (BATCH_PART_AT, 'batch_part_at'), (BATCH_PARTS, 'batch_part'),
        (BATCH_ITEM, 'batch_put'),
    ),
    'DELETE': (
        (BATCH_CANCEL, 'batch_cancel'), (BATCH_ITEM, 'batch_cancel'),
        (GLOBAL_BATCH_CANCEL, 'batch_cancel_global'), (GLOBAL_BATCH, 'batch_cancel_global'),
    ),
}
ROUTES = {method: [(re.compile(pattern), action) for pattern, action in patterns]
          for method, patterns in PATTERNS.items()}


def match(method, path):
    for pattern, action in ROUTES.get(method, []):
        result = pattern.fullmatch(path)
        if result:
            return action, result.groups()
    return None
