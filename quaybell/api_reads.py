from . import catalog, deadletters, drain, evidence, inspection, overview, retention, routing
from .protocol import Reply

# GET endpoints that perform bounded, idempotent housekeeping (lease reap and
# lazy drain-round transitions) and therefore need a write transaction.
WRITE_GET_ACTIONS = frozenset({'drain_view', 'drain_list', 'drain_detail',
                               'group_view', 'pause_view'})


def group_state(db, channel, group, now):
    from . import subscriptions
    sub = subscriptions.get(db, channel, group)
    report = drain.current_round(db, channel, group, now)
    view = dict(sub)
    if report is not None:
        view['drain'] = report
        view['drain_round'] = report['round']
        view['round'] = report['round']
        view['state'] = report['state']
    else:
        view['drain'] = None
        view['drain_round'] = 0
    return view


def read(db, action, args, now):
    if action == 'channels':
        return Reply({'channels': catalog.list_all(db)})
    if action == 'board':
        return Reply(overview.board(db, args[0], now))
    if action == 'group_view':
        return Reply(group_state(db, args[0], args[1], now))
    if action == 'pause_view':
        return Reply(group_state(db, args[0], args[1], now))
    if action == 'dead':
        return Reply({'items': deadletters.list_recent(db, args[0])})
    if action == 'trace':
        return Reply(inspection.trace(db, int(args[0])))
    if action == 'retention':
        return Reply(retention.plan(db, args[0], now))
    if action == 'export':
        return Reply(evidence.export_trace(db, int(args[0]), now),
                     content_type='application/json; charset=utf-8',
                     filename=f'quaybell-trace-{args[0]}.json')
    if action == 'drain_view':
        report = drain.current_round(db, args[0], args[1], now)
        if report is None:
            from . import subscriptions
            sub = subscriptions.get(db, args[0], args[1])
            report = {
                'round': 0, 'drain_round': 0, 'round_no': 0, 'epoch': 0,
                'generation': 0, 'id': 0, 'state': 'idle', 'status': 'idle',
                'active': False, 'finished': True, 'paused': bool(sub['paused']),
                'channel': args[0], 'group': args[1], 'counts': {},
                'completed': [], 'in_hand': [], 'in_flight': [], 'requeued': [],
                'expired': [], 'unclaimed': [], 'waiting': [], 'dead': [],
            }
            report['drain'] = dict(report)
        return Reply(report)
    if action == 'drain_list':
        return Reply(drain.list_rounds(db, args[0], args[1], now))
    if action == 'drain_detail':
        return Reply(drain.detail(db, args[0], args[1], args[2], now))
    raise AssertionError(action)


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))
