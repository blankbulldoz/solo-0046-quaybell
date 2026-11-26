from pathlib import Path
from string import Template

from .errors import missing
from .protocol import Reply

ASSETS = Path(__file__).parent
STATIC = {'/static/base.css': 'text/css; charset=utf-8',
          '/static/relay.js': 'text/javascript; charset=utf-8',
          '/static/console.js': 'text/javascript; charset=utf-8'}


def fragment(name):
    return (ASSETS / 'pages' / f'{name}.html').read_text(encoding='utf-8')


def serve(path):
    if path == '/':
        page = Template(fragment('shell')).substitute(
            channels=fragment('channels'), queue=fragment('queue'),
            tools=fragment('tools'), trace=fragment('trace'), redrive=fragment('redrive'))
        return Reply(page.encode('utf-8'), content_type='text/html; charset=utf-8')
    if path in STATIC:
        return Reply((ASSETS / path[1:]).read_bytes(), content_type=STATIC[path])
    raise missing('找不到此页面或接口')
