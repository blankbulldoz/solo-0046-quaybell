"""产量结果对账规则。

两代消费者在同一个 ACK 流程上报处理结果：旧版不带结果，新版按供应商各自的
单位上报同一台设备的产量。规则以频道为单位做版本化，归一化到一个整数刻度的
基准量后再做精确比较，避免：

- JSON 序列化差异（键顺序、数字写法）被误判成不同结果；
- 克与千克小数之间的单位换算误差（用分数而不是二进制浮点）；
- 界面按整数取整后看起来相同、实际量不同的记录被当成重复。

供应商原始明细只允许以摘要形式留在内部，绝不进入对外证据。
"""
from fractions import Fraction

from . import codec, inputs, journal
from .errors import conflict, invalid, missing

# 决定原因码：写入内部列并以稳定标识出现在回执里，便于跨规则版本解释。
ACCEPTED = 'result_accepted'
CONFLICT = 'result_amount_conflict'

# 对外可见字段（原始明细与原始数字均不在其中）。
PUBLIC_RESULT = ('result_rule', 'result_unit', 'canonical_amount', 'result_reason')

DEFAULT_BASE_UNIT = 'g'
DEFAULT_TICK_SCALE = 3
# 第一版规则覆盖两个供应商约定的单位；因子以十进制字符串表示，换算无浮点误差。
DEFAULT_UNITS = {
    'g': '1',
    'kg': '1000',
}
MAX_UNITS = 12
MAX_UNIT_CODE = 16


def _fraction(value, label):
    """把 JSON 数字或十进制字符串严格解析成有理数，拒绝 NaN/无穷和科学计数法。"""
    if isinstance(value, bool) or value is None:
        raise invalid(f'{label}必须是有限数字')
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, float):
        # 只接受有限值；Fraction(float) 会保留二进制表示，故要求显式十进制字符串。
        raise invalid(f'{label}须以十进制字符串或整数给出，避免浮点歧义')
    if isinstance(value, str):
        text = value.strip()
        if not text or any(ch in text for ch in 'eE/ '):
            raise invalid(f'{label}须为普通十进制数字')
        try:
            return Fraction(text)
        except (ValueError, ZeroDivisionError):
            raise invalid(f'{label}不是有效十进制数字') from None
    raise invalid(f'{label}必须是有限数字')


def _validate_units(raw):
    if not isinstance(raw, dict) or not raw:
        raise invalid('单位表须为非空对象')
    if len(raw) > MAX_UNITS:
        raise invalid(f'单位表最多 {MAX_UNITS} 项')
    units = {}
    for code, spec in raw.items():
        if not isinstance(code, str) or not code or len(code) > MAX_UNIT_CODE:
            raise invalid('单位代码须为 1～16 个字符')
        if not isinstance(spec, dict) or set(spec) != {'factor'}:
            raise invalid(f'单位 {code} 只能声明 factor')
        factor = _fraction(spec['factor'], f'单位 {code} 的换算因子')
        if factor <= 0:
            raise invalid(f'单位 {code} 的换算因子必须为正数')
        units[code] = factor
    return units


def _validate_rule(raw):
    missing = [key for key in ('base_unit', 'tick_scale', 'units') if key not in raw]
    if missing:
        raise invalid(f"规则缺少字段：{', '.join(missing)}")
    base_unit = raw['base_unit']
    if not isinstance(base_unit, str) or not base_unit or len(base_unit) > MAX_UNIT_CODE:
        raise invalid(f'基准单位须为 1～{MAX_UNIT_CODE} 个字符')
    tick_scale = inputs.integer(raw['tick_scale'], 0, 12, '刻度小数位')
    units = _validate_units(raw['units'])
    if base_unit not in units:
        raise invalid('基准单位必须出现在单位表中')
    return base_unit, tick_scale, units


def seed_default(db, channel, now=None):
    """新建频道或迁移旧库时写入第一版规则。"""
    if now is None:
        from time import time
        now = time()
    record = dict(version=1, base_unit=DEFAULT_BASE_UNIT, tick_scale=DEFAULT_TICK_SCALE,
                  units=DEFAULT_UNITS)
    return _insert(db, channel, record, now)


def _insert(db, channel, record, now):
    db.execute('INSERT INTO result_rules(channel,version,base_unit,tick_scale,units,created) VALUES (?,?,?,?,?,?)',
               (channel, record['version'], record['base_unit'], record['tick_scale'],
                codec.pack(_unit_specs(record['units'])), now))
    return read(db, channel, record['version'])


def _unit_specs(units):
    return {code: str(factor) for code, factor in sorted(units.items())}


def _decode(row):
    record = dict(row)
    record['units'] = dict(sorted(codec.unpack(row['units']).items()))
    return record


def read(db, channel, version=None):
    base = 'SELECT * FROM result_rules WHERE channel=?'
    if version is None:
        row = db.execute(base + ' ORDER BY version DESC LIMIT 1', (channel,)).fetchone()
    else:
        row = db.execute(base + ' AND version=?', (channel, version)).fetchone()
    if row is None:
        raise missing('找不到结果对账规则')
    return _decode(row)


def revise(db, channel, expected, raw, now):
    current = read(db, channel)
    if current['version'] != expected:
        raise conflict('结果规则已更新，请重新载入')
    base_unit, tick_scale, units = _validate_rule(raw)
    record = dict(version=expected + 1, base_unit=base_unit, tick_scale=tick_scale, units=units)
    saved = _insert(db, channel, record, now)
    journal.append(db, channel, now, 'result_rule_revised',
                   {'version': saved['version'], 'base_unit': base_unit,
                    'tick_scale': tick_scale, 'units': _unit_specs(units)}, None)
    return saved


def normalize(raw_result, rule):
    """把一次 ACK 上报的结果归一化成基准单位下的整数刻度字符串。

    只校验核对所需的 unit/amount；供应商附加明细不参与判断，仅整体进入摘要，
    因此不会因供应商各自的字段差异误报冲突，明文也不会落到对外视图。
    返回 (单位, 整数刻度字符串, 原始报文摘要)。
    """
    if not isinstance(raw_result, dict):
        raise invalid('处理结果须为对象')
    if 'unit' not in raw_result or 'amount' not in raw_result:
        raise invalid('处理结果须包含 unit 和 amount')
    unit = raw_result['unit']
    if not isinstance(unit, str) or unit not in rule['units']:
        raise invalid('处理结果使用了规则未定义的单位')
    amount = _fraction(raw_result['amount'], '产量数值')
    if amount < 0:
        raise invalid('产量数值不能为负')
    factor = Fraction(rule['units'][unit])
    tick = Fraction(10) ** rule['tick_scale']
    scaled = amount * factor * tick
    if scaled.denominator != 1:
        raise invalid('产量数值超出规则刻度精度，无法精确归一化')
    canonical = str(scaled.numerator)
    digest = codec.fingerprint(codec.pack(raw_result))
    return unit, canonical, digest


def same_amount(previous_canonical, incoming_canonical):
    """同一尝试、同一钉住规则版本下的整数刻度精确比较。"""
    return previous_canonical == incoming_canonical


def public_view(attempt):
    """从尝试行抽取可对外展示的结果字段，绝不包含原始明细或摘要以外的输入。"""
    if attempt.get('result_rule') is None:
        return None
    return {key: attempt[key] for key in PUBLIC_RESULT}


def describe(rule, canonical_amount):
    """把整数刻度还原成基准量的十进制字符串，用于对外解释，不做界面取整。"""
    scale = 10 ** rule['tick_scale']
    value = int(canonical_amount)
    if rule['tick_scale'] == 0:
        return f'{value} {rule["base_unit"]}'
    whole, part = divmod(value, scale)
    return f'{whole}.{str(part).zfill(rule["tick_scale"])} {rule["base_unit"]}'
