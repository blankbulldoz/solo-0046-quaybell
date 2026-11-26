import {$, TEXT, api, button, clock, node, notice, onForm, parse, rows, run, status} from './relay.js';

const state = {channel: null, group: null, board: null, assignment: null, sequence: 0, retention: null, leases: new Map()};
const policyFields = ['lease_seconds', 'retry_seconds', 'max_attempts', 'retention_seconds'];
const group = () => state.board?.groups.find(item => item.name === state.group);
const channelPath = () => `/channels/${state.channel}`;
const groupPath = () => `${channelPath()}/groups/${state.group}`;
const traceLink = (id, label = `#${id}`) => button(label, () => trace(id), {className: 'link'});

function selectButton(name, active, action) {
  const control = button(name, async () => {
    const scope = control.closest('nav') ? $('channels') : $('groups');
    await action();
    scope.querySelector('[aria-pressed="true"]')?.focus();
  }, {className: 'selector'});
  control.setAttribute('aria-pressed', String(active));
  return control;
}

function resetRetention() {
  state.retention = null;
  $('retention-apply').disabled = true;
  $('retention-result').textContent = '';
}

async function choose(channel) {
  if (channel !== state.channel) {
    state.channel = channel;
    state.group = null;
    state.board = null;
    $('publication-result').textContent = '';
    resetRetention();
  }
  await refresh();
}

async function refresh() {
  const current = ++state.sequence;
  const topology = (await api('/channels')).channels;
  if (current !== state.sequence) return;
  state.channel ||= topology[0]?.name ?? null;
  const board = state.channel ? await api(channelPath()) : null;
  if (current !== state.sequence) return;
  state.board = board;
  if (!group()) state.group = board?.groups[0]?.name ?? null;
  $('channels').replaceChildren(...(topology.length ? topology.map(({name}) =>
    selectButton(name, name === state.channel, () => choose(name))) : [node('p', '暂无频道', 'muted')]));
  render();
  await loadAssignment();
}

function render() {
  const board = state.board;
  const selected = group();
  $('channel-title').textContent = state.channel ?? '选择频道';
  $('observed').textContent = board ? `观测于 ${clock(board.observed_at)}` : '';
  $('policy-version').textContent = board ? `当前策略 v${board.policy.version}` : '';
  for (const item of document.querySelectorAll('[data-needs-channel]')) item.hidden = !board;
  $('groups').replaceChildren(...(board?.groups.length ? board.groups.map(item =>
    selectButton(item.paused ? `${item.name} · 暂停` : item.name, item.name === state.group, () => {
      state.group = item.name;
      state.assignment = null;
      render();
      loadAssignment();
    })) : [node('p', '暂无消费组', 'muted')]));
  $('claim').querySelector('button').disabled = !selected || selected.paused;
  $('toggle-group').disabled = !selected;
  $('toggle-group').textContent = selected?.paused ? '恢复取件' : '暂停取件';
  const form = $('revise');
  const stamp = board ? `${board.channel}/${board.policy.version}` : '';
  if (form.dataset.stamp !== stamp) {
    for (const field of policyFields) form.elements[field].value = board?.policy[field] ?? '';
    form.dataset.stamp = stamp;
  }
  $('queue-group').textContent = selected?.name ?? '';
  $('stats').replaceChildren(...['ready','leased','blocked','delayed','paused','expired'].map(key => {
    const card = node('div', '', 'stat');
    card.dataset.state = key;
    card.append(node('strong', selected?.counts[key] ?? 0), node('span', TEXT[key], 'muted'));
    return card;
  }));
  rows('queue-rows', (selected?.sample || []).map(item => [traceLink(item.message_id, `#${item.id}`),
    item.lane, status(item.cause), `#${item.head_id}`, clock(item.ready_at)]), 5,
    selected ? '当前消费组没有未完成投递' : '请选择已有消费组');
  const messages = board?.messages || [];
  $('message-total').textContent = messages.length ? `最近 ${messages.length} 条` : '';
  rows('message-rows', messages.map(item => [traceLink(item.id), item.producer, item.dedup_key,
    `v${item.policy}`, clock(item.published)]), 5, '当前频道没有消息');
  const dead = board?.dead || [];
  $('dead-total').textContent = dead.length ? `最近 ${dead.length} 条` : '';
  rows('dead-rows', dead.map(item => [traceLink(item.message_id, `#${item.id}`), item.group_name,
    item.last_attempt.reason, item.redriven_as ? `已追加 #${item.redriven_as}` :
      button('重投', async () => {
        const result = await api(`/deliveries/${item.id}/redrive`, {});
        notice(`重投记录 #${result.delivery_id}`);
        resetRetention();
        await refresh();
      })]), 4, '当前频道没有死信');
  renderLeases();
  renderAssignment();
}

async function loadAssignment() {
  if (!state.channel || !state.group) {
    state.assignment = null;
    renderAssignment();
    return;
  }
  const channel = state.channel, groupName = state.group;
  try {
    const assignment = await api(`${channelPath()}/groups/${groupName}/assignments`);
    if (state.channel === channel && state.group === groupName) state.assignment = assignment;
  } catch {
    if (state.channel === channel && state.group === groupName) state.assignment = null;
  }
  renderAssignment();
}

function renderAssignment() {
  const meta = $('assignment-meta');
  if (!meta) return;
  if (!state.group) {
    meta.textContent = '请选择消费组';
    rows('partition-rows', [], 2, '尚未选择消费组');
    rows('ownership-rows', [], 6, '尚未选择消费组');
    return;
  }
  const assignment = state.assignment;
  if (!assignment || !assignment.configured) {
    meta.textContent = `${state.group} 尚未配置固定分片；当前任何 worker 都可按同键队首规则取件`;
    rows('partition-rows', [], 2, '尚未配置分片');
    rows('ownership-rows', [], 6, '暂无分片归属');
    return;
  }
  meta.textContent = `分配版本 v${assignment.version}（generation ${assignment.generation ?? assignment.version}）`
    + ` · ${assignment.partition_count} 分片 · 策略 ${assignment.strategy || 'round_robin'}`;
  rows('partition-rows', (assignment.partitions || []).map(item =>
    [`#${item.partition}`, item.owner]), 2, '无分片');
  rows('ownership-rows', (assignment.keys || []).map(item => [
    item.lane,
    `#${item.partition}`,
    item.owner || '—',
    item.serving_worker || '—',
    status(item.state),
    item.open_attempt
      ? `租约 v${item.open_attempt.assignment_version ?? '?'}${item.open_attempt.stale ? ' · 旧版' : ''}`
      : `当前 v${assignment.version}`,
  ]), 6, '暂无顺序键（发布消息后在此核对每键归属）');
  const form = $('assignment-form');
  const stamp = `${state.channel}/${state.group}/v${assignment.version}`;
  if (form.dataset.stamp !== stamp) {
    form.elements.partitions.value = assignment.partition_count;
    form.elements.workers.value = [...new Set((assignment.partitions || []).map(p => p.owner))].join(',');
    form.dataset.stamp = stamp;
  }
}

function renderLeases() {
  const entries = [...state.leases.values()].filter(item => item.channel === state.channel && item.group === state.group);
  $('leases').replaceChildren(...entries.map(item => {
    const card = node('div', '', 'lease-card');
    const reason = node('input');
    reason.placeholder = '填写拒绝原因';
    reason.setAttribute('aria-label', `投递 ${item.delivery_id} 的拒绝原因`);
    reason.maxLength = 240;
    const actions = node('div', '', 'button-row');
    const settle = action => async () => {
      const data = {token: item.lease.token};
      if (action === 'nack') data.reason = reason.value;
      const result = await api(`/deliveries/${item.delivery_id}/${action}`, data);
      state.leases.delete(item.delivery_id);
      notice(`投递 #${item.delivery_id}：${TEXT[result.state]}`);
      await refresh();
    };
    actions.append(button('确认', settle('ack')), button('拒绝', settle('nack'), {className: 'danger'}),
      button('移除卡片', () => { state.leases.delete(item.delivery_id); renderLeases(); }));
    card.append(node('p', `投递 #${item.delivery_id} · 第 ${item.lease.number} 次`, 'mono'),
      node('p', `租约截止 ${clock(item.lease.expires)}`, 'muted'), reason, actions);
    return card;
  }));
}

async function trace(id) {
  const facts = await api(`/messages/${id}`);
  $('trace-title').textContent = `消息 #${id}`;
  const m = facts.message;
  $('trace-meta').textContent = `${m.channel} · ${m.producer}/${m.dedup_key} · v${m.policy} · ${clock(m.published)}`;
  $('trace-envelope').textContent = JSON.stringify(m.envelope, null, 2);
  $('trace-deliveries').replaceChildren(...facts.deliveries.map(item => {
    const section = node('section');
    const label = node('h4', `#${item.id} · ${item.group_name} · v${item.policy} `);
    if (item.parent_id) label.append(`· 源投递 #${item.parent_id} `);
    label.append(status(item.state));
    const entries = node('ul', '', 'trace-attempts');
    entries.append(...item.attempts.map(a => node('li',
      `第 ${a.number} 次 · ${a.worker} · ${TEXT[a.outcome]} · ${clock(a.started)}`
      + `${a.assignment_version != null ? ` · 分片代际 v${a.assignment_version}` : ''}`
      + `${a.reason ? ` · ${a.reason}` : ''}`)));
    section.append(label, entries);
    return section;
  }));
  if (!facts.deliveries.length) $('trace-deliveries').append(node('p', '发布时没有匹配消费组', 'muted'));
  $('trace-events').replaceChildren(...facts.events.map(item => node('li',
    `${clock(item.at)} · ${TEXT[item.kind === 'leased' ? 'leased_event' : item.kind]} · ${JSON.stringify(item.details)}`)));
  $('trace-export').href = `/v1/messages/${id}/evidence`;
  $('trace').showModal();
}

onForm('create-channel', async ({name}) => {
  await api('/channels', {name});
  await choose(name);
  $('create-channel').reset();
  notice('频道已建立');
});
onForm('create-group', async ({name, tags}) => {
  const channel = state.channel;
  await api(`${channelPath()}/groups`, {name, tags: parse(tags, '匹配标签')});
  if (state.channel === channel) state.group = name;
  $('create-group').reset();
  await refresh();
  notice('消费组已添加');
});
onForm('publish', async (values, mode) => {
  const channel = state.channel;
  const reply = await api(`${channelPath()}/${mode || 'messages'}`, {
    ...values, tags: parse(values.tags, '标签'), payload: parse(values.payload, '载荷'),
    delay_seconds: Number(values.delay_seconds),
  });
  if (state.channel !== channel) return;
  $('publication-result').textContent = mode === 'preview'
    ? (reply.groups.length ? `匹配：${reply.groups.join('、')}` : '当前无匹配消费组')
    : `消息 #${reply.message_id} · ${reply.repeated ? '已复用原回执' : '已发布'} · ${Object.keys(reply.fanout).length} 个消费组`;
  if (mode !== 'preview') await refresh();
});
onForm('claim', async ({worker, limit}) => {
  if (!group()) return;
  const channel = state.channel, selected = state.group;
  const reply = await api(`${groupPath()}/claim`, {worker, limit: Number(limit)});
  for (const item of reply.items) state.leases.set(item.delivery_id, {...item, channel, group: selected});
  notice(reply.paused ? '消费组已暂停' : `领取 ${reply.items.length} 条`);
  await refresh();
});
onForm('revise', async values => {
  const expected = Number($('revise').dataset.stamp.split('/')[1]);
  const report = await api(`${channelPath()}/policy`, Object.fromEntries([
    ...Object.entries(values).map(([key, value]) => [key, Number(value)]), ['expected', expected],
  ]));
  notice(`已提交策略 v${report.version}`);
  await refresh();
});
$('toggle-group').addEventListener('click', () => run(async () => {
  if (!group()) return;
  await api(`${groupPath()}/pause`, {paused: !group().paused});
  await refresh();
}));
onForm('assignment-form', async values => {
  if (!group()) return;
  const workers = values.workers.split(',').map(item => item.trim()).filter(Boolean);
  if (!workers.length) throw new Error('请至少填写一个工作进程');
  const expected = state.assignment?.version;
  const body = {partitions: Number(values.partitions), workers};
  if (expected) body.expected = expected;
  const reply = await api(`${groupPath()}/partitions`, body);
  state.assignment = reply;
  const fenced = reply.fenced_leases?.length ? `，围栏 ${reply.fenced_leases.length} 个失权旧租约` : '';
  notice(`分片分配已提交：版本 v${reply.version}${fenced}`);
  await refresh();
});
$('assignment-fence').addEventListener('click', () => run(async () => {
  if (!group()) return;
  const reply = await api(`${groupPath()}/fence`, {});
  notice(reply.fenced_leases.length ? `已围栏 ${reply.fenced_leases.length} 个失权租约` : '没有需要围栏的失权租约');
  await refresh();
}, $('assignment-fence')));
$('recover').addEventListener('click', () => run(async () => {
  const report = await api('/sweep', {limit: 100});
  notice(`已回收 ${report.recovered.length} 个过期租约`);
  await refresh();
}, $('recover')));
$('refresh').addEventListener('click', () => run(refresh, $('refresh')));
$('retention-plan').addEventListener('click', () => run(async () => {
  const channel = state.channel;
  const reply = await api(`${channelPath()}/retention`);
  if (state.channel !== channel) return;
  state.retention = channel;
  $('retention-result').textContent = reply.eligible.length
    ? `候选 ${reply.eligible.length} 条：${reply.eligible.map(item => `#${item.id}`).join('、')}`
    : '暂时没有符合清理条件的消息';
  $('retention-apply').disabled = !reply.eligible.length;
}, $('retention-plan')));
$('retention-apply').addEventListener('click', () => run(async () => {
  if (state.retention !== state.channel || !window.confirm('立即重新检查保留条件并清理？')) return;
  const reply = await api(`${channelPath()}/retention`, {limit: 100});
  resetRetention();
  notice(`已清理 ${reply.removed.length} 条消息`);
  await refresh();
}));
render();
run(refresh);
