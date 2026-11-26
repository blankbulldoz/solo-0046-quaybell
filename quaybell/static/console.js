import {$, TEXT, api, button, clock, node, notice, onForm, parse, rows, run, status} from './relay.js';

const state = {channel: null, group: null, board: null, sequence: 0, retention: null, leases: new Map(), impact: null};
const policyFields = ['lease_seconds', 'retry_seconds', 'max_attempts', 'retention_seconds'];
const group = () => state.board?.groups.find(item => item.name === state.group);
const channelPath = () => `/channels/${state.channel}`;
const groupPath = () => `${channelPath()}/groups/${state.group}`;
const traceLink = (id, label = `#${id}`) => button(label, () => trace(id), {className: 'link'});
const submitButton = () => $('revise').querySelector('button[value="commit"]');
const holdSubmit = disabled => {
  const control = submitButton();
  control.disabled = disabled;
  control.dataset.keepDisabled = String(disabled);
};
const proposedValues = form => Object.fromEntries(policyFields.map(key => [key, Number(form.elements[key].value)]));
const sameValues = (left, right) => policyFields.every(key => left?.[key] === right?.[key]);

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

function clearImpact(message) {
  state.impact = null;
  holdSubmit(true);
  $('impact').hidden = true;
  $('impact').replaceChildren();
  $('impact-status').textContent = message || '先对比影响再提交；提交须基于当前对照。';
  $('impact-status').dataset.state = 'idle';
}

async function choose(channel) {
  if (channel !== state.channel) {
    state.channel = channel;
    state.group = null;
    state.board = null;
    $('publication-result').textContent = '';
    resetRetention();
    clearImpact();
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
      render();
    })) : [node('p', '暂无消费组', 'muted')]));
  $('claim').querySelector('button').disabled = !selected || selected.paused;
  $('toggle-group').disabled = !selected;
  $('toggle-group').textContent = selected?.paused ? '恢复取件' : '暂停取件';
  const form = $('revise');
  const stamp = board ? `${board.channel}/${board.policy.version}` : '';
  if (form.dataset.stamp !== stamp) {
    for (const field of policyFields) form.elements[field].value = board?.policy[field] ?? '';
    form.dataset.stamp = stamp;
    if (stamp) clearImpact(`当前v${board.policy.version}，表单已重置；先对比影响再提交。`);
    else clearImpact();
  } else {
    syncImpact();
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
      `第 ${a.number} 次 · ${a.worker} · ${TEXT[a.outcome]} · ${clock(a.started)}${a.reason ? ` · ${a.reason}` : ''}`)));
    section.append(label, entries);
    return section;
  }));
  if (!facts.deliveries.length) $('trace-deliveries').append(node('p', '发布时没有匹配消费组', 'muted'));
  $('trace-events').replaceChildren(...facts.events.map(item => node('li',
    `${clock(item.at)} · ${TEXT[item.kind === 'leased' ? 'leased_event' : item.kind]} · ${JSON.stringify(item.details)}`)));
  $('trace-export').href = `/v1/messages/${id}/evidence`;
  $('trace').showModal();
}

const CERTAINTY = {settled: '已确定', pending: '待后续操作'};

function effectCard(item) {
  const card = node('article', '', 'effect');
  card.dataset.certainty = item.certainty;
  const head = node('div', '', 'effect-head');
  head.append(node('h4', item.title), node('span', CERTAINTY[item.certainty] || item.certainty, 'certainty'));
  card.append(head, node('p', item.summary, 'muted'),
    node('p', item.current_text, 'effect-line'), node('p', item.proposed_text, 'effect-line'));
  if (item.fresh_note) card.append(node('p', item.fresh_note, 'muted'));
  if (item.items?.length) {
    const list = node('ul', '', 'effect-items');
    list.append(...item.items.map(entry => node('li',
      `#${entry.delivery_id} · ${entry.group_name} · 顺序键 ${entry.lane} · v${entry.policy}` +
      (entry.expires != null
        ? ` · ${entry.expired ? '已到期待回收' : `剩余 ${entry.remaining_seconds} 秒`}` : '') +
      (entry.next_backoff_seconds != null
        ? ` · 下次退避 ${entry.next_backoff_seconds} 秒 / 租约 ${entry.next_lease_seconds} 秒` : ''))));
    if (item.omitted) list.append(node('li', `另有 ${item.omitted} 条未展开`, 'muted'));
    card.append(list);
  }
  return card;
}

function renderImpact() {
  const report = state.impact;
  if (!report) return;
  const {baseline, proposed, effects, settled_count: settled} = report;
  $('impact').replaceChildren(
    node('p', `v${baseline.version} → v${proposed.version} · 在手已确定 ${settled} · 其余待后续操作`,
      'impact-head'),
    node('p', report.binding_note, 'muted'), ...effects.map(effectCard));
  $('impact').hidden = false;
  $('impact-status').textContent = report.stale
    ? '提交被拒，以下为最新状态的重算对照：请核对后再提交。'
    : `对照基于 ${clock(report.observed_at)} 的快照；确认无误后再提交。`;
  $('impact-status').dataset.state = report.stale ? 'stale' : 'ready';
  holdSubmit(Boolean(report.stale));
}

function syncImpact() {
  const form = $('revise');
  const report = state.impact;
  if (!report) return;
  if (form.dataset.stamp !== report.stamp) {
    clearImpact('频道或版本已变化，旧对照失效；请重新对比影响。');
  } else if (state.board && report.baseline.state && state.board.state_token !== report.baseline.state) {
    clearImpact('对照之后频道已有新发布、领取或处置，旧对照失效；请重新对比影响。');
  }
}

async function showImpact() {
  await refresh();
  const form = $('revise');
  const channel = state.channel;
  const board = state.board;
  const expected = Number(form.dataset.stamp.split('/')[1]);
  const proposed = proposedValues(form);
  let report;
  try {
    report = await api(`${channelPath()}/policy/impact`, {...proposed, expected});
  } catch (problem) {
    if (problem.status !== 409) throw problem;
    clearImpact('频道策略已被他人修订，旧版号不再适用；请核对新版本后重新对比影响。');
    await refresh();
    return;
  }
  if (state.channel !== channel || form.dataset.stamp !== `${channel}/${expected}`) {
    clearImpact('对照期间频道已切换或刷新，请重新对比影响。');
    return;
  }
  if (board && board.state_token !== report.baseline.state) {
    clearImpact('对照期间频道状态已有变化，本次对照不可用于提交；请重新对比影响。');
    return;
  }
  state.impact = {...report, stamp: form.dataset.stamp, values: proposed};
  renderImpact();
}

async function commitRevision() {
  const form = $('revise');
  const report = state.impact;
  const proposed = proposedValues(form);
  if (!report || report.stamp !== form.dataset.stamp || !sameValues(report.values, proposed)) {
    clearImpact('拟修订值已改动或对照已失效；请重新对比影响。');
    return;
  }
  const expected = Number(form.dataset.stamp.split('/')[1]);
  try {
    const result = await api(`${channelPath()}/policy`, {
      ...proposed, expected, impact_token: report.baseline.token,
    });
    clearImpact();
    notice(`已提交策略 v${result.version}`);
    await refresh();
  } catch (problem) {
    if (problem.code === 'stale_preview' && problem.detail) {
      state.impact = {...problem.detail, stamp: form.dataset.stamp, values: proposed, stale: true};
      renderImpact();
      $('impact-status').textContent =
        '提交被拒：对照之后频道已有变化，旧对照不能当作刚提交的结果。' +
        '下面是按最新状态重算的对照，尚未提交；请重新对比影响后再提交。';
      $('impact-status').dataset.state = 'stale';
      await refresh();
    } else if (problem.code === 'stale_preview' || problem.status === 409) {
      clearImpact('提交被拒：旧对照已失效，未提交任何改动；请重新对比影响。');
      $('impact-status').dataset.state = 'stale';
      await refresh();
    } else {
      throw problem;
    }
    notice('提交被拒：旧对照已失效，未提交', true);
  }
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
onForm('revise', async (values, mode) => {
  if (mode === 'commit') return commitRevision(values);
  return showImpact(values);
});
$('revise').addEventListener('input', () => {
  if (!state.impact) return;
  clearImpact('拟修订值已改动，旧对照失效；请重新对比影响。');
});
$('toggle-group').addEventListener('click', () => run(async () => {
  if (!group()) return;
  await api(`${groupPath()}/pause`, {paused: !group().paused});
  await refresh();
}));
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
