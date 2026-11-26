import {$, TEXT, api, button, clock, node, notice, onForm, parse, rows, run, status} from './relay.js';

const state = {channel: null, group: null, board: null, sequence: 0, retention: null, leases: new Map()};
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
  $('retention-reload').hidden = true;
  $('retention-panel').replaceChildren();
  $('retention-result').textContent = '';
}

const SKIP_TEXT = {
  redriven: '死信已被重投，存在活跃后继分发',
  active_delivery: '仍有等待或租约中的分发',
  active_attempt: '仍有未结束的租约尝试',
  not_due: '复查时尚未到保留截止时刻',
  not_found: '消息已不存在（可能已被其他清理删除）',
};

function renderCandidates(reply) {
  const panel = $('retention-panel');
  const card = node('div', '', 'plan-card');
  card.append(node('h4', `当前候选 ${reply.eligible.length} 条 · 观测于 ${clock(reply.observed_at)}`));
  if (reply.eligible.length) {
    card.append(node('p', reply.eligible.map(item => `#${item.id}`).join('、'), 'mono'));
    card.append(node('p', '候选会随时间和处置状态变化；点击“生成批准计划”才会冻结为具体名单。', 'muted'));
  } else {
    card.append(node('p', '暂时没有符合清理条件的消息', 'muted'));
  }
  panel.replaceChildren(card);
}

function renderRetentionPlan() {
  const plan = state.retention?.plan;
  $('retention-reload').hidden = !plan;
  $('retention-apply').disabled = !plan || plan.status !== 'prepared';
  if (!plan) return;
  const card = node('div', '', `plan-card plan-${plan.status}`);
  const head = node('div', '', 'dialog-head');
  head.append(node('h4', `清理计划 #${plan.plan_id}`));
  let badgeText = '待批准';
  let badgeState = 'delayed';
  if (plan.status === 'applied') {
    const skipCount = (plan.skipped || []).length;
    const removeCount = (plan.removed || []).length;
    if (skipCount && removeCount) {
      badgeText = '部分删除';
      badgeState = 'delayed';
    } else if (skipCount && !removeCount) {
      badgeText = '全部跳过';
      badgeState = 'dead';
    } else {
      badgeText = '已执行';
      badgeState = 'acked';
    }
  }
  const badge = node('span', badgeText, 'status');
  badge.dataset.state = badgeState;
  head.append(badge);
  card.append(head);
  const ids = plan.message_ids.length ? plan.message_ids.map(id => `#${id}`).join('、') : '（空名单）';
  card.append(node('p', `冻结名单 ${plan.message_ids.length} 条：${ids}`, 'mono'));
  card.append(node('p', `预览时刻 ${clock(plan.observed_at)} · 建计划 ${clock(plan.created_at)}`, 'muted'));
  if (plan.approved_by) {
    card.append(node('p', `首位批准人：${plan.approved_by} · 执行于 ${clock(plan.applied_at)}`));
  }
  if (plan.approvals?.length) {
    const text = plan.approvals
      .map(entry => `${entry.by || '未署名'}${entry.repeated ? '（重复请求）' : ''} ${clock(entry.at)}`)
      .join('；');
    card.append(node('p', `批准记录：${text}`, 'muted'));
  }
  if (plan.status === 'applied') {
    const removed = plan.removed || [];
    const skipped = plan.skipped || [];
    const outcomeText = {
      applied: '名单全部删除', partial: '部分删除，其余变化跳过',
      skipped: '全部因状态变化跳过', empty: '空名单',
    }[plan.outcome] || '已处理';
    card.append(node('p', `计划状态：已执行 · ${outcomeText}`, removed.length ? 'removed-line' : 'skip-title'));
    card.append(node('p',
      `已删除 ${removed.length} 条：${removed.length ? removed.map(id => `#${id}`).join('、') : '无'}`,
      removed.length ? 'removed-line' : 'muted'));
    card.append(node('p', `变化跳过 ${skipped.length} 条：`, skipped.length ? 'skip-title' : 'muted'));
    if (skipped.length) {
      const list = node('ul', '', 'skip-list');
      for (const item of skipped) {
        const detail = item.delivery_id ? ` · 活跃分发 #${item.delivery_id}` : '';
        list.append(node('li', `#${item.id} · ${SKIP_TEXT[item.reason] || item.reason}${detail}`));
      }
      card.append(list);
    } else {
      card.append(node('p', '名单内没有发生变化的条目。', 'muted'));
    }
  } else {
    card.append(node('p', '尚未执行：批准后只删除冻结名单；之后新到期的消息不会连带清理，状态变化的条目会跳过。', 'muted'));
  }
  $('retention-panel').replaceChildren(card);
}

async function syncRetentionPlan() {
  const holder = state.retention;
  if (!holder?.plan || holder.plan.status === 'applied') return;
  try {
    const plan = await api(`/channels/${holder.channel}/retention/plans/${holder.plan.plan_id}`);
    if (state.channel !== holder.channel) return;
    holder.plan = plan;
    renderRetentionPlan();
    $('retention-result').textContent =
      `计划 #${plan.plan_id} 检测到名单状态变化，批准时将跳过受保护条目`;
  } catch {
    // 面板上的“刷新计划状态”可手动重试
  }
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
        await syncRetentionPlan();
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
$('recover').addEventListener('click', () => run(async () => {
  const report = await api('/sweep', {limit: 100});
  notice(`已回收 ${report.recovered.length} 个过期租约`);
  await refresh();
}, $('recover')));
$('refresh').addEventListener('click', () => run(refresh, $('refresh')));
$('retention-plan').addEventListener('click', () => run(async () => {
  const channel = state.channel;
  if (!state.retention || state.retention.channel !== channel) {
    state.retention = {channel, plan: null};
  }
  const reply = await api(`${channelPath()}/retention`);
  if (state.channel !== channel) return;
  state.retention.candidates = reply.eligible;
  renderCandidates(reply);
  $('retention-result').textContent = reply.eligible.length
    ? `候选 ${reply.eligible.length} 条，可继续生成冻结名单计划`
    : '暂时没有符合清理条件的消息';
}, $('retention-plan')));
$('retention-freeze').addEventListener('click', () => {
  void run(async () => {
    const channel = state.channel;
    const plan = await api(`${channelPath()}/retention/plans`, {limit: 100});
    if (state.channel !== channel) return;
    state.retention = {channel, plan};
    renderRetentionPlan();
    $('retention-result').textContent =
      `已冻结计划 #${plan.plan_id}，名单 ${plan.message_ids.length} 条，等待批准`;
  }, $('retention-freeze')).finally(() => renderRetentionPlan());
});
$('retention-reload').addEventListener('click', () => run(async () => {
  const holder = state.retention;
  if (!holder?.plan) return;
  const plan = await api(`/channels/${holder.channel}/retention/plans/${holder.plan.plan_id}`);
  if (state.channel !== holder.channel) return;
  holder.plan = plan;
  renderRetentionPlan();
}, $('retention-reload')));
$('retention-apply').addEventListener('click', () => {
  void run(async () => {
    const holder = state.retention;
    const plan = holder?.plan;
    if (!plan || plan.status !== 'prepared') return;
    const who = $('retention-approver').value.trim() || 'console-admin';
    const nameList = plan.message_ids.length ? plan.message_ids.map(id => `#${id}`).join('、') : '空名单';
    const approved = window.confirm(
      `确认按清理计划 #${plan.plan_id} 的冻结名单执行？\n`
      + `名单：${nameList}\n批准人：${who}\n\n`
      + '预览后新到期的消息不会删除；状态已变化（如死信已重投）的条目会自动跳过。');
    if (!approved) return;
    const reply = await api(`${channelPath()}/retention/plans/${plan.plan_id}/apply`,
      {approved_by: who});
    holder.plan = reply;
    const tail = reply.repeated ? '；重复批准/重试，返回首次执行结果' : '';
    const summary = `计划 #${reply.plan_id}：已删除 ${reply.removed.length} 条，`
      + `变化跳过 ${reply.skipped.length} 条${tail}`;
    $('retention-result').textContent = summary;
    notice(summary);
    await refresh();
  }, $('retention-apply')).finally(() => renderRetentionPlan());
});
render();
run(refresh);
