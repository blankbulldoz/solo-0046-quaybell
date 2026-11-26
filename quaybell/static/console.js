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
        resetRetention();
        await refresh();
      })]), 4, '当前频道没有死信');
  renderLeases();
  void loadCandidates();
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
function jsonPre(value) {
  return node('pre', JSON.stringify(value, null, 2));
}

function fingerprintLine(digest) {
  return node('p', `内容指纹 sha256:${digest}`, 'mono fingerprint');
}

function destinationView(doc) {
  const wrap = node('div', '', 'chip-row');
  const fanout = doc.fanout || doc.actual_destinations;
  if (fanout) {
    const entries = Object.entries(fanout);
    if (!entries.length) wrap.append(node('span', '无匹配消费组（实际去向为空）', 'muted'));
    for (const [groupName, deliveryId] of entries) {
      const chip = node('span', '', 'chip chip-delivery');
      chip.append(node('b', groupName), node('span', `→ 投递 #${deliveryId}`));
      wrap.append(chip);
    }
    return wrap;
  }
  const names = doc.destinations_current || doc.destinations || doc.groups || [];
  if (!names.length) wrap.append(node('span', '当前没有匹配消费组', 'muted'));
  for (const name of names) wrap.append(node('span', name, 'chip'));
  return wrap;
}

function receiptPanel(receipt) {
  const box = node('div', '', 'receipt-box');
  box.append(node('h4', '稳定回执（重复确认读取同一记录）'));
  box.append(node('p', `消息 #${receipt.message_id} · 策略 v${receipt.policy}`, 'mono'));
  box.append(destinationView({fanout: receipt.fanout}));
  return box;
}

function showDialog() {
  const dialog = $('review-dialog');
  if (!dialog.open) dialog.showModal();
}

function openReview(review) {
  showDialog();
  renderConflict(review);
}

function openCandidate(doc) {
  showDialog();
  renderCandidate(doc);
}

function renderConflict(review) {
  const dialog = $('review-dialog');
  $('review-title').textContent = `键冲突复核 · ${review.producer}/${review.key}`;
  const body = $('review-body');
  body.replaceChildren();
  body.append(node('p',
    '这个生产键已绑定另一个信封，已承诺内容不会被覆盖。请核对两侧内容：重发原告警会复用原稳定回执；右侧确实是新告警时，用候选新键走显式确认。',
    'muted'));
  const grid = node('div', '', 'review-grid');

  const oldCard = node('section', '', 'review-card old');
  oldCard.append(node('h3', '① 原告警（已落库）'));
  const original = review.original;
  if (original) {
    oldCard.append(node('p', `消息 #${original.message_id} · ${clock(original.published)} · 策略 v${original.policy}`, 'mono'));
    oldCard.append(jsonPre(original.envelope));
    oldCard.append(fingerprintLine(original.fingerprint));
    oldCard.append(node('h4', '稳定回执与最初去向'));
    oldCard.append(destinationView({fanout: original.fanout}));
    oldCard.append(button(`重发旧告警 · 复用回执 #${original.message_id}`, async () => {
      const reply = await api(`/messages/${original.message_id}/resend`, {});
      notice(reply.repeated
        ? `已重发旧告警，复用稳定回执 #${reply.message_id}`
        : `原记录已不存在，按原信封发布为新消息 #${reply.message_id}`);
      dialog.close();
      await refresh();
      await loadCandidates();
    }, {className: 'primary'}));
  } else {
    oldCard.append(node('p', review.original_state === 'pruned'
      ? '原告警已被保留清理，旧稳定回执已失效。'
      : '该键当前没有已提交消息（可能已被清理）。', 'race-error'));
  }

  const incoming = review.incoming || {};
  const newCard = node('section', '', 'review-card incoming');
  newCard.append(node('h3', '② 表单里的告警（尚未发布）'));
  newCard.append(jsonPre(incoming.envelope));
  newCard.append(fingerprintLine(incoming.fingerprint || review.fingerprint));
  newCard.append(node('h4', '预演去向（按当前订阅，确认时重新计算）'));
  newCard.append(destinationView({destinations: incoming.destinations || review.destinations}));
  const keyLabel = node('label', '候选新键（可修改）', 'field-label');
  const keyInput = node('input');
  keyInput.id = 'review-new-key';
  keyInput.value = review.suggested_key || review.candidate_key || '';
  keyInput.autocomplete = 'off';
  keyLabel.append(keyInput);
  newCard.append(keyLabel);
  newCard.append(button('建立候选复核单（这一步仍不会发布）', async () => {
    const candidateKey = $('review-new-key').value.trim();
    const payload = {...incoming.envelope_request, candidate_key: candidateKey};
    if (original?.message_id) payload.original_message_id = original.message_id;
    const candidate = await api(`${channelPath()}/candidates`, payload);
    notice(`候选单 #${candidate.candidate_id} 已建立：未经确认不会产生消息`);
    renderCandidate(candidate);
    await loadCandidates();
  }, {className: 'primary'}));
  newCard.append(node('p', '建立后还有确认步骤，把最终信封、实际去向和稳定回执关联到同一张候选单。', 'muted'));

  grid.append(oldCard, newCard);
  body.append(grid);
}

function renderCandidate(candidate, problemBody = null) {
  const dialog = $('review-dialog');
  $('review-title').textContent = `发布候选单 #${candidate.candidate_id}`;
  const body = $('review-body');
  body.replaceChildren();

  const banner = node('div', '', `candidate-banner ${candidate.status}`);
  if (candidate.status === 'confirmed') {
    banner.append(node('strong', '已确认发布 · 最终信封、实际去向与稳定回执已关联'));
  } else if (candidate.status === 'discarded') {
    banner.append(node('strong', '候选已放弃 · 从未发布'));
  } else {
    banner.append(node('strong', '待确认 · 尚未发布，不会自动产生消息或投递'));
  }
  body.append(banner);

  if (problemBody) {
    const errorBox = node('div', '', 'race-error');
    errorBox.append(node('strong', problemBody.message || '确认未成功'));
    const suggested = problemBody.suggested_key || problemBody.review?.suggested_key;
    if (suggested) {
      const row = node('div', '', 'button-row');
      row.append(node('span', `该键刚刚被另一条告警占用，可改用未占用键：`, 'muted'));
      row.append(button(`采用建议键 ${suggested}`, () => {
        $('cand-final-key').value = suggested;
      }, {className: 'quiet mini'}));
      errorBox.append(row);
    }
    body.append(errorBox);
  }

  if (candidate.original_state === 'pruned') {
    const snapshotId = candidate.original?.message_id
      || candidate.original_snapshot?.message_id || candidate.original_message_id;
    const warn = node('p', '', 'prune-note');
    warn.append(node('strong', '原告警在等待期间已被清理。'));
    warn.append(document.createTextNode(
      ` 旧回执${snapshotId ? ` #${snapshotId}` : ''} 已失效，不能再“重发复用”；本次确认会生成全新的稳定回执。`));
    body.append(warn);
  }

  const grid = node('div', '', 'review-grid');
  const envelopeCard = node('section', '', 'review-card');
  envelopeCard.append(node('h3', '最终信封'));
  envelopeCard.append(jsonPre(candidate.final_envelope || candidate.envelope));
  envelopeCard.append(fingerprintLine(candidate.fingerprint));
  grid.append(envelopeCard);

  const sideCard = node('section', '', 'review-card');
  const keyLabel = node('label', '最终生产键（确认时生效）', 'field-label');
  const keyInput = node('input');
  keyInput.id = 'cand-final-key';
  keyInput.autocomplete = 'off';
  keyInput.disabled = candidate.status !== 'pending';
  keyInput.value = problemBody?.suggested_key
    || candidate.final_key || candidate.key || candidate.candidate_key || '';
  keyLabel.append(keyInput);
  sideCard.append(keyLabel);
  if (candidate.reuses_released_key) {
    sideCard.append(node('p', '原键已随清理释放；使用该键将创建一条全新消息与新回执，而不是复用旧回执。', 'prune-note'));
  }
  sideCard.append(node('h4', '实际去向'));
  sideCard.append(destinationView(candidate));
  if (candidate.status === 'pending') {
    sideCard.append(node('p', '待确认期间显示按当前订阅预演的组；确认事务内会重新匹配，并记录真实投递 ID。', 'muted'));
  }
  if (candidate.receipt) sideCard.append(receiptPanel(candidate.receipt));

  const actions = node('div', '', 'button-row review-actions');
  if (candidate.status === 'pending') {
    actions.append(button('确认发布 · 关联最终信封 / 实际去向 / 稳定回执', async () => {
      const wanted = $('cand-final-key').value.trim();
      const payload = {fingerprint: candidate.fingerprint};
      if (wanted && wanted !== candidate.candidate_key) payload.key = wanted;
      try {
        const reply = await api(`${channelPath()}/candidates/${candidate.candidate_id}/confirm`, payload);
        notice(reply.repeated
          ? `重复确认：返回同一稳定回执 #${reply.message_id}`
          : `已确认发布，稳定回执 #${reply.message_id}`);
        renderCandidate(reply);
        await refresh();
        await loadCandidates();
      } catch (problem) {
        if (problem.status === 409 && problem.body?.review) {
          const fresh = await api(`${channelPath()}/candidates/${candidate.candidate_id}`);
          renderCandidate(fresh, problem.body);
          return;
        }
        throw problem;
      }
    }, {className: 'primary'}));
    actions.append(button('放弃候选', async () => {
      await api(`${channelPath()}/candidates/${candidate.candidate_id}/discard`, {});
      notice(`候选单 #${candidate.candidate_id} 已放弃，未发布任何消息`);
      dialog.close();
      await loadCandidates();
    }, {className: 'danger'}));
  } else if (candidate.status === 'confirmed') {
    actions.append(button('再次提交确认（验证稳定回执）', async () => {
      const reply = await api(`${channelPath()}/candidates/${candidate.candidate_id}/confirm`,
        {fingerprint: candidate.fingerprint});
      notice(`稳定回执不变：消息 #${reply.message_id}，${Object.keys(reply.fanout || {}).length} 个去向`);
      renderCandidate(reply);
    }));
    if (candidate.message_id) {
      actions.append(button('查看消息轨迹', async () => {
        dialog.close();
        await trace(candidate.message_id);
      }, {className: 'quiet'}));
    }
    actions.append(button('完成', () => dialog.close(), {className: 'primary'}));
  } else {
    actions.append(button('关闭', () => dialog.close(), {className: 'primary'}));
  }
  sideCard.append(actions);
  grid.append(sideCard);
  body.append(grid);
}

async function loadCandidates() {
  const bar = $('candidate-bar');
  if (!bar) return;
  const channel = state.channel;
  if (!channel) { bar.hidden = true; return; }
  let reply;
  try {
    reply = await api(`${channelPath()}/candidates`);
  } catch {
    bar.hidden = true;
    return;
  }
  if (state.channel !== channel) return;
  const pending = (reply.candidates || []).filter(item => item.pending);
  bar.hidden = pending.length === 0;
  $('candidate-count').textContent = pending.length
    ? `${pending.length} 个未确认候选：仅暂存信封，不会悄悄发布`
    : '';
  $('candidate-list').replaceChildren(...pending.slice(0, 6).map(item =>
    button(`#${item.id} · ${item.candidate_key}`, async () => {
      const doc = await api(`${channelPath()}/candidates/${item.id}`);
      openCandidate(doc);
    }, {className: 'quiet mini'})));
}

$('review-close').addEventListener('click', () => $('review-dialog').close());
$('review-dialog').addEventListener('click', event => {
  if (event.target === $('review-dialog')) $('review-dialog').close();
});
$('review-dialog').addEventListener('close', () => { void loadCandidates(); });

onForm('publish', async (values, mode) => {
  const channel = state.channel;
  let reply;
  try {
    reply = await api(`${channelPath()}/${mode || 'messages'}`, {
      ...values, tags: parse(values.tags, '标签'), payload: parse(values.payload, '载荷'),
      delay_seconds: Number(values.delay_seconds),
    });
  } catch (problem) {
    // A key clash opens the review/re-publication workflow instead of leaving
    // the operator with only an error notice.
    if (mode !== 'preview' && problem.status === 409 && problem.body?.review) {
      if (state.channel !== channel) return;
      openReview(problem.body.review);
      return;
    }
    throw problem;
  }
  if (state.channel !== channel) return;
  $('publication-result').textContent = mode === 'preview'
    ? (reply.groups.length ? `匹配：${reply.groups.join('、')}` : '当前无匹配消费组')
    : `消息 #${reply.message_id} · ${reply.repeated ? '已复用原回执' : '已发布'} · ${Object.keys(reply.fanout).length} 个消费组`;
  if (mode !== 'preview') {
    await refresh();
    await loadCandidates();
  }
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
