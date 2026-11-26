import {$, api, clock, node, notice, status} from './relay.js';

const state = {messageId: null, doc: null, frozen: false, pair: null};

const ABSENCE_BADGE = {
  group_created_after_publication: '后来才建的组 · 发布时不存在',
  tags_did_not_match: '当时标签不匹配',
  group_unknown: '消费组不存在',
};

function option(value, label, selected) {
  const item = node('option', label || value);
  item.value = value;
  item.selected = selected;
  return item;
}

export function groupNames(facts, board) {
  const names = [];
  for (const group of board?.groups || []) names.push(group.name);
  for (const delivery of facts?.deliveries || []) {
    if (delivery.group_name && !names.includes(delivery.group_name)) names.push(delivery.group_name);
  }
  return names;
}

export function setupPicker({messageId, names, pair}) {
  state.messageId = messageId;
  const a = $('compare-a'), b = $('compare-b');
  a.replaceChildren(...names.map(name => option(name, name, name === pair?.a)));
  b.replaceChildren(...names.map(name => option(name, name, name === pair?.b)));
  const [first, second] = names;
  if (!pair?.a && first) a.value = first;
  if (!pair?.b) b.value = names[1] || first || '';
  if (pair?.a && names.includes(pair.a)) a.value = pair.a;
  if (pair?.b && names.includes(pair.b)) b.value = pair.b;
  const enough = names.length >= 2;
  $('compare-run').disabled = !enough;
  $('compare-hint').textContent = enough
    ? '两个组的当前状态与尝试路径并列展示；标记区分当时未分发、等待与重投新身份。'
    : '该消息所在频道至少需要两个消费组才能对照（含发布后才建立的组）。';
}

function marker(text, kind) {
  return node('span', text, `marker marker-${kind}`);
}

function renderMarkers(side) {
  const wrap = node('div', '', 'badges');
  if (side.status === 'not_dispatched') {
    wrap.append(marker('当时没有分发', 'absent'));
    const code = side.absence_reason || side.absence?.code;
    if (ABSENCE_BADGE[code]) wrap.append(marker(ABSENCE_BADGE[code], 'reason'));
    if (side.absence?.label) wrap.append(node('p', side.absence.label, 'muted absence-note'));
  }
  if (side.waiting) {
    wrap.append(marker(side.retrying ? '重试等待中' : '等待中', 'waiting'));
    if (side.current?.label) wrap.append(node('span', side.current.label, 'muted'));
    if (side.current?.ready_at != null) {
      wrap.append(node('span', `最早就绪 ${clock(side.current.ready_at)}`, 'muted'));
    }
  }
  if (side.status === 'leased') wrap.append(marker('租约处理中', 'leased'));
  if (side.status === 'acked') wrap.append(marker('已确认', 'acked'));
  if (side.terminal_failure || side.status === 'dead') wrap.append(marker('终败 · 死信', 'dead'));
  for (const identity of side.new_identities || []) {
    wrap.append(marker(`重投后出现的新身份 #${identity.delivery_id}（源投递 #${identity.parent_id}）`,
      'successor'));
  }
  if (side.paused) wrap.append(marker('组已暂停取件', 'paused'));
  return wrap;
}

function pathNode(entry) {
  const item = node('li', '', `path-node path-${entry.kind}`);
  if (entry.kind === 'attempt') {
    const outcome = {acked: '已确认', nacked: '已拒绝', expired: '租约超时', open: '未应答'}[entry.outcome]
      || entry.outcome;
    item.append(node('span', `第 ${entry.number} 次尝试 · ${entry.worker} · ${outcome}`, 'path-head'));
    item.append(node('span', `开始 ${clock(entry.started)} · 截止 ${clock(entry.expires)}`
      + (entry.finished ? ` · 结束 ${clock(entry.finished)}` : ''), 'muted path-time'));
    if (entry.reason) item.append(node('span', `原因：${entry.reason}`, 'path-reason'));
  } else {
    const successor = entry.kind === 'redrive_identity';
    item.append(node('span', successor
      ? `重投新身份 #${entry.delivery_id} ← 源 #${entry.parent_id}`
      : `原始分发 #${entry.delivery_id}`, 'path-head'));
    item.append(node('span', `${clock(entry.at)} · 当前 ${entry.state}`, 'muted path-time'));
    if (successor) item.append(marker('新身份', 'successor'));
  }
  return item;
}

function renderSide(side) {
  const card = node('article', '', 'compare-card');
  card.dataset.status = side.status || 'unknown';
  card.dataset.classification = side.classification || '';
  const head = node('header', '', 'compare-card-head');
  head.append(node('h4', side.group_name));
  const badge = status(side.label || side.status || 'unknown');
  badge.dataset.state = ({not_dispatched: 'blocked', unknown_group: 'dead'}[side.status]) || side.status;
  head.append(badge);
  card.append(head, renderMarkers(side));

  const meta = [];
  if (side.tags && Object.keys(side.tags).length) meta.push(`标签 ${JSON.stringify(side.tags)}`);
  if (side.existed_at_publication === false) meta.push('发布后建组');
  if (side.subscription_id != null) meta.push(`订阅 #${side.subscription_id}`);
  if (meta.length) card.append(node('p', meta.join(' · '), 'muted compare-meta'));

  card.append(node('h5', `状态与尝试路径（${side.delivery_count ?? 0} 条投递 / ${side.attempt_count ?? 0} 次尝试）`));
  const path = node('ol', '', 'compare-path');
  const entries = side.path || [];
  if (entries.length) path.append(...entries.map(pathNode));
  else path.append(node('li', '没有任何分发或尝试：该组在发布时刻不在分发集合内。', 'muted'));
  card.append(path);
  return card;
}

async function verifyFooter(doc) {
  const verdict = $('compare-verify');
  try {
    const result = await api('/evidence/verify', doc);
    if (result.valid) {
      verdict.textContent = '✓ 摘要一致，快照未被改动';
      verdict.dataset.ok = 'true';
    } else {
      verdict.textContent = `✗ ${result.reason || '摘要不符'}`;
      verdict.dataset.ok = 'false';
    }
  } catch {
    verdict.textContent = '暂无法请求摘要校验接口，可用 quaybell.codec 按 sha256 离线复核';
    verdict.dataset.ok = 'unknown';
  }
}

export function renderSnapshot(doc, {frozen = false} = {}) {
  state.doc = doc;
  state.frozen = frozen;
  state.pair = {a: doc.groups?.[0], b: doc.groups?.[1]};
  $('compare-result').replaceChildren(renderSide(doc.left), renderSide(doc.right));
  const download = $('compare-download');
  const params = new URLSearchParams({a: doc.groups?.[0] || '', b: doc.groups?.[1] || ''});
  download.href = `/v1/messages/${doc.message_id}/compare/evidence?${params}`;
  download.setAttribute('download',
    `quaybell-compare-${doc.message_id}-${doc.groups?.[0]}-vs-${doc.groups?.[1]}.json`);
  const foot = $('compare-snapshot-foot');
  foot.hidden = false;
  $('compare-observed').textContent = `快照观察时刻：${clock(doc.observed_at)}（Unix ${doc.observed_at}）`;
  $('compare-sha').textContent = `sha256:${(doc.sha256 || '').slice(0, 16)}…`;
  $('compare-sha').title = doc.sha256 || '';
  const banner = $('compare-freeze-banner');
  if (frozen) {
    banner.hidden = false;
    banner.textContent = '这是导出/共享的冻结对照：内容与观察时刻固定，后续尝试不会改变它；可凭摘要核对。';
    banner.dataset.frozen = 'true';
  } else {
    banner.hidden = true;
  }
  verifyFooter(doc);
}

export async function runCompare() {
  const id = state.messageId;
  const a = $('compare-a').value, b = $('compare-b').value;
  if (!id || !a || !b || a === b) {
    $('compare-hint').textContent = '请选择两个不同的消费组。';
    return;
  }
  const query = `?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`;
  const doc = await api(`/messages/${id}/compare${query}`);
  history.replaceState(null, '', `#compare=${id}&a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`);
  renderSnapshot(doc);
}

function downloadSnapshot() {
  if (!state.doc) return;
  const doc = state.doc;
  const blob = new Blob([JSON.stringify(doc, null, 2)], {type: 'application/json'});
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `quaybell-compare-${doc.message_id}-${doc.groups?.[0]}-vs-${doc.groups?.[1]}.json`;
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 4000);
  notice('对照快照已导出；其中 observed_at 与 sha256 固定了观察时刻');
}

function encodeSnapshot(doc) {
  const json = JSON.stringify(doc);
  return btoa(unescape(encodeURIComponent(json)))
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function decodeSnapshot(token) {
  const base64 = token.replace(/-/g, '+').replace(/_/g, '/');
  return JSON.parse(decodeURIComponent(escape(atob(base64))));
}

async function shareSnapshot() {
  if (!state.doc) return;
  const url = `${location.origin}${location.pathname}#snapshot=${encodeSnapshot(state.doc)}`;
  const field = $('compare-share-url');
  try {
    await navigator.clipboard.writeText(url);
    if (field) field.hidden = true;
    notice('已复制冻结对照的共享链接；打开后可凭摘要核对观察时刻');
  } catch {
    if (field) {
      field.value = url;
      field.hidden = false;
      field.focus();
      field.select();
    }
    notice('浏览器未允许剪贴板，请手动复制下方共享链接', true);
  }
}

export function initCompareChrome() {
  $('compare-run').addEventListener('click', () => runGuarded(runCompare));
  $('compare-download').addEventListener('click', event =>
    runGuarded(async () => { event.preventDefault(); downloadSnapshot(); }));
  $('compare-share').addEventListener('click', () => runGuarded(shareSnapshot));
  for (const id of ['compare-a', 'compare-b']) {
    $(id).addEventListener('change', () => runGuarded(runCompare));
  }
}

async function runGuarded(action) {
  try { await action(); }
  catch (problem) { notice(problem.message || '对照加载失败', true); }
}

export async function handleCompareHash(onLive) {
  if (location.hash.startsWith('#snapshot=')) {
    try {
      const doc = decodeSnapshot(location.hash.slice('#snapshot='.length));
      $('trace-title').textContent = `消息 #${doc.message_id} · 共享对照快照`;
      $('trace-meta').textContent = `${doc.channel || ''} · 冻结于 ${clock(doc.observed_at)}`;
      for (const id of ['trace-envelope', 'trace-deliveries', 'trace-events']) {
        $(id).textContent = '共享快照只包含两组对照与摘要；实时信封和完整轨迹请在操作台打开该消息。';
      }
      setupPicker({messageId: doc.message_id, names: doc.groups || [], pair: {a: doc.groups?.[0], b: doc.groups?.[1]}});
      renderSnapshot(doc, {frozen: true});
      $('trace').showModal();
    } catch (problem) {
      notice(`共享快照无法解析：${problem.message}`, true);
    }
    return;
  }
  if (location.hash.startsWith('#compare=')) {
    const params = new URLSearchParams(location.hash.slice(1));
    const id = Number(params.get('compare'));
    if (id) await onLive(id, {a: params.get('a'), b: params.get('b')});
  }
}
