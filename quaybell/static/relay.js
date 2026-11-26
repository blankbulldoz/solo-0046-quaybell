export const $ = id => document.getElementById(id);
export const TEXT = {
  ready: '可领取', blocked: '顺序阻挡', delayed: '未到就绪时间', paused: '暂停取件',
  leased: '租约中', expired: '租约超时', acked: '已确认', nacked: '已拒绝',
  waiting: '等待中', dead: '死信', open: '未应答', leased_event: '发放租约',
  published: '已发布', redriven: '重投',
  direct: '直接', bridged: '桥接', bridged_event: '已桥接', bridge_received: '桥接收入',
  bridge_configured: '桥接建立', bridge_revised: '桥接换版',
};

export async function api(path, data) {
  const options = data === undefined ? {} : {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data),
  };
  const response = await fetch(`/v1${path}`, options);
  const body = await response.json();
  if (!response.ok) throw new Error(body.message || `请求失败（${response.status}）`);
  return body;
}

export function node(tag, content = '', className = '') {
  const element = document.createElement(tag);
  if (content instanceof Node) element.append(content);
  else element.textContent = content;
  if (className) element.className = className;
  return element;
}

export function button(label, action, {className = 'quiet', disabled = false} = {}) {
  const element = node('button', label, className);
  element.type = 'button';
  element.disabled = disabled;
  element.addEventListener('click', () => run(action, element));
  return element;
}

let noticeTimer;
export function notice(message, error = false) {
  const target = $('notice');
  target.textContent = message;
  target.dataset.error = error;
  target.hidden = false;
  clearTimeout(noticeTimer);
  noticeTimer = setTimeout(() => { target.hidden = true; }, 5200);
}

export async function run(action, control) {
  if (control) control.disabled = true;
  try { await action(); }
  catch (problem) { notice(problem.message || '操作失败', true); }
  finally { if (control) control.disabled = false; }
}

export function onForm(id, listener) {
  $(id).addEventListener('submit', event => {
    event.preventDefault();
    const values = Object.fromEntries(new FormData(event.currentTarget));
    run(() => listener(values, event.submitter?.value), event.submitter);
  });
}

export function parse(text, label) {
  try { return JSON.parse(text); }
  catch { throw new Error(`${label}须为有效 JSON`); }
}

export function clock(seconds) {
  return seconds == null ? '—' : new Date(seconds * 1000).toLocaleString('zh-CN', {hour12: false});
}

export function status(value) {
  const span = node('span', TEXT[value] || value, 'status');
  span.dataset.state = value;
  return span;
}

export function rows(id, values, columns, empty) {
  const target = $(id);
  if (!values.length) {
    const cell = node('td', empty, 'muted');
    cell.colSpan = columns;
    const row = node('tr', cell);
    target.replaceChildren(row);
    return;
  }
  target.replaceChildren(...values.map(cells => {
    const row = node('tr');
    for (const cell of cells) row.append(node('td', cell));
    return row;
  }));
}
