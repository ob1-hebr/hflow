// Shared DOM helpers and small components for the hflow dashboard.
//
// Everything renders through h(), which turns string children into text
// nodes, so run ids, paths, and secret keys can never be interpreted as
// markup. The single innerHTML exception is ICONS below: a dict of trusted,
// static SVG strings that never contain data.

// --- icons ----------------------------------------------------------------

const ICONS = {
  check: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 8.5l3.5 3.5L13 4.5"/></svg>',
  x: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M4 4l8 8m0-8l-8 8"/></svg>',
  copy: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="5.5" y="5.5" width="8" height="8" rx="1.5"/><path d="M10.5 4.5V4a1.5 1.5 0 0 0-1.5-1.5H4A1.5 1.5 0 0 0 2.5 4v5A1.5 1.5 0 0 0 4 10.5h.5"/></svg>',
  key: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="5" cy="11" r="2.5"/><path d="M6.8 9.2L13 3m-2.5 2.5l2 2"/></svg>',
  'arrow-up-right': '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 11.5l7-7M6 4.5h5.5V10"/></svg>',
  folder: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"><path d="M2 4.5A1.5 1.5 0 0 1 3.5 3h2.8L8 5h4.5A1.5 1.5 0 0 1 14 6.5v5a1.5 1.5 0 0 1-1.5 1.5h-9A1.5 1.5 0 0 1 2 11.5z"/></svg>',
  chevron: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3.5L10.5 8L6 12.5"/></svg>',
};

export function icon(name) {
  const span = document.createElement('span');
  span.className = 'icon';
  span.setAttribute('aria-hidden', 'true');
  span.innerHTML = ICONS[name]; // trusted static markup only, never data
  return span;
}

// --- DOM builder -------------------------------------------------------------

export function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  if (attrs) {
    for (const [key, value] of Object.entries(attrs)) {
      if (value == null) continue;
      if (key === 'class') el.className = value;
      else if (key === 'dataset') Object.assign(el.dataset, value);
      else if (key.startsWith('on')) el.addEventListener(key.slice(2).toLowerCase(), value);
      else if (key === 'value' || key === 'checked' || key === 'disabled' || key === 'hidden') el[key] = value;
      else el.setAttribute(key, value);
    }
  }
  const append = (child) => {
    if (child == null || child === false) return;
    if (Array.isArray(child)) { child.forEach(append); return; }
    el.append(child.nodeType ? child : document.createTextNode(String(child)));
  };
  children.forEach(append);
  return el;
}

// --- keyed row reconciliation --------------------------------------------------
//
// Reuses <tr> nodes by data-key so hover state, open confirms, and pulse
// animation phase survive poll refreshes. updateRow implementations are
// expected to write a cell only when its backing value changed.

export function syncRows(tbody, items, keyFn, createRow, updateRow) {
  const existingByKey = new Map();
  for (const row of tbody.children) existingByKey.set(row.dataset.key, row);
  let cursor = tbody.firstChild;
  for (const item of items) {
    const key = String(keyFn(item));
    let row = existingByKey.get(key);
    if (row) {
      existingByKey.delete(key);
      updateRow(row, item);
    } else {
      row = createRow(item);
      row.dataset.key = key;
    }
    if (row === cursor) cursor = cursor.nextSibling;
    else tbody.insertBefore(row, cursor);
  }
  for (const leftover of existingByKey.values()) leftover.remove();
}

// --- time formatting and the 1s ticker --------------------------------------------

export function relTime(iso) {
  if (!iso) return '—';
  const secondsAgo = (Date.now() - Date.parse(iso)) / 1000;
  if (secondsAgo < 45) return 'just now';
  if (secondsAgo < 3600) return `${Math.max(1, Math.floor(secondsAgo / 60))}m ago`;
  if (secondsAgo < 86400) return `${Math.floor(secondsAgo / 3600)}h ago`;
  if (secondsAgo < 7 * 86400) return `${Math.floor(secondsAgo / 86400)}d ago`;
  return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

export function duration(totalSeconds) {
  if (totalSeconds == null || Number.isNaN(totalSeconds)) return '—';
  const s = Math.max(0, Math.round(totalSeconds));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

setInterval(() => {
  for (const el of document.querySelectorAll('[data-rel-ts]')) {
    if (el.offsetParent) el.textContent = relTime(el.dataset.relTs);
  }
  for (const el of document.querySelectorAll('[data-dur-since]')) {
    if (el.offsetParent) el.textContent = duration((Date.now() - Date.parse(el.dataset.durSince)) / 1000);
  }
}, 1000);

// --- run/stage indicators ------------------------------------------------------------

export function statusDot(state) {
  const glyph = state === 'success' ? icon('check')
    : state === 'failed' ? icon('x')
    : h('span', { class: 'status__dot' });
  return h('span', { class: `status status--${state}` },
    glyph,
    h('span', { class: 'status__label' }, state));
}

const STAGE_ORDER = ['sync', 'meta', 'labels', 'media'];

export function stageStrip(stages) {
  const described = STAGE_ORDER.map((name) => `${name} -- ${(stages && stages[name]) || 'pending'}`);
  const strip = h('span', { class: 'stage-strip', role: 'img', 'aria-label': described.join(', ') });
  for (const name of STAGE_ORDER) {
    const state = (stages && stages[name]) || 'pending';
    strip.append(h('span', { class: `stage-seg stage-seg--${state}`, title: `${name} -- ${state}` }));
  }
  return strip;
}

export function stageStripDemo() {
  return h('span', { class: 'stage-strip stage-strip--demo', 'aria-hidden': 'true' },
    STAGE_ORDER.map(() => h('span', { class: 'stage-seg' })));
}

export function chip(text) {
  return h('span', { class: 'chip' }, text);
}

// --- clipboard, toasts ------------------------------------------------------------------

export function copyButton(getText, title = 'Copy') {
  return h('button', {
    class: 'icon-button', type: 'button', title,
    onclick: async (event) => {
      event.stopPropagation();
      try {
        await navigator.clipboard.writeText(typeof getText === 'function' ? getText() : getText);
        toast('Copied');
      } catch {
        toast('Request failed: clipboard unavailable');
      }
    },
  }, icon('copy'));
}

export function toast(message) {
  const region = document.getElementById('toast-region');
  const el = h('div', { class: 'toast' }, message);
  region.append(el);
  setTimeout(() => {
    el.classList.add('toast--out');
    setTimeout(() => el.remove(), 200);
  }, 4000);
}

// --- callouts, empty states, loading ---------------------------------------------------------

export function commandBlock(command) {
  return h('div', { class: 'command-block' },
    h('code', null, command),
    copyButton(() => command, 'Copy command'));
}

export function callout({ title, body, command }) {
  return h('div', { class: 'callout' },
    h('div', { class: 'callout__title' }, title),
    h('div', { class: 'callout__body' }, body),
    command ? commandBlock(command) : null);
}

export function emptyState({ glyph, title, body, trailing }) {
  return h('div', { class: 'empty-state' },
    glyph || null,
    h('div', { class: 'empty-state__title' }, title),
    h('div', { class: 'empty-state__body' }, body),
    trailing || null);
}

export function loadingRow(colspan) {
  return h('tr', { class: 'loading-row' }, h('td', { colspan: String(colspan) }, 'Loading'));
}

// --- two-step confirm --------------------------------------------------------------------------
//
// Replaces `cell` content with "{label} [Confirm] [Cancel]" in error styling.
// Reverts via `restore` on Escape, outside click, or after 5s.

export function confirmInline(cell, label, restore, onConfirm) {
  let timeout = null;
  const cleanup = () => {
    clearTimeout(timeout);
    document.removeEventListener('keydown', onKeydown);
    document.removeEventListener('pointerdown', onPointerdown);
  };
  const revert = () => { cleanup(); restore(); };
  const onKeydown = (event) => { if (event.key === 'Escape') revert(); };
  const onPointerdown = (event) => { if (!cell.contains(event.target)) revert(); };
  document.addEventListener('keydown', onKeydown);
  document.addEventListener('pointerdown', onPointerdown);
  timeout = setTimeout(revert, 5000);
  cell.replaceChildren(h('span', { class: 'confirm-inline' },
    h('span', null, label),
    h('button', {
      class: 'button button--danger-ghost button--small', type: 'button',
      onclick: (event) => { event.stopPropagation(); cleanup(); onConfirm(); },
    }, 'Confirm'),
    h('button', {
      class: 'button button--ghost button--small', type: 'button',
      onclick: (event) => { event.stopPropagation(); revert(); },
    }, 'Cancel')));
}

// --- popover ------------------------------------------------------------------------------------

let openPopoverCleanup = null;

export function closePopover() {
  if (openPopoverCleanup) {
    openPopoverCleanup();
    openPopoverCleanup = null;
  }
}

export function popover(anchor, content) {
  closePopover();
  const pop = h('div', { class: 'popover', role: 'dialog' }, content);
  document.body.append(pop);
  const rect = anchor.getBoundingClientRect();
  pop.style.top = `${rect.bottom + window.scrollY + 6}px`;
  pop.style.left = `${Math.max(8, rect.right + window.scrollX - pop.offsetWidth)}px`;
  const onKeydown = (event) => { if (event.key === 'Escape') closePopover(); };
  const onPointerdown = (event) => {
    if (!pop.contains(event.target) && !anchor.contains(event.target)) closePopover();
  };
  document.addEventListener('keydown', onKeydown);
  document.addEventListener('pointerdown', onPointerdown);
  openPopoverCleanup = () => {
    document.removeEventListener('keydown', onKeydown);
    document.removeEventListener('pointerdown', onPointerdown);
    pop.remove();
  };
  return closePopover;
}
