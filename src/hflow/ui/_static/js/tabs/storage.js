// Storage tab: registered data roots (polled every 10s, catalog stats
// fetched lazily per root) and a per-root file browser (no polling).

import { api } from '../api.js';
import { Poller } from '../app.js';
import {
  h, icon, chip, callout, confirmInline, emptyState, loadingRow, relTime,
  syncRows, toast, commandBlock,
} from '../ui.js';

const POLL_MS = 10000;
const BUCKET_SCHEMES = ['gs', 's3', 'az'];

let container = null;
let poller = null;
let roots;                    // last successful roots list
const catalogs = new Map();   // root_id -> last catalog payload
let params = {};
let rootsMode = null;
let rootsBody = null;
let rootsTbody = null;
let addRootButton = null;
let addFormSlot = null;

export function mount(section, routeParams) {
  container = section;
  params = routeParams;
  if (!poller) poller = new Poller(loadRoots, POLL_MS);
  if (params.view === 'browse') {
    renderBrowse(params.root, params.prefix || '');
  } else {
    buildRootsShell();
    renderRoots();
    poller.start();
  }
}

export function unmount() {
  poller.stop();
}

// --- roots list -------------------------------------------------------------------

async function loadRoots() {
  try {
    roots = (await api('/api/storage/roots')).roots;
  } catch {
    return; // keep last data; the banner reports connection loss
  }
  if (params.view === 'browse') return;
  renderRoots();
  for (const root of roots) refreshCatalog(root.root_id);
}

async function refreshCatalog(rootId) {
  try {
    catalogs.set(rootId, await api(`/api/storage/roots/${encodeURIComponent(rootId)}/catalog`));
  } catch {
    return; // leave the placeholder dashes
  }
  const row = rootsTbody && rootsTbody.querySelector(`tr[data-key="${CSS.escape(rootId)}"]`);
  if (row) updateCatalogCells(row, catalogs.get(rootId));
}

function buildRootsShell() {
  addRootButton = h('button', {
    class: 'button button--primary', type: 'button', onclick: showAddForm,
  }, '+ Add data root');
  addFormSlot = h('div', { class: 'inline-form-slot' });
  rootsBody = h('div', { class: 'tab-body' });
  rootsMode = null;
  container.replaceChildren(
    h('div', { class: 'tab-toolbar' },
      h('p', { class: 'subtitle' }, 'Registered data roots and their catalogs.'),
      addRootButton),
    addFormSlot,
    rootsBody);
}

function renderRoots() {
  const mode = roots === undefined ? 'loading' : roots.length === 0 ? 'empty' : 'table';
  addRootButton.hidden = mode === 'empty';
  if (mode !== rootsMode) {
    rootsMode = mode;
    if (mode === 'empty') {
      rootsBody.replaceChildren(emptyState({
        glyph: icon('folder'),
        title: 'No data roots',
        body: 'Register a data root to see its catalog stats and browse its files.',
        trailing: h('button', {
          class: 'button button--primary', type: 'button', onclick: showAddForm,
        }, '+ Add data root'),
      }));
    } else {
      rootsTbody = h('tbody', null, mode === 'loading' ? loadingRow(7) : null);
      rootsBody.replaceChildren(rootsTable(rootsTbody));
    }
  }
  if (mode === 'table') {
    syncRows(rootsTbody, roots, (root) => root.root_id, createRootRow, updateRootRow);
  }
}

function rootsTable(tbodyEl) {
  return h('table', { class: 'data-table roots-table' },
    h('thead', null, h('tr', null,
      h('th', { class: 'col-root' }, 'Root'),
      h('th', { class: 'col-kind' }, 'Kind'),
      h('th', { class: 'col-episodes th-num' }, 'Episodes'),
      h('th', { class: 'col-quarantined th-num' }, 'Quarantined'),
      h('th', { class: 'col-activity' }, 'Last activity'),
      h('th', { class: 'col-actions' }),
      h('th', { class: 'col-chevron', 'aria-label': 'Browse' }))),
    tbodyEl);
}

function rootKindLabel(root) {
  if (root.kind === 'local') return 'local';
  const scheme = root.root.split('://', 1)[0];
  return BUCKET_SCHEMES.includes(scheme) ? scheme : 'bucket';
}

function rootSignature(root) {
  return `${root.root}|${root.kind}|${root.implicit}|${root.added_at}`;
}

function createRootRow(root) {
  const catalog = catalogs.get(root.root_id);
  const tr = h('tr', {
    class: 'row-clickable',
    onclick: (event) => {
      if (event.target.closest('button, a')) return;
      location.hash = browseHash(root.root_id, '');
    },
  },
    h('td', { class: 'col-root cell-mono', title: root.root }, root.root),
    h('td', { class: 'col-kind' }, chip(rootKindLabel(root))),
    h('td', { class: 'col-episodes cell-num' }, episodesContent(catalog)),
    h('td', { class: 'col-quarantined cell-num' }, quarantinedContent(catalog)),
    h('td', { class: 'col-activity cell-dim' }, activityContent(catalog)),
    h('td', { class: 'col-actions' }, root.implicit ? null : removeControl(root)),
    h('td', { class: 'col-chevron' },
      h('a', { class: 'chevron-link', href: browseHash(root.root_id, ''), 'aria-label': `Browse ${root.root}` },
        icon('chevron'))));
  tr._rootSig = rootSignature(root);
  tr._catalogSig = catalog ? JSON.stringify(catalog) : null;
  return tr;
}

function updateRootRow(tr, root) {
  const signature = rootSignature(root);
  if (tr._rootSig !== signature) {
    const fresh = createRootRow(root);
    tr.replaceChildren(...fresh.children);
    tr._rootSig = signature;
    tr._catalogSig = fresh._catalogSig;
    return;
  }
  const catalog = catalogs.get(root.root_id);
  if (catalog) updateCatalogCells(tr, catalog);
}

function updateCatalogCells(tr, catalog) {
  const signature = JSON.stringify(catalog);
  if (tr._catalogSig === signature) return;
  tr._catalogSig = signature;
  tr.querySelector('.col-episodes').replaceChildren(episodesContent(catalog));
  tr.querySelector('.col-quarantined').replaceChildren(quarantinedContent(catalog));
  tr.querySelector('.col-activity').replaceChildren(activityContent(catalog));
}

function episodesContent(catalog) {
  return h('span', null, catalog && catalog.present ? String(catalog.episode_count) : '—');
}

function quarantinedContent(catalog) {
  if (!catalog || !catalog.present) return h('span', null, '—');
  return h('span', { class: catalog.quarantined_count > 0 ? 'warn' : null },
    String(catalog.quarantined_count));
}

function activityContent(catalog) {
  const ts = catalog && catalog.present ? catalog.latest_recorded_at : null;
  return ts ? h('span', { dataset: { relTs: ts }, title: ts }, relTime(ts)) : h('span', null, '—');
}

function removeControl(root) {
  const wrap = h('span', { class: 'row-actions' });
  const removeButton = h('button', {
    class: 'button button--danger-ghost button--small', type: 'button',
    onclick: () => {
      const cell = wrap.parentElement;
      confirmInline(cell, 'Remove?', () => cell.replaceChildren(wrap), async () => {
        try {
          await api(`/api/storage/roots/${encodeURIComponent(root.root_id)}`, { method: 'DELETE' });
        } catch (err) {
          toast(`Request failed: ${err.message}`);
          cell.replaceChildren(wrap);
        }
        poller.kick();
      });
    },
  }, 'Remove');
  wrap.append(removeButton);
  return wrap;
}

// --- add-root inline form ------------------------------------------------------------

function showAddForm() {
  const existingInput = addFormSlot.querySelector('input');
  if (existingInput) { existingInput.focus(); return; }
  const errorLine = h('div', { class: 'field-error', hidden: true });
  const input = h('input', {
    class: 'input input--mono', type: 'text', spellcheck: 'false',
    placeholder: './data or gs://bucket/prefix',
    oninput: () => { input.classList.remove('invalid'); errorLine.hidden = true; },
  });
  const closeForm = () => addFormSlot.replaceChildren();
  const showError = (message, hint) => {
    input.classList.add('invalid');
    errorLine.hidden = false;
    errorLine.replaceChildren(message, hint ? commandBlock(hint) : null);
  };
  const form = h('form', {
    class: 'inline-form',
    onsubmit: async (event) => {
      event.preventDefault();
      const value = input.value.trim();
      if (!value) { input.focus(); return; }
      const clientError = validateRootInput(value);
      if (clientError) { showError(clientError); return; }
      try {
        await api('/api/storage/roots', { method: 'POST', body: { root: value } });
      } catch (err) {
        showError(err.message, err.hint);
        return;
      }
      toast('Data root registered');
      closeForm();
      poller.kick();
    },
    onkeydown: (event) => { if (event.key === 'Escape') closeForm(); },
  },
    h('div', { class: 'inline-form-main' },
      h('div', { class: 'inline-form-fields' }, input),
      errorLine),
    h('button', { class: 'button button--primary', type: 'submit' }, 'Add root'),
    h('button', { class: 'button button--ghost', type: 'button', onclick: closeForm }, 'Cancel'));
  addFormSlot.replaceChildren(form);
  input.focus();
}

function validateRootInput(value) {
  if (value.includes('://')) {
    const scheme = value.split('://', 1)[0];
    if (!['gs', 's3', 'az', 'file'].includes(scheme)) {
      return 'Expected a local path or a gs://, s3://, or az:// URL.';
    }
  }
  if (roots && roots.some((root) => root.root === value)) {
    return 'This data root is already registered.';
  }
  return null;
}

// --- browse view -------------------------------------------------------------------------

function browseHash(rootId, prefix) {
  return `#/storage/browse?root=${encodeURIComponent(rootId)}&prefix=${encodeURIComponent(prefix)}`;
}

async function renderBrowse(rootId, prefix) {
  const crumbs = h('div', { class: 'browse-crumbs' }, breadcrumbNodes(rootId, prefix));
  const browseBody = h('div', { class: 'tab-body' });
  container.replaceChildren(crumbs, browseBody);
  const tbody = h('tbody', null, loadingRow(2));
  browseBody.replaceChildren(browseTable(tbody));
  if (roots === undefined) {
    try {
      roots = (await api('/api/storage/roots')).roots;
      crumbs.replaceChildren(...breadcrumbNodes(rootId, prefix));
    } catch {
      // Breadcrumb falls back to the root id; the listing fetch reports errors.
    }
  }
  let listing;
  try {
    const query = prefix ? `?prefix=${encodeURIComponent(prefix)}` : '';
    listing = await api(`/api/storage/roots/${encodeURIComponent(rootId)}/browse${query}`);
  } catch (err) {
    browseBody.replaceChildren(browseErrorCallout(err));
    return;
  }
  const rows = [
    ...[...listing.directories].sort().map((entry) => directoryRow(rootId, prefix, entry)),
    ...[...listing.files].sort((a, b) => a.name.localeCompare(b.name)).map(fileRow),
  ];
  if (rows.length === 0) {
    rows.push(h('tr', { class: 'empty-row' }, h('td', { colspan: '2' }, 'Empty prefix.')));
  }
  tbody.replaceChildren(...rows);
}

function breadcrumbNodes(rootId, prefix) {
  const rootInfo = roots && roots.find((root) => root.root_id === rootId);
  const rootLabel = rootInfo ? rootInfo.root : rootId;
  const segments = prefix ? prefix.split('/').filter(Boolean) : [];
  const nodes = [h('a', { class: 'crumb-back', href: '#/storage' }, '‹ Roots')];
  nodes.push(segments.length
    ? h('a', { class: 'crumb', href: browseHash(rootId, ''), title: rootLabel }, rootLabel)
    : h('span', { class: 'crumb crumb--current', title: rootLabel }, rootLabel));
  segments.forEach((segment, index) => {
    nodes.push(h('span', { class: 'crumb-sep' }, '/'));
    const subPrefix = segments.slice(0, index + 1).join('/');
    nodes.push(index === segments.length - 1
      ? h('span', { class: 'crumb crumb--current' }, segment)
      : h('a', { class: 'crumb', href: browseHash(rootId, subPrefix) }, segment));
  });
  return nodes;
}

function browseTable(tbodyEl) {
  return h('table', { class: 'data-table browse-table' },
    h('thead', null, h('tr', null,
      h('th', { class: 'col-name' }, 'Name'),
      h('th', { class: 'col-size th-num' }, 'Size'))),
    tbodyEl);
}

function directoryRow(rootId, prefix, entry) {
  // Servers may list directories as bare names or as prefixed paths; accept both.
  const clean = entry.replace(/\/+$/, '');
  const nextPrefix = clean.includes('/') ? clean : prefix ? `${prefix}/${clean}` : clean;
  const name = clean.split('/').pop();
  const href = browseHash(rootId, nextPrefix);
  return h('tr', {
    class: 'row-clickable',
    onclick: (event) => { if (!event.target.closest('a')) location.hash = href; },
  },
    h('td', { class: 'col-name' },
      h('span', { class: 'browse-name' },
        icon('folder'),
        h('a', { class: 'cell-mono', href }, `${name}/`))),
    h('td', { class: 'col-size cell-num cell-dim' }, '—'));
}

function fileRow(file) {
  return h('tr', null,
    h('td', { class: 'col-name', title: file.name },
      h('span', { class: 'browse-name' },
        h('span', { class: 'cell-mono' }, file.name))),
    h('td', { class: 'col-size cell-num' }, formatBytes(file.size)));
}

function formatBytes(bytes) {
  if (bytes == null) return '—';
  if (bytes < 1024) return `${bytes} B`;
  let value = bytes / 1024;
  for (const unit of ['KB', 'MB']) {
    if (value < 1024) return `${value.toFixed(1)} ${unit}`;
    value /= 1024;
  }
  return `${value.toFixed(1)} GB`;
}

function browseErrorCallout(err) {
  if (err.hint && err.hint.includes('hflow[bucket]')) {
    return callout({
      title: 'Bucket backend not installed',
      body: 'Bucket roots need the optional obstore backend. Local paths work without it.',
      command: err.hint,
    });
  }
  return callout({ title: 'Could not browse this root', body: err.message, command: err.hint });
}
