// Pipelines tab: recent runs of the ingest DAG, polled every 3s.

import { api } from '../api.js';
import { Poller } from '../app.js';
import {
  h, icon, statusDot, stageStrip, stageStripDemo, chip, copyButton, callout,
  commandBlock, emptyState, loadingRow, popover, relTime, duration, syncRows,
} from '../ui.js';

const POLL_MS = 3000;

let container = null;
let poller = null;
let lastData;             // last successful /api/pipelines/runs payload
let runtimeDown = false;  // last poll returned 503 (no bundle / Airflow down)
let renderedMode = null;
let credentialsButton = null;
let body = null;
let tbody = null;

export function mount(section, _params) {
  container = section;
  if (!poller) poller = new Poller(load, POLL_MS);
  render();
  poller.start();
}

export function unmount() {
  poller.stop();
}

async function load() {
  try {
    lastData = await api('/api/pipelines/runs?limit=25');
    // dag_id arrives once at the payload level; the cells read it per run.
    for (const run of lastData.runs) run.dag_id = lastData.dag_id;
    runtimeDown = false;
  } catch (err) {
    if (err.status === 503) {
      runtimeDown = true;
    } else {
      return; // network blip: keep the last table, the banner reports it
    }
  }
  render();
}

// --- rendering ---------------------------------------------------------------

function ensureShell() {
  if (credentialsButton && container.contains(credentialsButton)) return;
  credentialsButton = h('button', {
    class: 'button button--ghost', type: 'button',
    onclick: () => openCredentials(credentialsButton),
  }, icon('key'), 'Airflow credentials');
  body = h('div', { class: 'tab-body' });
  renderedMode = null;
  container.replaceChildren(
    h('div', { class: 'tab-toolbar' },
      h('p', { class: 'subtitle' }, 'Runs of the ingest DAG. Open a run in Airflow for tasks, logs, and retries.'),
      credentialsButton),
    body);
}

function render() {
  ensureShell();
  const mode = runtimeDown ? 'down'
    : lastData === undefined ? 'loading'
    : lastData.runs.length === 0 ? 'empty' : 'table';
  credentialsButton.hidden = mode === 'down';
  if (mode !== renderedMode) {
    renderedMode = mode;
    if (mode === 'down') {
      body.replaceChildren(callout({
        title: 'Runtime not running',
        body: 'The local Airflow runtime is not reachable. Start it to see pipeline runs.',
        command: 'hflow up',
      }));
    } else if (mode === 'empty') {
      body.replaceChildren(emptyState({
        glyph: stageStripDemo(),
        title: 'No runs yet',
        body: 'When you ingest episodes, runs appear here.',
        trailing: commandBlock('hflow ingest <episode-uri>'),
      }));
    } else {
      tbody = h('tbody', null, mode === 'loading' ? loadingRow(9) : null);
      body.replaceChildren(runsTable(tbody));
    }
  }
  if (mode === 'table') {
    syncRows(tbody, sortRuns(lastData.runs), (run) => run.run_id, createRow, updateRow);
  }
}

function runsTable(tbodyEl) {
  return h('table', { class: 'data-table runs-table' },
    h('thead', null, h('tr', null,
      h('th', { class: 'col-state' }, 'State'),
      h('th', { class: 'col-pipeline' }, 'Pipeline'),
      h('th', { class: 'col-run' }, 'Run'),
      h('th', { class: 'col-stages' }, 'Stages'),
      h('th', { class: 'col-episodes th-num' }, 'Episodes'),
      h('th', { class: 'col-profile' }, 'Profile'),
      h('th', { class: 'col-started' }, 'Started'),
      h('th', { class: 'col-duration th-num' }, 'Duration'),
      h('th', { class: 'col-airflow', 'aria-label': 'Airflow' }))),
    tbodyEl);
}

// Client-side stable sort: running, then queued, then finished; each group
// newest first by run_after.
const STATE_RANK = { running: 0, queued: 1, success: 2, failed: 2 };

function sortRuns(runs) {
  return [...runs].sort((a, b) =>
    ((STATE_RANK[a.state] ?? 3) - (STATE_RANK[b.state] ?? 3))
    || ((Date.parse(b.run_after) || 0) - (Date.parse(a.run_after) || 0)));
}

// --- rows ------------------------------------------------------------------------
//
// Each cell declares a signature; updateRow rebuilds a cell only when its
// signature changed since the previous poll.

const CELLS = [
  { signature: (r) => r.state, build: buildStateCell },
  { signature: (r) => r.dag_id, build: buildPipelineCell },
  { signature: (r) => r.run_id, build: buildRunCell },
  { signature: (r) => JSON.stringify(r.stages), build: buildStagesCell },
  { signature: (r) => String(r.episode_count), build: buildEpisodesCell },
  { signature: (r) => String(r.profile), build: buildProfileCell },
  { signature: (r) => `${r.state}|${r.start_date}|${r.run_after}`, build: buildStartedCell },
  { signature: (r) => `${r.state}|${r.start_date}|${r.duration_s}`, build: buildDurationCell },
  { signature: (r) => String(r.airflow_url), build: buildAirflowCell },
];

function createRow(run) {
  const tr = h('tr', { class: run.state === 'running' ? 'row--running' : null });
  tr._prev = CELLS.map((cell) => {
    tr.append(cell.build(run));
    return cell.signature(run);
  });
  return tr;
}

function updateRow(tr, run) {
  tr.classList.toggle('row--running', run.state === 'running');
  CELLS.forEach((cell, index) => {
    const signature = cell.signature(run);
    if (tr._prev[index] !== signature) {
      tr.children[index].replaceWith(cell.build(run));
      tr._prev[index] = signature;
    }
  });
}

function buildStateCell(run) {
  return h('td', { class: 'col-state' }, statusDot(run.state));
}

function buildPipelineCell(run) {
  const pipelineName = run.dag_id.replace(/_ingest$/, '');
  return h('td', { class: 'col-pipeline cell-pipeline', title: run.dag_id }, pipelineName);
}

function buildRunCell(run) {
  const isManual = run.run_id.startsWith('manual__');
  return h('td', { class: 'col-run', title: run.run_id },
    h('span', { class: 'cell-run-wrap' },
      h('span', { class: 'run-id' },
        isManual ? h('span', { class: 'run-id__prefix' }, 'manual__') : null,
        isManual ? run.run_id.slice('manual__'.length) : run.run_id),
      copyButton(() => run.run_id, 'Copy run id')));
}

function buildStagesCell(run) {
  return h('td', { class: 'col-stages' }, stageStrip(run.stages));
}

function buildEpisodesCell(run) {
  return h('td', { class: 'col-episodes cell-num' },
    run.episode_count == null ? '—' : String(run.episode_count));
}

function buildProfileCell(run) {
  return h('td', { class: 'col-profile' }, run.profile ? chip(run.profile) : '—');
}

function buildStartedCell(run) {
  const ts = run.start_date || (run.state !== 'queued' ? run.run_after : null);
  return h('td', { class: 'col-started cell-dim' },
    ts ? h('span', { dataset: { relTs: ts }, title: ts }, relTime(ts)) : '—');
}

function buildDurationCell(run) {
  let content = '—';
  if (run.state === 'running' && run.start_date) {
    content = h('span', { dataset: { durSince: run.start_date } },
      duration((Date.now() - Date.parse(run.start_date)) / 1000));
  } else if (run.duration_s != null && (run.state === 'success' || run.state === 'failed')) {
    content = duration(run.duration_s);
  }
  return h('td', { class: 'col-duration cell-num' }, content);
}

function buildAirflowCell(run) {
  return h('td', { class: 'col-airflow' },
    run.airflow_url
      ? h('a', {
          class: 'icon-button', href: run.airflow_url, target: '_blank',
          rel: 'noopener', title: 'Open in Airflow',
        }, icon('arrow-up-right'))
      : null);
}

// --- credentials popover -------------------------------------------------------------
//
// The only reveal affordance in the app: the password string enters the DOM
// only after Reveal; Copy copies from the fetched value without revealing.

function openCredentials(anchor) {
  const content = h('div', null, h('div', { class: 'popover-pending' }, 'Loading'));
  popover(anchor, content);
  api('/api/airflow-credentials').then((creds) => {
    content.replaceChildren(
      credentialRow('URL', creds.url),
      credentialRow('Username', creds.username),
      passwordRow(creds.password),
      h('p', { class: 'cred-footer' }, 'Airflow signs you in with these -- it cannot be done automatically.'));
  }).catch((err) => {
    content.replaceChildren(h('p', { class: 'cred-error' }, err.message));
  });
}

function credentialRow(label, value) {
  return h('div', { class: 'cred-row' },
    h('span', { class: 'cred-label' }, label),
    h('span', { class: 'cred-value', title: value }, value),
    copyButton(() => value, `Copy ${label.toLowerCase()}`));
}

function passwordRow(password) {
  const valueEl = h('span', { class: 'cred-value' }, '••••••••');
  const revealButton = h('button', {
    class: 'button button--ghost button--small', type: 'button',
    onclick: () => {
      const revealing = revealButton.textContent === 'Reveal';
      valueEl.textContent = revealing ? password : '••••••••';
      revealButton.textContent = revealing ? 'Hide' : 'Reveal';
    },
  }, 'Reveal');
  return h('div', { class: 'cred-row' },
    h('span', { class: 'cred-label' }, 'Password'),
    valueEl,
    revealButton,
    copyButton(() => password, 'Copy password'));
}
