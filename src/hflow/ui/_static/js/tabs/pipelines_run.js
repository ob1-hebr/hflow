// Run page: one run's DAG with live task state, so following a run never
// means going to find it in Airflow's own UI. Polls every 3s while the run is
// active and stops once it finishes -- a run that ended cannot change again.
//
// The same page renders a stage's own run: the master's trigger nodes drill
// into it, and the backend resolves which sub-run that was, so the only run
// id this module ever holds is the master's.

import { api } from '../api.js';
import { Poller } from '../app.js';
import { createDagGraph } from '../graph.js';
import {
  h, icon, statusDot, stageStrip, chip, copyButton, callout, duration, relTime,
} from '../ui.js';

const POLL_MS = 3000;
const TERMINAL_RUN_STATES = new Set(['success', 'failed']);

let container = null;
let poller = null;
let runId = null;
let stage = null;         // the stage being drilled into, null on the master page
let data;                 // last successful graph payload
let mode = null;          // 'graph' | 'down' | 'missing' | 'no-run'
let graph = null;
let headerEl = null;
let headerSignature = null;
let panelSlot = null;
let panelSignature = null;
let selectedId = null;

export function runHash(id, stageName = null) {
  // Run ids carry '+' and ':', so the query is always built with
  // encodeURIComponent -- never by hand.
  const suffix = stageName ? `&stage=${encodeURIComponent(stageName)}` : '';
  return `#/pipelines/run?id=${encodeURIComponent(id)}${suffix}`;
}

export function mount(section, params) {
  container = section;
  runId = params.id || null;
  stage = params.stage || null;
  data = undefined;
  mode = null;
  graph = null;
  selectedId = null;
  panelSignature = null;
  document.addEventListener('keydown', onKeydown);
  if (!runId) {
    renderMessage('no-run', {
      title: 'No run selected',
      body: 'Open a run from the runs list to see its pipeline.',
    });
    return;
  }
  if (!poller) poller = new Poller(load, POLL_MS);
  poller.start();
}

export function unmount() {
  if (poller) poller.stop();
  document.removeEventListener('keydown', onKeydown);
  container = null;
}

function onKeydown(event) {
  if (event.key === 'Escape' && selectedId) selectNode(null);
}

function graphPath(id) {
  const base = `/api/pipelines/runs/${encodeURIComponent(id)}`;
  return stage ? `${base}/stages/${encodeURIComponent(stage)}/graph` : `${base}/graph`;
}

async function load() {
  const [requested, requestedStage] = [runId, stage];
  const stale = () => requested !== runId || requestedStage !== stage || container === null;
  let payload;
  try {
    payload = await api(graphPath(requested));
  } catch (err) {
    if (stale()) return; // navigated away mid-request
    if (err.status === 404) {
      poller.stop(); // a run id that is absent now will not appear later
      renderMessage('missing', {
        title: 'Run not found',
        body: 'This run is no longer in Airflow. It may have been cleaned up.',
      });
    } else if (err.status === 503) {
      // Keep polling: the runtime coming back should heal the page.
      renderMessage('down', {
        title: 'Runtime not running',
        body: 'The local Airflow runtime is not reachable. Start it to follow this run.',
        command: 'hflow up',
      });
    }
    return; // any other failure keeps the last render; the banner reports it
  }
  if (stale()) return;
  data = payload;
  render();
  if (finished()) poller.stop();
}

function finished() {
  if (!stage) return TERMINAL_RUN_STATES.has(data.run.state);
  // A stage that never started still settles once its master run does.
  return TERMINAL_RUN_STATES.has(data.master_state)
    && (data.run === null || TERMINAL_RUN_STATES.has(data.run.state));
}

// --- rendering ---------------------------------------------------------------

function crumbs() {
  return h('div', { class: 'browse-crumbs' },
    h('a', { class: 'crumb-back', href: '#/pipelines' }, '‹ Runs'),
    stage
      ? h('a', { class: 'crumb', href: runHash(runId), title: runId }, runId)
      : h('span', { class: 'crumb crumb--current', title: runId }, runId),
    stage ? h('span', { class: 'crumb-sep' }, '/') : null,
    stage ? h('span', { class: 'crumb crumb--current' }, stage) : null);
}

function renderMessage(nextMode, { title, body, command }) {
  if (mode === nextMode) return;
  mode = nextMode;
  graph = null;
  container.replaceChildren(crumbs(), callout({ title, body, command }));
}

function render() {
  if (mode !== 'graph') {
    mode = 'graph';
    graph = createDagGraph({
      onSelect: selectNode,
      // Only the master's trigger nodes carry a stage, so only they drill in.
      onDrillIn: stage ? null : (stageName) => { location.hash = runHash(runId, stageName); },
    });
    headerEl = header();
    headerSignature = runSignature(data);
    panelSlot = h('div', { class: 'panel-slot' });
    panelSignature = null;
    container.replaceChildren(
      crumbs(),
      headerEl,
      h('div', { class: 'run-layout' }, graph.el, panelSlot));
  } else if (runSignature(data) !== headerSignature) {
    headerSignature = runSignature(data);
    const next = header();
    headerEl.replaceWith(next);
    headerEl = next;
  }
  graph.render(data);
  graph.select(selectedId);
  renderPanel();
}

function runSignature({ run, master_state: masterState }) {
  if (run === null) return `none|${masterState}`;
  return [run.state, run.start_date, run.duration_s, JSON.stringify(run.stages)].join('|');
}

function header() {
  if (data.run !== null) return runHeader(data.run);
  return h('div', { class: 'run-header' },
    statusDot('pending'),
    h('span', { class: 'run-header__fact' },
      `The ${stage} stage has not started -- these are the tasks it will run.`));
}

function runHeader(run) {
  const started = run.start_date || run.run_after;
  return h('div', { class: 'run-header' },
    statusDot(run.state),
    h('span', { class: 'run-header__id', title: run.run_id }, run.run_id),
    copyButton(() => run.run_id, 'Copy run id'),
    run.profile ? chip(run.profile) : null,
    h('span', { class: 'run-header__fact' },
      run.episode_count == null ? '—' : `${run.episode_count} episodes`),
    started
      ? h('span', { class: 'run-header__fact', dataset: { relTs: started }, title: started },
          relTime(started))
      : null,
    h('span', { class: 'run-header__fact' }, runDuration(run)),
    run.stages ? stageStrip(run.stages) : null,
    h('a', {
      class: 'icon-button', href: run.airflow_url, target: '_blank',
      rel: 'noopener', title: 'Open in Airflow',
    }, icon('arrow-up-right')));
}

function runDuration(run) {
  if (run.state === 'running' && run.start_date) {
    return h('span', { dataset: { durSince: run.start_date } },
      duration((Date.now() - Date.parse(run.start_date)) / 1000));
  }
  return duration(run.duration_s);
}

// --- task details panel -------------------------------------------------------
//
// Everything the graph cannot show without shouting: exact timings, attempts,
// the task's own doc line, and a mapped task's per-batch breakdown.

function selectNode(id) {
  selectedId = id;
  if (graph) graph.select(id);
  renderPanel();
}

function renderPanel() {
  if (mode !== 'graph') return;
  const node = selectedId ? data.nodes.find((entry) => entry.id === selectedId) : null;
  if (!node) {
    // The selected task vanished (a re-render changed the shape): close.
    selectedId = null;
    if (panelSignature !== null) {
      panelSignature = null;
      panelSlot.replaceChildren();
    }
    return;
  }
  const signature = `${node.id}|${node.state}|${node.duration_s}|${JSON.stringify(node.mapped)}`;
  if (signature === panelSignature) return;
  panelSignature = signature;
  panelSlot.replaceChildren(taskPanel(node));
}

function taskPanel(node) {
  const showsRawState = node.airflow_state && node.airflow_state !== node.state;
  return h('aside', { class: 'detail-panel' },
    h('div', { class: 'detail-panel__head' },
      h('span', { class: 'detail-panel__title', title: node.id }, node.label),
      h('button', {
        class: 'icon-button', type: 'button', title: 'Close',
        onclick: () => selectNode(null),
      }, icon('x'))),
    h('div', { class: 'detail-panel__state' },
      statusDot(node.state),
      showsRawState ? h('span', { class: 'note' }, node.airflow_state) : null),
    node.operator ? chip(node.operator) : null,
    node.doc ? h('p', { class: 'detail-panel__doc' }, node.doc) : null,
    panelRow('Started', node.start_date ? relTime(node.start_date) : '—', node.start_date),
    panelRow('Ended', node.end_date ? relTime(node.end_date) : '—', node.end_date),
    panelRow('Duration', taskDuration(node)),
    panelRow('Attempt', node.try_number == null ? '—' : String(node.try_number)),
    mappedBreakdown(node),
    node.stage
      ? h('a', { class: 'detail-panel__link', href: runHash(runId, node.stage) },
          `View the ${node.stage} stage`, icon('chevron'))
      : null,
    node.airflow_url
      ? h('a', {
          class: 'detail-panel__link', href: node.airflow_url,
          target: '_blank', rel: 'noopener',
        }, 'Logs in Airflow', icon('arrow-up-right'))
      : null);
}

function taskDuration(node) {
  if (node.state === 'running' && node.start_date) {
    return h('span', { dataset: { durSince: node.start_date } },
      duration((Date.now() - Date.parse(node.start_date)) / 1000));
  }
  return duration(node.duration_s);
}

function panelRow(label, value, title = null) {
  return h('div', { class: 'panel-row' },
    h('span', { class: 'panel-row__label' }, label),
    h('span', { class: 'panel-row__value', title }, value));
}

function mappedBreakdown(node) {
  if (!node.mapped || !node.mapped.length) return null;
  return h('div', { class: 'panel-batches' },
    h('div', { class: 'panel-row__label' }, `Batches (${node.mapped.length})`),
    h('table', { class: 'data-table batches-table' },
      h('tbody', null, node.mapped.map((entry) => h('tr', null,
        h('td', { class: 'cell-num batches-table__index' },
          entry.rendered_map_index ?? String(entry.map_index)),
        h('td', null, statusDot(entry.state)),
        h('td', { class: 'cell-num cell-dim' }, duration(entry.duration_s)),
        h('td', { class: 'batches-table__link' },
          entry.airflow_url
            ? h('a', {
                class: 'icon-button', href: entry.airflow_url, target: '_blank',
                rel: 'noopener', title: `Logs for batch ${entry.map_index}`,
              }, icon('arrow-up-right'))
            : null))))));
}
