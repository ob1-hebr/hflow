// One run, at three altitudes.
//
//   ?id=<run>                 the pipeline as its stages: what each one is
//                             for, how many episodes are through, what is
//                             holding the rest up. The default, because that
//                             is the question an operator actually has.
//   ?id=<run>&stage=<stage>    one stage: its progress, what its checks found,
//                             and the batch tasks that did the work.
//   ?id=<run>&graph=tasks      the master DAG's own tasks, for when the
//                             orchestration itself is the suspect.
//
// Polls every 3s while the run is active and stops once it finishes -- a run
// that ended cannot change again. The stage views never hold a sub-run id:
// the backend resolves which run a stage was, so the only id here is the
// master's.

import { api } from '../api.js';
import { Poller } from '../app.js';
import { createDagGraph } from '../graph.js';
import { checksTable, createStageCards } from '../stage_cards.js';
import {
  h, icon, statusDot, chip, copyButton, callout, duration, relTime,
} from '../ui.js';

const POLL_MS = 3000;
const TERMINAL_RUN_STATES = new Set(['success', 'failed']);

let container = null;
let poller = null;
let runId = null;
let stage = null;         // the stage being drilled into, null otherwise
let view = null;          // 'cards' | 'stage' | 'tasks'
let data;                 // last successful payload for this view
let mode = null;          // 'pending' | 'cards' | 'stage' | 'graph' | 'down' | 'missing' | 'no-run'
let cards = null;
let graph = null;
let headerEl = null;
let headerSignature = null;
let checksSlot = null;
let checksSignature = null;
let panelSlot = null;
let panelSignature = null;
let selectedId = null;

export function runHash(id, { stage: stageName = null, graph: graphName = null } = {}) {
  // Run ids carry '+' and ':', so the query is always built with
  // encodeURIComponent -- never by hand.
  const parts = [`id=${encodeURIComponent(id)}`];
  if (stageName) parts.push(`stage=${encodeURIComponent(stageName)}`);
  if (graphName) parts.push(`graph=${encodeURIComponent(graphName)}`);
  return `#/pipelines/run?${parts.join('&')}`;
}

export function mount(section, params) {
  container = section;
  runId = params.id || null;
  stage = params.stage || null;
  view = stage ? 'stage' : params.graph === 'tasks' ? 'tasks' : 'cards';
  data = undefined;
  mode = null;
  cards = null;
  graph = null;
  selectedId = null;
  panelSignature = null;
  checksSignature = null;
  document.addEventListener('keydown', onKeydown);
  if (!runId) {
    renderMessage('no-run', {
      title: 'No run selected',
      body: 'Open a run from the runs list to see its pipeline.',
    });
    return;
  }
  // Paint this page's own frame right away: the runs table must not linger
  // on screen while the first fetch loads.
  mode = 'pending';
  container.replaceChildren(crumbs(), h('div', { class: 'block-pending' }, 'Loading'));
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

// --- loading ------------------------------------------------------------------

function runPath(id) {
  return `/api/pipelines/runs/${encodeURIComponent(id)}`;
}

async function load() {
  const [requested, requestedStage, requestedView] = [runId, stage, view];
  const stale = () =>
    requested !== runId || requestedStage !== stage || requestedView !== view || container === null;
  let payload;
  try {
    payload = await fetchView(requested, requestedStage, requestedView);
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

function fetchView(id, stageName, viewName) {
  const base = runPath(id);
  if (viewName === 'tasks') return api(`${base}/graph`);
  if (viewName === 'cards') return api(base);
  const stagePath = `${base}/stages/${encodeURIComponent(stageName)}`;
  return Promise.all([api(base), api(`${stagePath}/checks`), api(`${stagePath}/graph`)]).then(
    ([run, checks, graphData]) => ({ ...graphData, run_detail: run, checks: checks.checks }),
  );
}

function finished() {
  if (view === 'cards') return TERMINAL_RUN_STATES.has(data.run.state);
  if (view === 'tasks') return TERMINAL_RUN_STATES.has(data.run.state);
  // A stage that never started still settles once its master run does.
  return TERMINAL_RUN_STATES.has(data.master_state)
    && (data.run === null || TERMINAL_RUN_STATES.has(data.run.state));
}

// --- rendering ----------------------------------------------------------------

function crumbs() {
  const onRunPage = view === 'cards';
  return h('div', { class: 'browse-crumbs' },
    h('a', { class: 'crumb-back', href: '#/pipelines' }, '‹ Runs'),
    onRunPage
      ? h('span', { class: 'crumb crumb--current', title: runId }, runId)
      : h('a', { class: 'crumb', href: runHash(runId), title: runId }, runId),
    onRunPage ? null : h('span', { class: 'crumb-sep' }, '/'),
    onRunPage ? null : h('span', { class: 'crumb crumb--current' }, stage || 'task graph'));
}

function renderMessage(nextMode, { title, body, command }) {
  if (mode === nextMode) return;
  mode = nextMode;
  cards = null;
  graph = null;
  container.replaceChildren(crumbs(), callout({ title, body, command }));
}

function render() {
  if (view === 'cards') renderCards();
  else if (view === 'stage') renderStage();
  else renderTaskGraph();
}

// --- the stage list: the run's pipeline ---------------------------------------

function renderCards() {
  if (mode !== 'cards') {
    mode = 'cards';
    cards = createStageCards({
      onOpen: (stageName) => { location.hash = runHash(runId, { stage: stageName }); },
    });
    headerEl = header(data.run);
    headerSignature = runSignature(data.run);
    container.replaceChildren(crumbs(), headerEl, cards.el);
  } else {
    refreshHeader(data.run);
  }
  cards.render(data.stages);
}

function header(run) {
  return h('div', { class: 'run-header' },
    statusDot(run.state),
    h('span', { class: 'run-header__id', title: run.run_id }, run.run_id),
    copyButton(() => run.run_id, 'Copy run id'),
    run.profile ? chip(run.profile) : null,
    h('span', { class: 'run-header__fact' },
      run.episode_count == null ? '—' : `${run.episode_count} episodes`),
    run.start_date || run.run_after
      ? h('span', {
          class: 'run-header__fact',
          dataset: { relTs: run.start_date || run.run_after },
          title: run.start_date || run.run_after,
        }, relTime(run.start_date || run.run_after))
      : null,
    h('span', { class: 'run-header__fact' }, runDuration(run)),
    h('span', { class: 'run-header__spacer' }),
    view === 'tasks'
      ? h('a', { class: 'text-link', href: runHash(runId) }, 'Stages')
      : h('a', { class: 'text-link', href: runHash(runId, { graph: 'tasks' }) }, 'Task graph'),
    h('a', {
      class: 'icon-button', href: run.airflow_url, target: '_blank',
      rel: 'noopener', title: 'Open in Airflow',
    }, icon('arrow-up-right')));
}

function refreshHeader(run) {
  if (runSignature(run) === headerSignature) return;
  headerSignature = runSignature(run);
  const next = header(run);
  headerEl.replaceWith(next);
  headerEl = next;
}

function runSignature(run) {
  if (!run) return 'none';
  return [run.state, run.start_date, run.duration_s].join('|');
}

function runDuration(run) {
  if (run.state === 'running' && run.start_date) {
    return h('span', { dataset: { durSince: run.start_date } },
      duration((Date.now() - Date.parse(run.start_date)) / 1000));
  }
  return duration(run.duration_s);
}

// --- one stage: its card, what its checks found, and the tasks that ran -------

function renderStage() {
  if (mode !== 'stage') {
    mode = 'stage';
    cards = createStageCards({});  // the stage's own card: nothing to open
    graph = createDagGraph({ onSelect: selectNode, onDrillIn: null });
    checksSlot = h('div', { class: 'stage-checks' });
    checksSignature = null;
    panelSlot = h('div', { class: 'panel-slot' });
    panelSignature = null;
    container.replaceChildren(
      crumbs(),
      cards.el,
      checksSlot,
      h('div', { class: 'run-layout' }, graph.el, panelSlot));
  }
  const position = data.run_detail.stages.findIndex((entry) => entry.stage === stage);
  cards.render(position < 0 ? [] : [data.run_detail.stages[position]], position + 1);
  renderChecks();
  graph.render(data);
  graph.select(selectedId);
  renderPanel();
}

function renderChecks() {
  const signature = JSON.stringify(data.checks);
  if (signature === checksSignature) return;
  checksSignature = signature;
  if (!data.checks.length) {
    checksSlot.replaceChildren(h('p', { class: 'stage-checks__note' },
      stage === 'sync'
        ? 'This stage records episodes, not checks — its evidence is the canonical file itself.'
        : 'No check evidence recorded for this run yet.'));
    return;
  }
  checksSlot.replaceChildren(
    h('h4', { class: 'stage-checks__title' }, 'What its checks found'),
    checksTable(data.checks));
}

// --- the master task graph, for orchestration questions -----------------------

function renderTaskGraph() {
  if (mode !== 'graph') {
    mode = 'graph';
    graph = createDagGraph({
      onSelect: selectNode,
      onDrillIn: (stageName) => { location.hash = runHash(runId, { stage: stageName }); },
    });
    headerEl = header(data.run);
    headerSignature = runSignature(data.run);
    panelSlot = h('div', { class: 'panel-slot' });
    panelSignature = null;
    container.replaceChildren(
      crumbs(),
      headerEl,
      h('div', { class: 'run-layout' }, graph.el, panelSlot));
  } else {
    refreshHeader(data.run);
  }
  graph.render(data);
  graph.select(selectedId);
  renderPanel();
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
  if (mode !== 'graph' && mode !== 'stage') return;
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
      ? h('a', {
          class: 'detail-panel__link', href: runHash(runId, { stage: node.stage }),
        }, `View the ${node.stage} stage`, icon('chevron'))
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
