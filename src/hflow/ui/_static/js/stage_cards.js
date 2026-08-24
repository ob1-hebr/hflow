// The run's pipeline as its stages: what each one is for, how far its
// episodes have got, and what is holding the rest up.
//
// Cards are keyed and updated in place (the syncRows idiom for a list of
// sections), so a 3s poll never restarts the running card's pulse or drops
// the reader's scroll position. A card reads only the payload's own fields --
// nothing here knows about Airflow -- so a stage backed by an external
// verification service renders the same way.

import { h, icon, statusDot, chip, duration } from './ui.js';

const LAYER_LABELS = {
  automated: 'automated checks',
  model: 'model verification',
  human: 'human review',
};

// `firstNumber` is the pipeline position of the first card given, so a single
// stage rendered on its own page still shows the number it has in the run.
export function createStageCards({ onOpen = null }) {
  const el = h('div', { class: 'stage-cards' });

  function render(cards, firstNumber = 1) {
    const existing = new Map();
    for (const node of el.children) existing.set(node.dataset.key, node);
    let cursor = el.firstChild;
    cards.forEach((card, index) => {
      let node = existing.get(card.stage);
      const signature = cardSignature(card);
      if (node) {
        existing.delete(card.stage);
        if (node.dataset.signature !== signature) {
          const next = stageCard(card, firstNumber + index, onOpen);
          node.replaceWith(next);
          node = next;
        } else {
          // Pace and estimate move on every poll of a running stage. Writing
          // them in place is what keeps the card itself alive: rebuilding it
          // would restart the pulse and drop the reader's focus every 3s.
          refreshMetrics(node, card);
        }
      } else {
        node = stageCard(card, firstNumber + index, onOpen);
      }
      node.dataset.key = card.stage;
      node.dataset.signature = signature;
      if (node === cursor) cursor = cursor.nextSibling;
      else el.insertBefore(node, cursor);
    });
    for (const leftover of existing.values()) leftover.remove();
  }

  return { el, render };
}

// Everything that changes the card's shape -- deliberately not the pace and
// estimate, which move every poll and are written in place instead.
function cardSignature(card) {
  const p = card.progress;
  return JSON.stringify([
    card.state, card.waiting_on, card.reached, card.total, card.duration_s, card.sub_run_url,
    p && [p.done, p.quarantined, p.errors, p.stalled],
  ]);
}

function refreshMetrics(node, card) {
  const row = node.querySelector('.stage-card__pace');
  if (!row || !card.progress) return;
  const next = metricsRow(card, card.progress);
  if (next) row.replaceChildren(...next.childNodes);
}

// --- one card -----------------------------------------------------------------

function stageCard(card, number, onOpen) {
  const progress = card.progress;
  const stalled = Boolean(progress && progress.stalled);
  const classes = ['stage-card', `stage-card--${card.state}`];
  if (stalled) classes.push('stage-card--stalled');
  const openable = Boolean(onOpen) && card.state !== 'pending';
  return h('section', {
      class: classes.join(' '),
      role: openable ? 'button' : null,
      tabindex: openable ? '0' : null,
      onclick: openable ? (event) => {
        if (event.target.closest('a, button')) return;
        onOpen(card.stage);
      } : null,
      onkeydown: openable ? (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          onOpen(card.stage);
        }
      } : null,
    },
    h('div', { class: 'stage-card__num' }, String(number).padStart(2, '0')),
    h('div', { class: 'stage-card__body' },
      h('div', { class: 'stage-card__head' },
        h('h3', { class: 'stage-card__title' }, card.title),
        chip(LAYER_LABELS[card.layer] || card.layer),
        h('span', { class: 'stage-card__spacer' }),
        statusDot(card.state),
        card.sub_run_url
          ? h('a', {
              class: 'icon-button', href: card.sub_run_url, target: '_blank',
              rel: 'noopener', title: 'Open this stage in Airflow',
            }, icon('arrow-up-right'))
          : null,
        openable ? h('span', { class: 'stage-card__chevron' }, icon('chevron')) : null),
      h('p', { class: 'stage-card__desc' }, card.description),
      card.state === 'pending'
        ? h('p', { class: 'stage-card__metrics' },
            card.reached === false ? 'Never ran — the run ended first'
              : card.waiting_on ? `Waiting on ${card.waiting_on}`
              : 'Next up')
        : null,
      card.state === 'skipped'
        ? h('p', { class: 'stage-card__metrics' }, 'Not part of this run’s profile')
        : null,
      progress ? progressBar(card, progress) : null,
      progress ? metricsRow(card, progress) : null,
      progress === null && card.state !== 'pending' && card.state !== 'skipped'
        ? h('p', {
            class: 'stage-card__metrics',
            title: 'Counts come from the pipeline’s catalog under the served data root',
          }, 'Episode counts unavailable')
        : null,
      stalled ? stalledNote(progress) : null));
}

function progressBar(card, progress) {
  const total = card.total;
  const fraction = total ? Math.min(1, progress.done / total) : 0;
  return h('div', { class: 'progress' },
    h('div', { class: 'progress-bar' },
      h('div', {
        class: 'progress-bar__fill',
        style: `width: ${(fraction * 100).toFixed(1)}%`,
      })),
    h('span', { class: 'progress__count' },
      total == null ? `${progress.done} episodes` : `${progress.done} / ${total} episodes`));
}

function metricsRow(card, progress) {
  const facts = [];
  if (card.state === 'running' && card.started_at) {
    facts.push(h('span', { dataset: { durSince: card.started_at } },
      duration((Date.now() - Date.parse(card.started_at)) / 1000)));
  } else if (card.duration_s != null) {
    facts.push(duration(card.duration_s));
  }
  if (progress.throughput_eps_per_min != null) {
    facts.push(`${formatRate(progress.throughput_eps_per_min)} eps/min`);
  }
  if (progress.eta_s != null) facts.push(`~${duration(progress.eta_s)} left`);
  if (progress.quarantined) {
    facts.push(h('span', { class: 'metric--warn' }, `${progress.quarantined} quarantined`));
  }
  if (progress.errors) {
    facts.push(h('span', { class: 'metric--error' }, `${progress.errors} errored`));
  }
  if (!facts.length) return null;
  return h('p', { class: 'stage-card__metrics stage-card__pace' }, joinFacts(facts));
}

function stalledNote(progress) {
  const quiet = progress.last_completed_at
    ? duration((Date.now() - Date.parse(progress.last_completed_at)) / 1000)
    : null;
  return h('p', { class: 'stage-card__stalled' },
    quiet
      ? `Stalled: nothing has finished for ${quiet}, well past this stage’s own pace.`
      : 'Stalled: nothing has finished for well past this stage’s own pace.');
}

function formatRate(rate) {
  return rate >= 10 ? String(Math.round(rate)) : rate.toFixed(1);
}

function joinFacts(facts) {
  return facts.flatMap((fact, index) =>
    index === 0 ? [fact] : [h('span', { class: 'metric-sep' }, '·'), fact]);
}

// --- the check breakdown, shown on a stage's own page --------------------------

const STATUS_ORDER = ['passed', 'failed', 'measured', 'skipped', 'error'];

export function checksTable(checks) {
  return h('table', { class: 'data-table checks-table' },
    h('thead', null,
      h('tr', null,
        h('th', null, 'Check'),
        h('th', null, 'Outcomes'),
        h('th', { class: 'cell-num' }, 'Avg'))),
    h('tbody', null, checks.map((check) => h('tr', null,
      h('td', null,
        h('span', { class: 'checks-table__name' }, check.name),
        check.critical ? chip('critical') : null),
      h('td', null, STATUS_ORDER
        .filter((status) => check.statuses[status])
        .map((status) => h('span', { class: `status-count status-count--${status}` },
          `${check.statuses[status]} ${status}`))),
      h('td', { class: 'cell-num cell-dim' }, checkDuration(check.avg_duration_s))))));
}

// A check's cost is often well under a second, where duration()'s whole
// seconds would read as "0s" for every fast check.
function checkDuration(seconds) {
  if (seconds == null) return '—';
  if (seconds < 10) return `${seconds.toFixed(2)}s`;
  return duration(seconds);
}
