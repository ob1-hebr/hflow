// App shell: boot, hash router, tab lifecycle, Poller, visibilitychange.

import { api } from './api.js';
import { closePopover } from './ui.js';
import * as pipelinesTab from './tabs/pipelines.js';
import * as storageTab from './tabs/storage.js';
import * as secretsTab from './tabs/secrets.js';

const TABS = { pipelines: pipelinesTab, storage: storageTab, secrets: secretsTab };

// --- Poller ---------------------------------------------------------------
//
// All pollers register themselves so visibilitychange can pause every
// running poller while the tab is hidden and resume, with an immediate
// fire, when it becomes visible again.

const allPollers = new Set();

export class Poller {
  constructor(fn, intervalMs) {
    this.fn = fn;
    this.intervalMs = intervalMs;
    this.timer = null;
    this.pausedByVisibility = false;
    allPollers.add(this);
  }

  start() {
    this.stop();
    this.fn();
    this.timer = setInterval(this.fn, this.intervalMs);
  }

  stop() {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.pausedByVisibility = false;
  }

  kick() {
    if (this.timer !== null) this.start();
    else this.fn();
  }
}

document.addEventListener('visibilitychange', () => {
  for (const poller of allPollers) {
    if (document.hidden) {
      if (poller.timer !== null) {
        poller.stop();
        poller.pausedByVisibility = true;
      }
    } else if (poller.pausedByVisibility) {
      poller.start();
    }
  }
});

// --- hash router ---------------------------------------------------------------
//
// Routes: #/pipelines (default), #/storage,
// #/storage/browse?root=<id>&prefix=<encoded>, #/secrets.

let activeTabName = null;

function parseRoute() {
  const [path, query = ''] = location.hash.replace(/^#\/?/, '').split('?');
  const segments = path.split('/').filter(Boolean);
  return {
    tab: segments[0],
    params: { view: segments[1] || null, ...Object.fromEntries(new URLSearchParams(query)) },
  };
}

function route() {
  const { tab, params } = parseRoute();
  if (!TABS[tab]) {
    location.replace('#/pipelines');
    return;
  }
  closePopover();
  if (activeTabName) {
    TABS[activeTabName].unmount();
    document.getElementById(`tab-${activeTabName}`).hidden = true;
  }
  activeTabName = tab;
  for (const link of document.querySelectorAll('.topbar__tabs a')) {
    link.classList.toggle('active', link.dataset.tab === tab);
  }
  const section = document.getElementById(`tab-${tab}`);
  section.hidden = false;
  TABS[tab].mount(section, params);
}

window.addEventListener('hashchange', route);

// --- boot -----------------------------------------------------------------------

async function boot() {
  route();
  try {
    const status = await api('/api/status');
    document.getElementById('app-version').textContent = `v${status.hflow_version}`;
  } catch {
    // Unreachable at boot: the failure tracking in api.js raises the banner.
  }
}

boot();
