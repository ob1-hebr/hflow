// Secrets tab: keys and masked values only. There is no reveal endpoint --
// secret values never reach the browser, so the value column is a constant
// mask. No polling; the list mutates only on user action.

import { api } from '../api.js';
import {
  h, icon, callout, confirmInline, emptyState, loadingRow, toast,
} from '../ui.js';

// Matches the backend rule for environment variable names (mixed case allowed).
const KEY_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const VALUE_MASK = '••••••••';
const KEY_HINT = 'Keys are environment variable names: A-Z, 0-9, and underscore, not starting with a digit.';

let container = null;
let data;             // last successful /api/secrets payload
let loadError = null;
let addFormSlot = null;

export function mount(section, _params) {
  container = section;
  render();
  load();
}

export function unmount() {}

async function load() {
  try {
    data = await api('/api/secrets');
    loadError = null;
  } catch (err) {
    if (data === undefined) loadError = err;
  }
  render();
}

function secretNames() {
  return new Set(((data && data.secrets) || []).map((secret) => secret.name));
}

// --- rendering (plain full re-render on every change) ---------------------------

function render() {
  addFormSlot = h('div', { class: 'inline-form-slot' });
  const hasSecrets = Boolean(data && data.secrets.length > 0);
  const notes = h('div', { class: 'tab-notes' },
    h('p', { class: 'note' }, 'Changes apply on the next ', h('code', null, 'hflow up'), '.'));
  if (data && data.wired_into_bundle === false) {
    notes.append(h('p', { class: 'note' },
      'Secrets are not wired into the current runtime — re-run ', h('code', null, 'hflow up'), '.'));
  }
  container.replaceChildren(
    h('div', { class: 'tab-toolbar' },
      h('p', { class: 'subtitle' }, 'Secrets are injected into task containers as environment variables.'),
      hasSecrets
        ? h('button', { class: 'button button--primary', type: 'button', onclick: showAddForm }, '+ Add secret')
        : null),
    notes,
    addFormSlot,
    h('div', { class: 'tab-body' }, bodyContent()));
}

function bodyContent() {
  if (loadError) {
    return callout({ title: 'Could not load secrets', body: loadError.message, command: loadError.hint });
  }
  if (data === undefined) {
    return secretsTable(h('tbody', null, loadingRow(3)));
  }
  if (data.secrets.length === 0) {
    return emptyState({
      glyph: icon('key'),
      title: 'No secrets yet',
      body: 'Add a key and value to make it available to task containers.',
      trailing: h('button', {
        class: 'button button--primary', type: 'button', onclick: showAddForm,
      }, '+ Add secret'),
    });
  }
  return secretsTable(h('tbody', null, data.secrets.map(secretRow)));
}

function secretsTable(tbodyEl) {
  return h('table', { class: 'data-table secrets-table' },
    h('thead', null, h('tr', null,
      h('th', { class: 'col-key' }, 'Key'),
      h('th', { class: 'col-value' }, 'Value'),
      h('th', { class: 'col-actions' }))),
    tbodyEl);
}

function secretRow(secret) {
  const valueCell = h('td', { class: 'col-value cell-mono cell-dim' }, VALUE_MASK);
  const actionsCell = h('td', { class: 'col-actions' });
  const actions = h('span', { class: 'row-actions' },
    h('button', {
      class: 'button button--ghost button--small', type: 'button',
      onclick: () => startEdit(valueCell, secret.name),
    }, 'Edit'),
    h('button', {
      class: 'button button--danger-ghost button--small', type: 'button',
      onclick: () => startDelete(actionsCell, actions, secret.name),
    }, 'Delete'));
  actionsCell.append(actions);
  return h('tr', null,
    h('td', { class: 'col-key cell-key', title: secret.name }, secret.name),
    valueCell,
    actionsCell);
}

// --- edit in place ---------------------------------------------------------------

function startEdit(valueCell, name) {
  const input = h('input', {
    class: 'input input--mono', type: 'password', autocomplete: 'new-password',
    'aria-label': `New value for ${name}`,
  });
  const save = async () => {
    try {
      await api(`/api/secrets/${encodeURIComponent(name)}`, { method: 'PUT', body: { value: input.value } });
    } catch (err) {
      toast(`Request failed: ${err.message}`);
      return;
    }
    toast('Secret updated');
    load();
  };
  valueCell.replaceChildren(h('form', {
    class: 'cell-form',
    onsubmit: (event) => { event.preventDefault(); save(); },
    onkeydown: (event) => { if (event.key === 'Escape') render(); },
  },
    input,
    h('button', { class: 'button button--primary button--small', type: 'submit' }, 'Save'),
    h('button', { class: 'button button--ghost button--small', type: 'button', onclick: () => render() }, 'Cancel')));
  input.focus();
}

// --- delete with two-step confirm ---------------------------------------------------

function startDelete(actionsCell, actions, name) {
  confirmInline(actionsCell, `Delete ${name}?`, () => actionsCell.replaceChildren(actions), async () => {
    try {
      await api(`/api/secrets/${encodeURIComponent(name)}`, { method: 'DELETE' });
    } catch (err) {
      toast(`Request failed: ${err.message}`);
      actionsCell.replaceChildren(actions);
      return;
    }
    toast('Secret deleted');
    load();
  });
}

// --- add/update inline form -----------------------------------------------------------

function showAddForm() {
  const existingInput = addFormSlot.querySelector('input');
  if (existingInput) { existingInput.focus(); return; }
  const hint = h('p', { class: 'field-hint' }, KEY_HINT);
  const errorLine = h('div', { class: 'field-error', hidden: true });
  const submitButton = h('button', { class: 'button button--primary', type: 'submit' }, 'Add secret');
  const keyInput = h('input', {
    class: 'input input--mono', type: 'text', spellcheck: 'false', autocomplete: 'off',
    placeholder: 'MY_API_KEY', 'aria-label': 'Key',
    oninput: () => {
      // Relabel live when the typed key would overwrite an existing secret.
      submitButton.textContent = secretNames().has(keyInput.value.trim()) ? 'Update secret' : 'Add secret';
      keyInput.classList.remove('invalid');
      hint.classList.remove('field-hint--error');
      errorLine.hidden = true;
    },
  });
  const valueInput = h('input', {
    class: 'input input--mono', type: 'password', autocomplete: 'new-password',
    placeholder: 'Value', 'aria-label': 'Value',
  });
  const showToggle = h('button', {
    class: 'button button--ghost button--small', type: 'button',
    onclick: () => {
      const showing = valueInput.type === 'text';
      valueInput.type = showing ? 'password' : 'text';
      showToggle.textContent = showing ? 'Show' : 'Hide';
    },
  }, 'Show');
  const form = h('form', {
    class: 'inline-form',
    onsubmit: async (event) => {
      event.preventDefault();
      const key = keyInput.value.trim();
      if (!KEY_PATTERN.test(key)) {
        keyInput.classList.add('invalid');
        hint.classList.add('field-hint--error');
        keyInput.focus();
        return;
      }
      const isUpdate = secretNames().has(key);
      try {
        await api(`/api/secrets/${encodeURIComponent(key)}`, { method: 'PUT', body: { value: valueInput.value } });
      } catch (err) {
        keyInput.classList.add('invalid');
        errorLine.hidden = false;
        errorLine.replaceChildren(err.message);
        return;
      }
      toast(isUpdate ? 'Secret updated' : 'Secret added');
      load();
    },
    onkeydown: (event) => { if (event.key === 'Escape') addFormSlot.replaceChildren(); },
  },
    h('div', { class: 'inline-form-main' },
      h('div', { class: 'inline-form-fields' },
        keyInput,
        h('span', { class: 'input-group' }, valueInput, showToggle)),
      hint,
      errorLine),
    submitButton,
    h('button', {
      class: 'button button--ghost', type: 'button',
      onclick: () => addFormSlot.replaceChildren(),
    }, 'Cancel'));
  addFormSlot.replaceChildren(form);
  keyInput.focus();
}
