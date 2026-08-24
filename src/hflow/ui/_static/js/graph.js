// DAG graph: one run drawn as a layered, top-to-bottom SVG.
//
// Layout runs only when the DAG's shape changes; a poll that brings new task
// states restyles the existing nodes in place, so nothing moves under the
// cursor and a running node's pulse keeps its phase. SVG elements need
// createElementNS, so h() from ui.js (HTML only) cannot build them.

const SVG_NS = 'http://www.w3.org/2000/svg';

const NODE_W = 224;
const NODE_H = 46;
const H_GAP = 28;
const V_GAP = 34;
const PAD = 20;
// The label is 12.5px mono (~7.5px per character) in NODE_W minus the dot
// gutter, so it stays inside its box without measuring text.
const LABEL_MAX = 23;

function svgEl(tag, attrs, ...children) {
  const el = document.createElementNS(SVG_NS, tag);
  if (attrs) {
    for (const [key, value] of Object.entries(attrs)) {
      if (value == null) continue;
      if (key.startsWith('on')) el.addEventListener(key.slice(2).toLowerCase(), value);
      else el.setAttribute(key, String(value));
    }
  }
  for (const child of children) {
    if (child == null || child === false) continue;
    el.append(child.nodeType ? child : document.createTextNode(String(child)));
  }
  return el;
}

// --- layout ------------------------------------------------------------------
//
// Longest-path layering: a task sits one layer below its latest upstream, so
// every edge points downward. Within a layer, nodes follow the average
// position of their predecessors, which keeps parallel branches untangled.

function layerNodes(nodes, edges) {
  const indegree = new Map(nodes.map((node) => [node.id, 0]));
  const downstream = new Map(nodes.map((node) => [node.id, []]));
  for (const edge of edges) {
    if (!indegree.has(edge.target) || !downstream.has(edge.source)) continue;
    indegree.set(edge.target, indegree.get(edge.target) + 1);
    downstream.get(edge.source).push(edge.target);
  }
  const layer = new Map(nodes.map((node) => [node.id, 0]));
  const queue = nodes.filter((node) => indegree.get(node.id) === 0).map((node) => node.id);
  while (queue.length) {
    const id = queue.shift();
    for (const next of downstream.get(id)) {
      layer.set(next, Math.max(layer.get(next), layer.get(id) + 1));
      indegree.set(next, indegree.get(next) - 1);
      if (indegree.get(next) === 0) queue.push(next);
    }
  }
  return layer; // a cycle would leave nodes at layer 0 rather than drop them
}

function layoutGraph(nodes, edges) {
  const layer = layerNodes(nodes, edges);
  const rows = [];
  for (const node of nodes) {
    const index = layer.get(node.id);
    (rows[index] ||= []).push(node.id);
  }
  const predecessors = new Map(nodes.map((node) => [node.id, []]));
  for (const edge of edges) {
    if (predecessors.has(edge.target)) predecessors.get(edge.target).push(edge.source);
  }
  const column = new Map();
  for (const row of rows) {
    const barycenter = (id) => {
      const placed = predecessors.get(id).filter((source) => column.has(source));
      if (!placed.length) return 0;
      return placed.reduce((total, source) => total + column.get(source), 0) / placed.length;
    };
    row.sort((a, b) => barycenter(a) - barycenter(b) || (a < b ? -1 : 1));
    row.forEach((id, index) => column.set(id, index));
  }

  const widest = Math.max(...rows.map((row) => row.length));
  const innerWidth = widest * NODE_W + (widest - 1) * H_GAP;
  // Edges that skip layers would cross the nodes between them, so they bow
  // out into a gutter on the left -- deeper the further they reach.
  const spans = edges.map((edge) => (layer.get(edge.target) ?? 0) - (layer.get(edge.source) ?? 0));
  const maxSpan = Math.max(1, ...spans);
  const gutter = maxSpan > 1 ? 16 + 14 * (maxSpan - 1) : 0;

  const placed = new Map();
  rows.forEach((row, rowIndex) => {
    const rowWidth = row.length * NODE_W + (row.length - 1) * H_GAP;
    const offset = (innerWidth - rowWidth) / 2;
    row.forEach((id, index) => {
      placed.set(id, {
        x: PAD + gutter + offset + index * (NODE_W + H_GAP),
        y: PAD + rowIndex * (NODE_H + V_GAP),
      });
    });
  });
  return {
    placed,
    layer,
    gutter,
    width: PAD * 2 + gutter + innerWidth,
    height: PAD * 2 + rows.length * NODE_H + (rows.length - 1) * V_GAP,
  };
}

function edgePath(edge, { placed, layer, gutter }) {
  const from = placed.get(edge.source);
  const to = placed.get(edge.target);
  if (!from || !to) return null;
  const span = layer.get(edge.target) - layer.get(edge.source);
  if (span === 1) {
    const [x1, y1] = [from.x + NODE_W / 2, from.y + NODE_H];
    const [x2, y2] = [to.x + NODE_W / 2, to.y];
    const bend = V_GAP * 0.6;
    return `M ${x1} ${y1} C ${x1} ${y1 + bend} ${x2} ${y2 - bend} ${x2} ${y2}`;
  }
  // Leaves the source's left edge, arcs down the gutter, re-enters the
  // target's left edge -- the arrowhead orients itself.
  const [y1, y2] = [from.y + NODE_H / 2, to.y + NODE_H / 2];
  const bowX = PAD + gutter - 14 * (span - 2) - 8;
  return `M ${from.x} ${y1} C ${bowX} ${y1} ${bowX} ${y2} ${to.x} ${y2}`;
}

// --- nodes --------------------------------------------------------------------

function truncate(text) {
  const value = String(text ?? '');
  return value.length > LABEL_MAX ? `${value.slice(0, LABEL_MAX - 1)}…` : value;
}

function subtitleFor(node) {
  if (node.mapped && node.mapped.length) {
    const done = node.mapped.filter((entry) => entry.state === 'success').length;
    return `${done}/${node.mapped.length} batches`;
  }
  return node.operator || '';
}

function nodeSignature(node) {
  return [node.state, node.airflow_state, node.duration_s, subtitleFor(node), node.label].join('|');
}

// --- the graph ---------------------------------------------------------------

export function createDagGraph({ onSelect, onDrillIn } = {}) {
  const svg = svgEl('svg', { class: 'dag-graph' });
  const wrap = document.createElement('div');
  wrap.className = 'graph-wrap';
  wrap.append(svg);

  let structureKey = null;
  let selectedId = null;
  const groups = new Map(); // node id -> { group, signature }

  const classFor = (node) =>
    `dag-node dag-node--${node.state}${node.id === selectedId ? ' dag-node--selected' : ''}`;

  function buildNode(node, position) {
    const group = svgEl('g', {
      class: classFor(node),
      'data-node-id': node.id,
      transform: `translate(${position.x},${position.y})`,
      tabindex: '0',
      role: 'button',
      onclick: () => onSelect && onSelect(node.id),
      onkeydown: (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          if (onSelect) onSelect(node.id);
        }
      },
    });
    if (node.is_mapped) {
      // A mapped task fans out; the offset card behind says so at a glance.
      group.append(
        svgEl('rect', { class: 'dag-node__stack', x: 5, y: 5, width: NODE_W, height: NODE_H, rx: 7 }),
      );
    }
    group.append(
      svgEl('rect', { class: 'dag-node__box', width: NODE_W, height: NODE_H, rx: 7 }),
      svgEl('circle', { class: 'dag-node__dot', cx: 17, cy: NODE_H / 2, r: 4 }),
      svgEl('text', { class: 'dag-node__label', x: 31, y: 20 }, truncate(node.label)),
      svgEl('text', { class: 'dag-node__sub', x: 31, y: 34 }, subtitleFor(node)),
      svgEl('title', null, `${node.label} -- ${node.state}`),
    );
    if (node.stage && onDrillIn) {
      group.append(
        svgEl('g', {
          class: 'dag-node__drill',
          role: 'link',
          'aria-label': `Open the ${node.stage} stage`,
          onclick: (event) => {
            event.stopPropagation();
            onDrillIn(node.stage);
          },
        },
          svgEl('rect', { x: NODE_W - 26, y: 0, width: 26, height: NODE_H, fill: 'transparent' }),
          svgEl('path', { d: `M ${NODE_W - 16} ${NODE_H / 2 - 4} l 4 4 l -4 4` }),
        ),
      );
    }
    return group;
  }

  function rebuild(data) {
    const geometry = layoutGraph(data.nodes, data.edges);
    svg.replaceChildren(
      svgEl('defs', null,
        svgEl('marker', {
          id: 'dag-arrow', viewBox: '0 0 8 8', refX: 7, refY: 4,
          markerWidth: 6, markerHeight: 6, orient: 'auto',
        }, svgEl('path', { class: 'dag-arrow', d: 'M 0 1 L 7 4 L 0 7 z' })),
      ),
    );
    for (const edge of data.edges) {
      const path = edgePath(edge, geometry);
      if (path) svg.append(svgEl('path', { class: 'dag-edge', d: path, 'marker-end': 'url(#dag-arrow)' }));
    }
    groups.clear();
    for (const node of data.nodes) {
      const position = geometry.placed.get(node.id);
      if (!position) continue;
      const group = buildNode(node, position);
      svg.append(group);
      groups.set(node.id, { group, signature: nodeSignature(node) });
    }
    svg.setAttribute('viewBox', `0 0 ${geometry.width} ${geometry.height}`);
    svg.setAttribute('width', String(geometry.width));
    svg.setAttribute('height', String(geometry.height));
  }

  function restyle(data) {
    for (const node of data.nodes) {
      const entry = groups.get(node.id);
      if (!entry) continue;
      const signature = nodeSignature(node);
      entry.group.setAttribute('class', classFor(node));
      if (entry.signature === signature) continue;
      entry.signature = signature;
      entry.group.querySelector('.dag-node__label').textContent = truncate(node.label);
      entry.group.querySelector('.dag-node__sub').textContent = subtitleFor(node);
      entry.group.querySelector('title').textContent = `${node.label} -- ${node.state}`;
    }
  }

  function structureSignature(data) {
    return [
      data.nodes.map((node) => node.id).join(','),
      data.edges.map((edge) => `${edge.source}>${edge.target}`).join(','),
    ].join('|');
  }

  svg.addEventListener('click', (event) => {
    if (!event.target.closest('.dag-node') && onSelect) onSelect(null);
  });

  return {
    el: wrap,
    render(data) {
      const key = structureSignature(data);
      if (key !== structureKey) {
        structureKey = key;
        rebuild(data);
      } else {
        restyle(data);
      }
    },
    // Selection lives here so it survives polls: the class is reapplied from
    // the id every render, whether or not the node was rebuilt.
    select(id) {
      selectedId = id;
      for (const [nodeId, entry] of groups) {
        entry.group.classList.toggle('dag-node--selected', nodeId === selectedId);
      }
    },
  };
}
