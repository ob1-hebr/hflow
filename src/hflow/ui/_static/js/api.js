// Fetch wrapper for the hflow ui JSON API.
//
// Error model: network-level failures throw ApiError("unreachable") with
// status 0; non-2xx responses throw an ApiError carrying the backend's
// {"error", "hint"} envelope. Two consecutive network failures show the
// connection banner and any completed response hides it -- a 503 from the
// runs endpoint still proves the hflow ui server itself is reachable.

export class ApiError extends Error {
  constructor(message, status = 0, hint = null) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.hint = hint;
  }
}

let consecutiveNetworkFailures = 0;

function setBannerVisible(visible) {
  const banner = document.getElementById('connection-banner');
  if (banner) banner.hidden = !visible;
}

function noteNetworkFailure() {
  consecutiveNetworkFailures += 1;
  if (consecutiveNetworkFailures >= 2) setBannerVisible(true);
}

function noteReachable() {
  consecutiveNetworkFailures = 0;
  setBannerVisible(false);
}

export async function api(path, { method = 'GET', body } = {}) {
  let response;
  try {
    response = await fetch(path, {
      method,
      headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch {
    noteNetworkFailure();
    throw new ApiError('unreachable');
  }
  noteReachable();
  if (response.status === 204) return null;
  if (!response.ok) {
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      // Non-JSON error body; fall through to a status-only message.
    }
    throw new ApiError(
      (payload && payload.error) || `HTTP ${response.status}`,
      response.status,
      (payload && payload.hint) || null,
    );
  }
  return response.json();
}
