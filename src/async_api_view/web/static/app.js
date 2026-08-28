"use strict";

(() => {
  const page = document.querySelector("[data-intent-page]");
  if (!page) return;

  const pollUrl = page.dataset.pollUrl;
  const pollState = document.querySelector("[data-poll-state]");
  const updatedAt = document.querySelector("[data-updated-at]");
  const errorBox = document.querySelector("[data-intent-error]");
  let timer = null;
  let failures = 0;

  const setText = (element, value, fallback = "Unknown") => {
    if (element) element.textContent = value || fallback;
  };

  const updateScope = (scope, index) => {
    const row = document.querySelector(`[data-scope-index="${index}"]`);
    if (!row) return;
    for (const field of ["label", "state", "target_kind", "target_id", "action_id", "eligible_at", "failure", "cached_context"]) {
      const element = row.querySelector(`[data-field="${field}"]`);
      setText(element, scope[field], field === "failure" ? "None" : "Unknown");
    }
    const badge = row.querySelector('[data-field="state"]');
    if (badge) badge.className = `badge badge--${String(scope.state).replace(/[^a-z_-]/g, "")}`;
  };

  const poll = async () => {
    try {
      const response = await fetch(pollUrl, {
        headers: { Accept: "application/json" },
        cache: "no-store",
        credentials: "same-origin",
      });
      if (!response.ok) throw new Error("status unavailable");
      const payload = await response.json();
      failures = 0;
      setText(updatedAt, payload.updated_at);
      payload.scopes.forEach(updateScope);
      setText(errorBox, payload.error, "");
      errorBox.classList.toggle("is-hidden", !payload.error);
      if (payload.terminal) {
        setText(pollState, "Final state");
        return;
      }
      setText(pollState, "Status current · checking every 3 seconds");
      timer = window.setTimeout(poll, 3000);
    } catch (_error) {
      failures += 1;
      setText(pollState, "Disconnected · retrying while cached status stays visible");
      const delay = Math.min(15000, 3000 * (2 ** Math.min(failures, 2)));
      timer = window.setTimeout(poll, delay);
    }
  };

  window.addEventListener("pagehide", () => {
    if (timer !== null) window.clearTimeout(timer);
  });
  timer = window.setTimeout(poll, 1000);
})();
