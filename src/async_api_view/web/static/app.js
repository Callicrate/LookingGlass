"use strict";

(() => {
  for (const form of document.querySelectorAll("form.refresh-row")) {
    form.addEventListener("submit", () => {
      const button = form.querySelector('button[type="submit"]');
      if (!button || button.disabled) return;
      button.disabled = true;
      button.setAttribute("aria-busy", "true");
      button.textContent = "Requesting…";
    });
  }

  const page = document.querySelector("[data-intent-page]");
  if (!page) return;

  const pollUrl = page.dataset.pollUrl;
  const pollState = document.querySelector("[data-poll-state]");
  const pulse = document.querySelector(".pulse");
  const updatedAt = document.querySelector("[data-updated-at]");
  const errorBox = document.querySelector("[data-intent-error]");
  let timer = null;
  let failures = 0;

  const setText = (element, value, fallback = "Unknown") => {
    const resolved = value || fallback;
    if (element && element.textContent !== resolved) element.textContent = resolved;
  };

  const setPollState = (text, state = "active") => {
    setText(pollState, text);
    if (!pulse) return;
    pulse.classList.toggle("pulse--disconnected", state === "disconnected");
    pulse.classList.toggle("pulse--unavailable", state === "unavailable");
    pulse.classList.toggle("pulse--final", state === "final");
  };

  const scheduleRetry = (text, state) => {
    failures += 1;
    setPollState(text, state);
    const delay = Math.min(15000, 3000 * (2 ** Math.min(failures, 2)));
    timer = window.setTimeout(poll, delay);
  };

  const updateScope = (scope, index) => {
    const row = document.querySelector(`[data-scope-index="${index}"]`);
    if (!row) return;
    for (const field of ["label", "state", "target_kind", "target_id", "action_id", "eligible_at", "failure", "cached_context"]) {
      const element = row.querySelector(`[data-field="${field}"]`);
      if (field === "action_id" && element && scope[field]) {
        const link = document.createElement("a");
        link.href = `/actions/${encodeURIComponent(scope[field])}`;
        link.textContent = scope[field];
        element.replaceChildren(link);
      } else {
        const value = field === "state" ? String(scope[field] ?? "").replaceAll("_", " ") : scope[field];
        setText(element, value, field === "failure" ? "No failure recorded" : "Unknown");
      }
    }
    const badge = row.querySelector('[data-field="state"]');
    if (badge) badge.className = `badge badge--${String(scope.state).replace(/[^a-z_-]/g, "")}`;
    const failure = row.querySelector('[data-field="failure"]');
    if (failure) {
      failure.classList.remove("failure", "warning", "subtle");
      failure.classList.add(
        scope.failure ? (["failed", "rejected"].includes(scope.state) ? "failure" : "warning") : "subtle",
      );
    }
  };

  const poll = async () => {
    try {
      const response = await fetch(pollUrl, {
        headers: { Accept: "application/json" },
        cache: "no-store",
        credentials: "same-origin",
      });
      if (response.status === 403 || response.status === 404) {
        window.location.reload();
        return;
      }
      if (response.status >= 500) {
        scheduleRetry(
          "Status unavailable · retrying while cached status stays visible",
          "unavailable",
        );
        return;
      }
      if (!response.ok) throw new Error("status unavailable");
      const payload = await response.json();
      failures = 0;
      setText(updatedAt, payload.updated_at);
      payload.scopes.forEach(updateScope);
      setText(errorBox, payload.error, "");
      errorBox.classList.toggle("is-hidden", !payload.error);
      if (payload.terminal) {
        setPollState("Final state", "final");
        return;
      }
      setPollState("Status current · checking every 3 seconds");
      timer = window.setTimeout(poll, 3000);
    } catch (_error) {
      scheduleRetry(
        "Disconnected · retrying while cached status stays visible",
        "disconnected",
      );
    }
  };

  window.addEventListener("pagehide", () => {
    if (timer !== null) window.clearTimeout(timer);
  });
  if (page.dataset.intentTerminal === "true") {
    setPollState("Final state", "final");
    return;
  }
  timer = window.setTimeout(poll, 1000);
})();
