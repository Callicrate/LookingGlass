"use strict";

(() => {
  const input = document.querySelector("[data-bootstrap-token]");
  const errorBox = document.querySelector("[data-bootstrap-error]");
  const token = window.location.hash.slice(1);
  if (!token) return;

  window.history.replaceState(null, "", `${window.location.pathname}${window.location.search}`);
  if (!/^[A-Za-z0-9_-]{32,128}$/.test(token)) return;

  const exchange = async () => {
    try {
      const response = await fetch("/bootstrap", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ bootstrap_token: token }),
        credentials: "same-origin",
      });
      if (!response.ok) throw new Error("activation rejected");
      window.location.replace("/");
    } catch (_error) {
      if (errorBox) errorBox.classList.remove("is-hidden");
      if (input) input.focus();
    }
  };

  void exchange();
})();
