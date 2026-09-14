(() => {
  "use strict";
  const byId = (id) => document.getElementById(id);
  const tokenKey = `x-sync-chat-token:${location.origin}`;
  const draftKey = `x-sync-chat-draft:${location.origin}`;
  const pendingKey = `x-sync-chat-pending:${location.origin}`;
  const token = location.hash ? decodeURIComponent(location.hash.slice(1)) : sessionStorage.getItem(tokenKey);
  if (location.hash && token) {
    sessionStorage.setItem(tokenKey, token);
    history.replaceState(null, "", location.pathname);
  }
  const prompt = byId("prompt");
  const send = byId("send");
  const nodes = new Map();
  let state = null;
  let connected = false;
  let submitting = false;
  let pendingRequest = null;
  let streamController = null;
  prompt.value = sessionStorage.getItem(draftKey) || "";
  try {
    const saved = JSON.parse(sessionStorage.getItem(pendingKey) || "null");
    if (saved && typeof saved.text === "string" && typeof saved.request_id === "string") {
      pendingRequest = { text: saved.text, request_id: saved.request_id };
    }
  } catch (_error) { sessionStorage.removeItem(pendingKey); }

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function notice(text = "") {
    byId("notice").textContent = text;
    byId("notice").hidden = !text;
  }

  async function request(path, options = {}) {
    const response = await fetch(path, {
      ...options, headers: { "Authorization": `Bearer ${token || ""}`, ...options.headers },
    });
    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      const error = new Error(data.error || `连接失败（${response.status}）`);
      error.status = response.status;
      throw error;
    }
    return response;
  }

  function inline(parent, text) {
    const pattern = /(`[^`\n]+`|\*\*[^*\n]+\*\*|\[[^\]\n]+\]\([^\s)]+\))/g;
    let offset = 0;
    for (const match of text.matchAll(pattern)) {
      parent.append(document.createTextNode(text.slice(offset, match.index)));
      const value = match[0];
      if (value.startsWith("`")) parent.append(element("code", "", value.slice(1, -1)));
      else if (value.startsWith("**")) parent.append(element("strong", "", value.slice(2, -2)));
      else {
        const parts = /^\[([^\]]+)\]\(([^)]+)\)$/.exec(value);
        const link = element(/^https?:\/\//i.test(parts[2]) ? "a" : "code", "", parts[1]);
        if (link.tagName === "A") {
          link.href = parts[2];
          link.target = "_blank";
          link.rel = "noopener noreferrer";
        } else link.title = parts[2];
        parent.append(link);
      }
      offset = match.index + value.length;
    }
    parent.append(document.createTextNode(text.slice(offset)));
  }

  function copyButton(text, label = "复制") {
    const button = element("button", "quiet", label);
    button.type = "button";
    button.onclick = async () => {
      try {
        await navigator.clipboard.writeText(text);
        button.textContent = "已复制";
      } catch (_error) { notice("无法访问剪贴板，请选中文字复制。"); }
    };
    return button;
  }

  function markdown(text) {
    const result = document.createDocumentFragment();
    const lines = text.replace(/\r\n/g, "\n").split("\n");
    let index = 0;
    while (index < lines.length) {
      const line = lines[index];
      if (!line.trim()) { index += 1; continue; }
      if (/^\s*```/.test(line)) {
        const language = line.trim().slice(3).trim();
        const code = [];
        index += 1;
        while (index < lines.length && !/^\s*```/.test(lines[index])) code.push(lines[index++]);
        index += 1;
        const block = element("div", "code-block");
        const heading = element("div", "code-heading");
        heading.append(element("span", "", language || "代码"), copyButton(code.join("\n"), "复制代码"));
        const pre = element("pre");
        pre.append(element("code", "", code.join("\n")));
        block.append(heading, pre);
        result.append(block);
        continue;
      }
      if (line.includes("|") && /^\s*\|?\s*:?-{3,}/.test(lines[index + 1] || "")) {
        const split = (value) => value.trim().replace(/^\||\|$/g, "").split("|").map((cell) => cell.trim());
        const table = element("table");
        const row = element("tr");
        split(line).forEach((cell) => { const th = element("th"); inline(th, cell); row.append(th); });
        const head = element("thead"); head.append(row); table.append(head);
        const body = element("tbody");
        index += 2;
        while (index < lines.length && lines[index].includes("|") && lines[index].trim()) {
          const tr = element("tr");
          split(lines[index++]).forEach((cell) => { const td = element("td"); inline(td, cell); tr.append(td); });
          body.append(tr);
        }
        table.append(body);
        const wrap = element("div", "table-wrap"); wrap.append(table); result.append(wrap);
        continue;
      }
      const heading = /^(#{1,6})\s+(.+)$/.exec(line);
      if (heading) {
        const title = element(`h${Math.min(heading[1].length + 1, 6)}`);
        inline(title, heading[2]); result.append(title); index += 1; continue;
      }
      const list = /^\s*(?:[-*+] |\d+\. )/.exec(line);
      if (list) {
        const node = element(/^\s*\d/.test(line) ? "ol" : "ul");
        while (index < lines.length && /^\s*(?:[-*+] |\d+\. )/.test(lines[index])) {
          const item = element("li");
          inline(item, lines[index++].replace(/^\s*(?:[-*+] |\d+\. )/, "")); node.append(item);
        }
        result.append(node); continue;
      }
      if (/^>\s?/.test(line)) {
        const quote = element("blockquote"); inline(quote, line.replace(/^>\s?/, ""));
        result.append(quote); index += 1; continue;
      }
      const paragraph = [line];
      index += 1;
      while (index < lines.length && lines[index].trim()
        && !/^(?:#{1,6}\s|\s*```|\s*[-*+] |\s*\d+\. |>)/.test(lines[index])) {
        if (lines[index].includes("|") && /^\s*\|?\s*:?-{3,}/.test(lines[index + 1] || "")) break;
        paragraph.push(lines[index++]);
      }
      const p = element("p"); inline(p, paragraph.join("\n")); result.append(p);
    }
    return result;
  }

  function suggestions(parent, questions) {
    parent.replaceChildren();
    questions.forEach((question) => {
      const button = element("button", "suggestion", question);
      button.type = "button";
      button.disabled = !!state?.busy || submitting;
      button.onclick = () => {
        prompt.value = question;
        saveDraft();
        byId("suggestion-tray").hidden = true;
        byId("recommend").setAttribute("aria-expanded", "false");
        submit();
      };
      parent.append(button);
    });
  }

  function controls() {
    send.disabled = !connected || submitting || (!state?.busy && !prompt.value.trim());
    send.type = state?.busy ? "button" : "submit";
    send.onclick = state?.busy ? () => mutate("/api/chat/stop", {}) : null;
    send.classList.toggle("stopping", !!state?.busy);
    send.setAttribute("aria-label", state?.busy ? "停止回答" : "发送问题");
    send.firstElementChild.textContent = state?.busy ? "■" : "↑";
    byId("export").disabled = !connected || !state?.messages.length;
    byId("recommend").disabled = !connected || !!state?.busy || submitting;
    byId("status").textContent = !connected ? "连接已断开，正在重新连接…"
      : state?.busy ? "正在回答…" : "";
  }

  function apply(next, fresh = false) {
    if (!fresh && state && next.session_id === state.session_id && next.revision < state.revision) return;
    const nearBottom = document.documentElement.scrollHeight - window.innerHeight - window.scrollY < 180;
    state = next;
    if (pendingRequest && state.messages.some((message) => message.role === "user" && message.request_id === pendingRequest.request_id)) {
      if (prompt.value.trim() === pendingRequest.text) { prompt.value = ""; saveDraft(); }
      pendingRequest = null;
      sessionStorage.removeItem(pendingKey);
    }
    byId("repository").textContent = state.repository;
    byId("host").textContent = state.host === "codex" ? "Codex" : "Claude Code";
    byId("welcome").hidden = state.messages.length > 0;
    suggestions(byId("suggestions"), state.suggestions);
    suggestions(byId("suggestion-tray"), state.suggestions);
    const present = new Set();
    state.messages.forEach((message, index) => {
      present.add(message.id);
      let node = nodes.get(message.id);
      if (!node) {
        node = element("article", `message ${message.role}`);
        node.dataset.messageId = message.id;
        byId("messages").append(node);
        nodes.set(message.id, node);
      }
      const fingerprint = JSON.stringify([message.text, message.status, message.error, index === state.messages.length - 1]);
      if (node.dataset.fingerprint === fingerprint) return;
      node.dataset.fingerprint = fingerprint;
      node.replaceChildren();
      if (message.role === "assistant") node.append(element("div", "author", message.host === "codex" ? "Codex" : "Claude Code"));
      const body = element("div", "message-body");
      if (message.role === "user") body.textContent = message.text;
      else if (message.text) body.append(markdown(message.text));
      else if (message.status === "pending") body.append(element("div", "thinking", "正在理解你的问题…"));
      node.append(body);
      if (message.error) node.append(element("p", "message-error", message.error));
      if (message.role === "assistant" && message.status !== "pending") {
        const actions = element("div", "message-actions");
        if (message.text) actions.append(copyButton(message.text));
        if (["error", "cancelled"].includes(message.status) && index === state.messages.length - 1) {
          const retry = element("button", "quiet", "重新回答");
          retry.type = "button";
          retry.onclick = () => mutate("/api/chat/retry", { message_id: message.id });
          actions.append(retry);
        }
        node.append(actions);
      }
    });
    for (const [id, node] of nodes) if (!present.has(id)) { node.remove(); nodes.delete(id); }
    controls();
    if (nearBottom) requestAnimationFrame(() => window.scrollTo({ top: document.documentElement.scrollHeight, behavior: "instant" }));
  }

  async function mutate(path, body) {
    notice();
    try {
      const response = await request(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      apply(await response.json());
      return true;
    } catch (error) { notice(error.message); return false; }
  }

  async function submit() {
    const text = prompt.value.trim();
    if (!text || state?.busy || submitting || !connected) return;
    if (new TextEncoder().encode(text).length > 32000) { notice("问题过长，请缩短到 32 KB 以内。"); return; }
    if (!pendingRequest || pendingRequest.text !== text) pendingRequest = { text, request_id: crypto.randomUUID() };
    sessionStorage.setItem(pendingKey, JSON.stringify(pendingRequest));
    submitting = true;
    controls();
    const committed = await mutate("/api/chat/messages", pendingRequest);
    if (committed) {
      if (prompt.value.trim() === text) prompt.value = "";
      pendingRequest = null;
      sessionStorage.removeItem(pendingKey);
      saveDraft();
      window.scrollTo({ top: document.documentElement.scrollHeight, behavior: "instant" });
    }
    submitting = false;
    controls();
    prompt.focus();
  }

  function saveDraft() {
    sessionStorage.setItem(draftKey, prompt.value);
    prompt.style.height = "auto";
    prompt.style.height = `${Math.min(prompt.scrollHeight, 180)}px`;
    controls();
  }

  byId("composer").onsubmit = (event) => {
    event.preventDefault();
    if (state?.busy) mutate("/api/chat/stop", {});
    else submit();
  };
  prompt.oninput = saveDraft;
  prompt.onkeydown = (event) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing && event.keyCode !== 229) {
      event.preventDefault();
      if (!state?.busy) submit();
    }
  };
  byId("recommend").onclick = () => {
    const tray = byId("suggestion-tray");
    tray.hidden = !tray.hidden;
    byId("recommend").setAttribute("aria-expanded", String(!tray.hidden));
  };
  byId("export").onclick = async () => {
    try {
      const response = await request("/api/chat/export", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const link = element("a");
      link.href = url;
      link.download = /filename="([^"]+)"/.exec(response.headers.get("Content-Disposition") || "")?.[1] || "x-sync.md";
      document.body.append(link); link.click(); link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      notice("Markdown 已导出，也已在本机保存一份。");
    } catch (error) { notice(error.message); }
  };

  async function connect() {
    if (!token) { notice("请使用启动时提供的完整链接打开页面。"); return; }
    while (true) {
      try {
        streamController = new AbortController();
        const initial = await request("/api/chat/state", { signal: streamController.signal });
        connected = true;
        apply(await initial.json(), true);
        const response = await request("/api/chat/events", { signal: streamController.signal });
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const { value, done } = await reader.read();
          if (done) throw new Error("连接中断，正在恢复…");
          buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
          let boundary;
          while ((boundary = buffer.indexOf("\n\n")) !== -1) {
            const event = buffer.slice(0, boundary); buffer = buffer.slice(boundary + 2);
            const data = event.split("\n").filter((line) => line.startsWith("data: ")).map((line) => line.slice(6)).join("\n");
            if (data) apply(JSON.parse(data));
          }
        }
      } catch (error) {
        connected = false;
        controls();
        if (error.name === "AbortError") return;
        if ([401, 403].includes(error.status)) { notice(error.message); return; }
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }
  }
  window.addEventListener("pagehide", () => streamController?.abort());
  saveDraft();
  connect();
})();
