(() => {
  "use strict";

  const byId = (id) => document.getElementById(id);
  const tokenKey = "x-sync-v2-capability";
  const hashToken = window.location.hash.startsWith("#")
    ? decodeURIComponent(window.location.hash.slice(1))
    : "";
  if (hashToken) {
    window.sessionStorage.setItem(tokenKey, hashToken);
    window.history.replaceState(null, "", window.location.pathname);
  }
  const capability = window.sessionStorage.getItem(tokenKey) || "";
  let snapshot = null;
  let cursor = 0;
  let streamStopped = false;

  const connection = byId("connection");
  const errorBox = byId("error");

  function showError(message) {
    errorBox.textContent = message;
    errorBox.hidden = false;
  }

  function clearError() {
    errorBox.textContent = "";
    errorBox.hidden = true;
  }

  function idempotencyKey(prefix) {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return `${prefix}.${window.crypto.randomUUID()}`;
    }
    const bytes = new Uint8Array(16);
    window.crypto.getRandomValues(bytes);
    return `${prefix}.${Array.from(bytes, (item) => item.toString(16).padStart(2, "0")).join("")}`;
  }

  async function request(path, options = {}) {
    const headers = new Headers(options.headers || {});
    headers.set("Authorization", `Bearer ${capability}`);
    const response = await window.fetch(path, { ...options, headers });
    if (!response.ok) {
      let code = `HTTP_${response.status}`;
      try {
        const payload = await response.json();
        code = payload.error && payload.error.code ? payload.error.code : code;
      } catch (_ignored) {
        // Stable HTTP status remains enough when the body is unavailable.
      }
      throw new Error(code);
    }
    return response;
  }

  async function refresh() {
    const response = await request("/api/v2/state");
    snapshot = await response.json();
    cursor = Math.max(cursor, snapshot.event_sequence);
    render(snapshot);
  }

  async function mutate(path, body, prefix) {
    if (!snapshot) return false;
    clearError();
    let committed = false;
    document.querySelectorAll("button").forEach((button) => { button.disabled = true; });
    try {
      const response = await request(path, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": idempotencyKey(prefix),
          "If-Match": `"conversation-v${snapshot.conversation_version}"`,
        },
        body: JSON.stringify(body),
      });
      const payload = await response.json();
      snapshot = payload.state;
      cursor = Math.max(cursor, snapshot.event_sequence);
      render(snapshot);
      committed = true;
    } catch (error) {
      showError(`操作未完成：${error.message}`);
      await refresh().catch(() => {});
    } finally {
      document.querySelectorAll("button").forEach((button) => { button.disabled = false; });
    }
    return committed;
  }

  function renderCandidates(state) {
    const panel = byId("candidates");
    const list = byId("candidate-list");
    const visible = state.allowed_actions.includes("select");
    panel.hidden = !visible;
    list.replaceChildren();
    if (!visible) return;
    state.candidates.forEach((candidate) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "candidate";
      button.textContent = candidate;
      button.addEventListener("click", () => mutate(
        "/api/v2/topic",
        { action: "select", candidate },
        "select",
      ));
      list.append(button);
    });
    byId("custom-topic-form").onsubmit = (event) => {
      event.preventDefault();
      const customTopic = byId("custom-topic");
      const topic = customTopic.value.trim();
      if (!topic) return;
      mutate(
        "/api/v2/topic",
        { action: "custom", topic },
        "custom-topic",
      ).then((committed) => {
        if (committed) customTopic.value = "";
      });
    };
  }

  function renderTopic(state) {
    const topic = state.topic;
    const panel = byId("topic");
    panel.hidden = !topic;
    if (!topic) return;
    byId("topic-lens").textContent = topic.lens;
    const lens = byId("lens");
    lens.value = topic.lens;
    lens.disabled = !state.allowed_actions.includes("set_lens");
    lens.onchange = () => {
      if (lens.value === topic.lens) return;
      mutate(
        "/api/v2/topic",
        { action: "set_lens", lens: lens.value },
        "set-lens",
      );
    };
    byId("topic-title").textContent = topic.title;
    byId("topic-objective").textContent = topic.objective;
    byId("guiding-question").textContent = topic.guiding_question;
    const pause = byId("pause");
    pause.hidden = !state.allowed_actions.includes("pause");
    pause.onclick = () => mutate("/api/v2/topic", { action: "pause" }, "pause");
    const switchTopic = byId("switch");
    switchTopic.hidden = !state.allowed_actions.includes("switch");
    switchTopic.onclick = () => mutate(
      "/api/v2/topic",
      { action: "switch" },
      "switch",
    );

    const question = topic.question;
    const card = byId("question");
    card.hidden = !question;
    if (!question) return;
    byId("heard").textContent = question.heard;
    byId("one-step").textContent = question.one_step_further;
    byId("question-text").textContent = question.text;
    byId("answer-form").onsubmit = (event) => {
      event.preventDefault();
      const answer = byId("answer");
      const text = answer.value.trim();
      if (!text) return;
      mutate(
        "/api/v2/turns",
        { question_id: question.id, text },
        "turn",
      ).then((committed) => {
        if (committed) answer.value = "";
      });
    };
  }

  function renderClarification(state) {
    const clarification = state.topic_clarification;
    const panel = byId("clarification");
    const visible = state.allowed_actions.includes("answer_clarification")
      && clarification
      && !clarification.answered;
    panel.hidden = !visible;
    if (!visible) return;
    byId("clarification-question").textContent = clarification.question;
    byId("clarification-form").onsubmit = (event) => {
      event.preventDefault();
      const answer = byId("clarification-answer");
      const text = answer.value.trim();
      if (!text) return;
      mutate(
        "/api/v2/topic",
        {
          action: "answer_clarification",
          question_id: clarification.question_id,
          answer: text,
        },
        "topic-clarification",
      ).then((committed) => {
        if (committed) answer.value = "";
      });
    };
  }

  function renderPaused(state) {
    const panel = byId("paused");
    const list = byId("paused-list");
    const visible = state.allowed_actions.includes("resume") && state.paused_topics.length > 0;
    panel.hidden = !visible;
    list.replaceChildren();
    if (!visible) return;
    state.paused_topics.forEach((topic) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "paused-item";
      button.textContent = `继续：${topic.title}`;
      button.addEventListener("click", () => mutate(
        "/api/v2/topic",
        { action: "resume", topic_run_id: topic.id },
        "resume",
      ));
      list.append(button);
    });
  }

  function render(state) {
    connection.textContent = `已连接 · ${state.phase}`;
    renderCandidates(state);
    renderClarification(state);
    renderTopic(state);
    renderPaused(state);
    const waiting = byId("waiting");
    waiting.hidden = state.phase !== "waiting_host";
    byId("selected-candidate").textContent = state.selected_candidate
      ? `你选择了「${state.selected_candidate}」`
      : "";
    byId("recoverable").hidden = state.phase !== "recoverable_error";
  }

  async function consumeStream() {
    while (!streamStopped) {
      try {
        const response = await request(`/api/v2/stream?after=${cursor}`);
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffered = "";
        while (!streamStopped) {
          const { value, done } = await reader.read();
          if (done) break;
          buffered += decoder.decode(value, { stream: true });
          const frames = buffered.split("\n\n");
          buffered = frames.pop() || "";
          for (const frame of frames) {
            const dataLine = frame.split("\n").find((line) => line.startsWith("data: "));
            if (!dataLine) continue;
            const event = JSON.parse(dataLine.slice(6));
            if (Number.isInteger(event.sequence)) cursor = Math.max(cursor, event.sequence);
            await refresh();
          }
        }
      } catch (_error) {
        connection.textContent = "正在重新连接…";
        await refresh().catch(() => {});
        await new Promise((resolve) => window.setTimeout(resolve, 1000));
      }
    }
  }

  window.addEventListener("pagehide", () => { streamStopped = true; });
  if (!capability) {
    connection.textContent = "未认证";
    showError("缺少本页的临时访问能力。请从 X-Sync Runtime 重新打开页面。");
    return;
  }
  refresh()
    .then(() => consumeStream())
    .catch((error) => {
      connection.textContent = "连接失败";
      showError(`无法读取会话：${error.message}`);
    });
})();
