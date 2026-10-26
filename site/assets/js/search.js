/* Lightweight, dependency-free site search.
   Builds an in-memory index by fetching the static pages once on first open.
   Section links use the heading id when present; otherwise a #q=<heading>
   fragment that this script resolves on the target page. */
(() => {
  "use strict";

  const PAGES = [
    { url: "index.html", label: "首页" },
    { url: "install.html", label: "安装" },
    { url: "usage.html", label: "使用" },
    { url: "api.html", label: "API" },
    { url: "changelog.html", label: "版本日志" },
  ];
  const MAX_RESULTS = 12;

  let indexPromise = null;
  let entries = [];
  let active = -1;
  let results = [];

  const norm = (s) => (s || "").toLocaleLowerCase("zh-CN").replace(/\s+/g, " ").trim();
  const clean = (s) => (s || "").replace(/\s+/g, " ").trim();

  function headingLink(page, id, title) {
    return id ? `${page}#${encodeURIComponent(id)}` : `${page}#q=${encodeURIComponent(title)}`;
  }

  function* walkSections(doc, page, label) {
    const main = doc.querySelector("main") || doc.body;
    if (!main) return;
    main.querySelectorAll("script,style,nav,header,footer,svg").forEach((el) => el.remove());
    let current = null;
    for (const el of main.querySelectorAll("h1,h2,h3,p,li,pre,td,th,blockquote,dt,dd")) {
      const text = clean(el.textContent);
      if (!text) continue;
      if (/^H[123]$/.test(el.tagName)) {
        if (current) yield current;
        current = {
          page,
          pageLabel: label,
          title: text,
          id: el.id || "",
          text,
        };
      } else if (current) {
        current.text += ` ${text}`;
      }
    }
    if (current) yield current;
  }

  async function loadIndex() {
    if (indexPromise) return indexPromise;
    indexPromise = (async () => {
      const built = [];
      await Promise.all(
        PAGES.map(async (p) => {
          const res = await fetch(p.url, { cache: "no-cache" });
          if (!res.ok) throw new Error(`${p.url}: HTTP ${res.status}`);
          const html = await res.text();
          const doc = new DOMParser().parseFromString(html, "text/html");
          for (const entry of walkSections(doc, p.url, p.label)) built.push(entry);
        })
      );
      built.sort((a, b) => PAGES.findIndex((p) => p.url === a.page) - PAGES.findIndex((p) => p.url === b.page));
      entries = built;
      return entries;
    })().catch((err) => {
      indexPromise = null;
      throw err;
    });
    return indexPromise;
  }

  function scoreEntry(entry, terms) {
    const title = norm(entry.title);
    const page = norm(entry.pageLabel);
    const hay = norm(`${entry.pageLabel} ${entry.title} ${entry.text}`);
    let score = 0;
    for (const term of terms) {
      if (!hay.includes(term)) return 0;
      score += title.includes(term) ? 24 : 6;
      if (page.includes(term)) score += 4;
      if (norm(entry.text).includes(term)) score += 1;
    }
    return score;
  }

  function snippet(entry, terms) {
    const text = clean(entry.text);
    const hay = norm(text);
    let at = -1;
    for (const term of terms) {
      const i = hay.indexOf(term);
      if (i >= 0 && (at < 0 || i < at)) at = i;
    }
    if (at < 0) at = 0;
    const start = Math.max(0, at - 48);
    const end = Math.min(text.length, start + 170);
    return (start > 0 ? "…" : "") + text.slice(start, end) + (end < text.length ? "…" : "");
  }

  function highlightFragment(text, terms) {
    const frag = document.createDocumentFragment();
    const lower = norm(text);
    const marks = [];
    for (const term of terms) {
      let from = 0;
      while (from < lower.length) {
        const i = lower.indexOf(term, from);
        if (i < 0) break;
        marks.push([i, i + term.length]);
        from = i + term.length;
      }
    }
    marks.sort((a, b) => a[0] - b[0]);
    let pos = 0;
    for (const [a, b] of marks) {
      if (a < pos) continue;
      if (a > pos) frag.append(text.slice(pos, a));
      const mark = document.createElement("mark");
      mark.textContent = text.slice(a, b);
      frag.append(mark);
      pos = b;
    }
    if (pos < text.length) frag.append(text.slice(pos));
    return frag;
  }

  function render(query) {
    const terms = norm(query).split(" ").filter(Boolean);
    const list = panel.querySelector(".search-results");
    if (!terms.length) {
      list.replaceChildren(empty("输入关键词搜索文档、配置与 API。"));
      meta.textContent = "支持中文与英文关键词；多个词用空格分隔。";
      results = [];
      active = -1;
      return;
    }
    results = entries
      .map((entry) => ({ entry, score: scoreEntry(entry, terms) }))
      .filter((r) => r.score > 0)
      .sort((a, b) => b.score - a.score || a.entry.title.localeCompare(b.entry.title, "zh-CN"))
      .slice(0, MAX_RESULTS);
    active = results.length ? 0 : -1;
    list.replaceChildren();
    if (!results.length) {
      list.append(empty("没有找到匹配内容，试试更短的关键词。"));
      meta.textContent = "0 条结果";
      return;
    }
    meta.textContent = `${results.length} 条结果`;
    results.forEach((r, i) => {
      const a = document.createElement("a");
      a.className = `search-result${i === active ? " is-active" : ""}`;
      a.href = headingLink(r.entry.page, r.entry.id, r.entry.title);
      a.setAttribute("role", "option");
      a.setAttribute("aria-selected", i === active ? "true" : "false");
      const title = document.createElement("span");
      title.className = "search-result-title";
      const name = document.createElement("span");
      name.append(highlightFragment(r.entry.title, terms));
      const page = document.createElement("span");
      page.className = "search-result-page";
      page.textContent = r.entry.pageLabel;
      title.append(name, page);
      const snip = document.createElement("span");
      snip.className = "search-result-snippet";
      snip.append(highlightFragment(snippet(r.entry, terms), terms));
      a.append(title, snip);
      a.addEventListener("mousemove", () => setActive(i));
      list.append(a);
    });
  }

  function empty(message) {
    const p = document.createElement("p");
    p.className = "search-empty";
    p.textContent = message;
    return p;
  }

  function setActive(i) {
    if (!results.length) return;
    active = (i + results.length) % results.length;
    const nodes = panel.querySelectorAll(".search-result");
    nodes.forEach((n, j) => {
      n.classList.toggle("is-active", j === active);
      n.setAttribute("aria-selected", j === active ? "true" : "false");
    });
    nodes[active]?.scrollIntoView({ block: "nearest" });
  }

  function open() {
    overlay.hidden = false;
    document.body.classList.add("search-open");
    toggle.setAttribute("aria-expanded", "true");
    input.value = "";
    render("");
    meta.textContent = "正在加载索引…";
    loadIndex()
      .then(() => {
        meta.textContent = "支持中文与英文关键词；多个词用空格分隔。";
        if (input.value.trim()) render(input.value);
        input.focus();
      })
      .catch((err) => {
        console.error("site search index failed", err);
        meta.textContent = location.protocol === "file:"
          ? "搜索需要 HTTP 访问（本地预览请用 python -m http.server）。"
          : "搜索索引加载失败，请刷新重试。";
        panel.querySelector(".search-results").replaceChildren(empty("索引不可用。"));
      });
    input.focus();
  }

  function close() {
    overlay.hidden = true;
    document.body.classList.remove("search-open");
    toggle.setAttribute("aria-expanded", "false");
    toggle.focus();
  }

  function go() {
    const r = results[active];
    if (r) location.href = headingLink(r.entry.page, r.entry.id, r.entry.title);
  }

  // Build the button and dialog once per page.
  const actions = document.querySelector(".header-actions");
  if (!actions || document.getElementById("site-search")) return;

  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.className = "search-toggle";
  toggle.setAttribute("aria-label", "搜索站点");
  toggle.setAttribute("aria-expanded", "false");
  toggle.setAttribute("aria-controls", "site-search");
  toggle.innerHTML =
    '<span class="search-toggle-icon" aria-hidden="true">⌕</span>' +
    '<span class="search-toggle-label">搜索</span><kbd>⌘K</kbd>';
  const menu = actions.querySelector(".menu-toggle");
  actions.insertBefore(toggle, menu || null);

  const overlay = document.createElement("div");
  overlay.className = "search-overlay";
  overlay.id = "site-search";
  overlay.hidden = true;
  overlay.innerHTML =
    '<div class="search-backdrop" data-search-close></div>' +
    '<div class="search-panel" role="dialog" aria-modal="true" aria-labelledby="site-search-title">' +
    '<div class="search-bar"><span class="search-icon" aria-hidden="true">⌕</span>' +
    '<h2 class="sr-only" id="site-search-title">搜索站点</h2>' +
    '<input id="site-search-input" type="search" placeholder="搜索文档、配置、API…" autocomplete="off" spellcheck="false" aria-label="搜索站点">' +
    '<button class="search-close" type="button" aria-label="关闭搜索" data-search-close>Esc</button></div>' +
    '<div class="search-meta"><span class="search-count">输入关键词开始搜索</span><span class="search-hint">↑↓ 选择 · Enter 打开</span></div>' +
    '<div class="search-results" role="listbox" aria-label="搜索结果"></div>' +
    '<div class="search-footer">搜索范围：首页 · 安装 · 使用 · API · 版本日志</div></div>';
  document.body.append(overlay);

  const panel = overlay.querySelector(".search-panel");
  const input = overlay.querySelector("#site-search-input");
  const meta = overlay.querySelector(".search-count");

  toggle.addEventListener("click", open);
  overlay.addEventListener("click", (e) => {
    if (e.target.closest("[data-search-close]")) close();
  });
  input.addEventListener("input", () => render(input.value));
  input.addEventListener("keydown", (e) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive(active + 1);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive(active - 1);
    } else if (e.key === "Enter") {
      e.preventDefault();
      go();
    } else if (e.key === "Escape") {
      e.preventDefault();
      close();
    }
  });
  bindShortcuts();
  function bindShortcuts() {
    document.addEventListener("keydown", (e) => {
      const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(e.target?.tagName || "");
      if ((e.key === "k" || e.key === "K") && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        overlay.hidden ? open() : close();
      } else if (e.key === "/" && !typing && overlay.hidden) {
        e.preventDefault();
        open();
      }
    });
  }

  // Resolve #q=<heading> links generated for headings without an id.
  if (location.hash.startsWith("#q=")) {
    let wanted = "";
    try {
      wanted = norm(decodeURIComponent(location.hash.slice(3)));
    } catch {
      // Malformed fragment: leave the page at the top, no search error.
    }
    const heading = wanted
      ? [...document.querySelectorAll("h1,h2,h3")].find((h) => norm(h.textContent) === wanted)
      : null;
    if (heading) {
      heading.scrollIntoView({ block: "start" });
      heading.classList.add("search-flash");
      setTimeout(() => heading.classList.remove("search-flash"), 1800);
      history.replaceState(null, "", location.pathname + location.search);
    }
  }
})();
