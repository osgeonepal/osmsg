// Editors and related-hashtags cards, rendered as plain HTML lists.

function _ensureChartsSection() {
  if (document.getElementById("osmsg-charts-row")) return;

  const main = document.querySelector("main[role='main']");
  if (!main) return;

  const section = document.createElement("section");
  section.setAttribute("aria-label", "Charts");
  section.id = "osmsg-charts-section";
  section.innerHTML = `
    <div id="osmsg-charts-row">

      <div id="editor-chart-card" class="osmsg-chart-card" hidden>
        <div class="osmsg-chart-title">
          <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"
               fill="none" stroke="currentColor" stroke-width="2"
               stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <rect x="3" y="3" width="18" height="18" rx="2"/>
            <path d="M3 9h18M9 21V9"/>
          </svg>
          Editors
        </div>
        <div id="editor-bar-legend" class="osmsg-bar-legend"></div>
        <div class="osmsg-chart-list-wrap" role="img" aria-label="Editors ranked by contributors"></div>
      </div>

      <div id="hashtag-chart-card" class="osmsg-chart-card" hidden>
        <div class="osmsg-chart-title">
          <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"
               fill="none" stroke="currentColor" stroke-width="2"
               stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <line x1="4" y1="9" x2="20" y2="9"/>
            <line x1="4" y1="15" x2="20" y2="15"/>
            <line x1="10" y1="3" x2="8" y2="21"/>
            <line x1="16" y1="3" x2="14" y2="21"/>
          </svg>
          Trending
        </div>
        <div class="osmsg-hashtag-stat-row">
          <span id="hashtag-stat-total"></span>
        </div>
        <div class="osmsg-chart-list-wrap" id="hashtag-list-wrap" role="img"
          aria-label="Hashtags ranked by number of contributors"></div>
      </div>

    </div>`;

  main.appendChild(section);
}

const _PER_PAGE = 5;
let _edPage = 0, _edLen = -1;
const _edExpanded = new Set();
const editorGroup = (s) =>
  (typeof editorFamily === "function" && editorFamily(s)) || String(s || "Unknown").split(/[/\s]/)[0] || "Unknown";
let _htPage = 0, _htLen = -1;

// Skeleton rows in both cards while their data loads.
function setChartsLoading() {
  _ensureChartsSection();
  const rows = Array.from({ length: 5 }, (_, i) => `
    <div class="ht-row">
      <div class="ht-head"><span class="skeleton" style="width:${90 - i * 9}px;height:11px"></span><span class="skeleton" style="width:32px;height:11px"></span></div>
      <div class="ht-bar"><div class="skeleton" style="width:${72 - i * 12}%;height:100%;border-radius:inherit"></div></div>
    </div>`).join("");
  const ec = document.getElementById("editor-chart-card");
  const ew = ec && ec.querySelector(".osmsg-chart-list-wrap");
  const el = document.getElementById("editor-bar-legend");
  if (ec && ew) { ec.hidden = false; if (el) el.innerHTML = ""; ew.style.height = "auto"; ew.innerHTML = rows; }
  const hc = document.getElementById("hashtag-chart-card");
  const hw = document.getElementById("hashtag-list-wrap");
  const ht = document.getElementById("hashtag-stat-total");
  if (hc && hw) { hc.hidden = false; if (ht) ht.textContent = ""; hw.style.height = "auto"; hw.innerHTML = rows; }
}

// Render a paginated list (rows + a prev/next footer) into `wrap`; wires the footer buttons to onPage.
function _pagedList(wrap, items, page, rowFn, gmax, onPage) {
  const pages = Math.ceil(items.length / _PER_PAGE);
  const p = Math.min(Math.max(0, page), pages - 1);
  const start = p * _PER_PAGE;
  const slice = items.slice(start, start + _PER_PAGE);
  let html = slice.map((it) => rowFn(it, gmax)).join("");
  if (pages > 1) {
    html += `<div class="ht-pager">
      <button class="ht-pg" data-dir="-1" ${p === 0 ? "disabled" : ""} aria-label="Previous">&lsaquo;</button>
      <span class="ht-pg-info">${start + 1}–${start + slice.length} of ${items.length}</span>
      <button class="ht-pg" data-dir="1" ${p >= pages - 1 ? "disabled" : ""} aria-label="Next">&rsaquo;</button>
    </div>`;
  }
  wrap.style.height = "auto";
  wrap.innerHTML = html;
  wrap.querySelectorAll(".ht-pg").forEach((b) => {
    b.onclick = () => { if (!b.disabled) onPage(p + parseInt(b.dataset.dir, 10)); };
  });
  return p;
}

function renderEditors() {
  _ensureChartsSection();
  const card = document.getElementById("editor-chart-card");
  const legendEl = document.getElementById("editor-bar-legend");
  const wrap = card ? card.querySelector(".osmsg-chart-list-wrap") : null;
  if (!card || !legendEl || !wrap) return;

  const src = (state.editorStats && state.editorStats.all) || [];
  if (!src.length) { _edPage = 0; _edLen = -1; card.hidden = true; return; }
  card.hidden = false;

  const valueOf = (r) => r.users || 0;

  // Group editor versions by family so the default view is compact; a family expands to its versions.
  const groups = new Map();
  for (const e of src) {
    const g = editorGroup(e.editor);
    let gr = groups.get(g);
    if (!gr) { gr = { editor: g, changes: 0, users: 0, changesets: 0, versions: [] }; groups.set(g, gr); }
    gr.changes += e.changes || 0; gr.users += e.users || 0; gr.changesets += e.changesets || 0;
    gr.versions.push(e);
  }
  const all = [...groups.values()].sort((a, b) => valueOf(b) - valueOf(a));
  if (all.length !== _edLen) { _edPage = 0; _edLen = all.length; }

  const gmax = Math.max(...all.map(valueOf), 1);
  legendEl.innerHTML = `<span class="osmsg-sub">Contributors per editor</span>`;

  const subRow = (v, max) => `
    <div class="ht-row ht-sub">
      <div class="ht-head"><span class="ht-tag" title="${escapeHtml(v.editor)}">${escapeHtml(v.editor)}</span><span class="ht-val">${fmt.format(valueOf(v))}</span></div>
      <div class="ht-bar"><div class="ht-fill" style="width:${Math.max(3, (valueOf(v) / max) * 100)}%"></div></div>
    </div>`;
  const rowFn = (r, max) => {
    const expandable = r.versions.length > 1;
    const open = _edExpanded.has(r.editor);
    const subs = expandable && open
      ? r.versions.slice().sort((a, b) => valueOf(b) - valueOf(a)).map((v) => subRow(v, max)).join("")
      : "";
    const caret = expandable ? `<span class="ht-caret">${open ? "▾" : "▸"}</span>` : "";
    const count = expandable ? `<span class="ht-count">${r.versions.length}</span>` : "";
    return `
      <div class="ht-row${expandable ? " ht-expandable" : ""}"${expandable ? ` data-egroup="${escapeHtml(r.editor)}"` : ""}>
        <div class="ht-head">
          <span class="ht-tag" title="${escapeHtml(r.editor)} · ${fmt.format(r.users)} contributors · ${fmt.format(r.changes)} map changes">${caret}${escapeHtml(r.editor)}${count}</span>
          <span class="ht-val">${fmt.format(valueOf(r))}</span>
        </div>
        <div class="ht-bar"><div class="ht-fill" style="width:${Math.max(3, (valueOf(r) / max) * 100)}%"></div></div>
      </div>${subs}`;
  };
  _edPage = _pagedList(wrap, all, _edPage, rowFn, gmax, (np) => { _edPage = np; renderEditors(); });
  wrap.querySelectorAll(".ht-expandable").forEach((el) => {
    const head = el.querySelector(".ht-head");
    if (!head) return;
    head.style.cursor = "pointer";
    head.onclick = () => {
      const g = el.dataset.egroup;
      if (_edExpanded.has(g)) _edExpanded.delete(g); else _edExpanded.add(g);
      renderEditors();
    };
  });
}

function renderRelatedHashtags() {
  _ensureChartsSection();
  const card = document.getElementById("hashtag-chart-card");
  const wrapEl = document.getElementById("hashtag-list-wrap");
  const totalEl = document.getElementById("hashtag-stat-total");
  if (!card || !wrapEl) return;

  const src = (state.hashtagTrends || [])
    .filter((e) => e && e.hashtag && e.users > 0)
    .map((e) => ({ tag: "#" + String(e.hashtag).replace(/^#/, ""), users: e.users || 0 }));

  if (src.length < 2) { _htPage = 0; _htLen = -1; card.hidden = true; return; }
  card.hidden = false;

  const valueOf = (e) => e.users;
  const trends = src.slice().sort((a, b) => valueOf(b) - valueOf(a));
  if (trends.length !== _htLen) { _htPage = 0; _htLen = trends.length; }

  const gmax = Math.max(...trends.map(valueOf), 1);
  if (totalEl) totalEl.textContent = "Contributors per hashtag";
  const rowFn = (e, max) => `
    <div class="ht-row">
      <div class="ht-head">
        <button type="button" class="ht-tag ht-tag-add" data-addtag="${escapeHtml(String(e.tag).replace(/^#/, ""))}"
          title="Add ${e.tag} to the search">${e.tag}</button>
        <span class="ht-val">${fmt.format(valueOf(e))}</span>
      </div>
      <div class="ht-bar"><div class="ht-fill" style="width:${Math.max(3, (valueOf(e) / max) * 100)}%"></div></div>
    </div>`;
  _htPage = _pagedList(wrapEl, trends, _htPage, rowFn, gmax, (np) => { _htPage = np; renderRelatedHashtags(); });
  // Delegated and reassigned on each render so it survives paging; the handler lives in app.js.
  wrapEl.onclick = (ev) => {
    const el = ev.target.closest("[data-addtag]");
    if (el && typeof addRelatedHashtag === "function") addRelatedHashtag(el.dataset.addtag);
  };
}
