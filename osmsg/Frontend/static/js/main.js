const _fpInstances = {};

document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.date-input[id^="daterange"]').forEach(el => {
        _fpInstances[el.id] = flatpickr(el, {
            mode: 'range',
            dateFormat: 'd-m-Y',
        });
    });
});

function setIntervalRangeForId(inputId, type) {
    const fp = _fpInstances[inputId];
    if (!fp) return;
    const end = new Date();
    const start = new Date();
    if (type === 'daily') start.setDate(end.getDate() - 1);
    else if (type === 'weekly') start.setDate(end.getDate() - 7);
    else if (type === 'monthly') start.setMonth(end.getMonth() - 1);
    fp.setDate([start, end], true);
}


function setIntervalRange(type) {
    const id = document.getElementById('daterange') ? 'daterange' : 'daterange-stats';
    setIntervalRangeForId(id, type);
}

function downloadCSV() {
    const table = document.getElementById('leaderboardTable');
    if (!table) return;

    const rows = [];
    for (const row of table.rows) {
        const cols = [];
        for (const cell of row.querySelectorAll('td, th')) {
            let text = cell.innerText
                .replace(/workspace_premium|add_task/g, '')
                .replace(/\s+/g, ' ')
                .trim();
            cols.push('"' + text.replace(/"/g, '""') + '"');
        }
        rows.push(cols.join(','));
    }

    const blob = new Blob([rows.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'osmsg_leaderboard.csv';
    link.click();
    URL.revokeObjectURL(url);
}

const TAGS = [
  "OzonGeo",
  "tt_event",
  "AfricaMapCup2026",
  "maproulette",
  "msf",
  "missingmaps",
  "Aweil",
  "MapComplete",
  "homtom",
  "osm-tr",
  "hotosm-project-49902",
  "GeoTETZ",
  "OSMTanzania",
  "hotosm-project-49935",
  "youthmappersoau",
  "hotosm-project-49638",
  "osmzwawakening",
  "syria-remapping-2025",
  "Kaart",
  "osmzimbabwe"
];

const input = document.getElementById("hashtag-input");
const suggestionsBox = document.getElementById("tag-suggestions");
const chipBox = document.getElementById("tag-chip-box");
const hiddenBox = document.getElementById("hashtag-hiddens");

let tags = [];

function renderTags() {
  chipBox.innerHTML = "";
  hiddenBox.innerHTML = "";

  tags.forEach((tag, i) => {
    const chip = document.createElement("span");
    chip.className = "tag-chip";
    chip.innerHTML = `
      ${tag}
      <span class="tag-remove" data-index="${i}">×</span>
    `;

    chipBox.appendChild(chip);

    const hidden = document.createElement("input");
    hidden.type = "hidden";
    hidden.name = "hashtags";
    hidden.value = tag;

    hiddenBox.appendChild(hidden);
  });
}

function showSuggestions(value) {
  suggestionsBox.innerHTML = "";

  if (!value) return;

  const filtered = TAGS.filter(
    t =>
      t.toLowerCase().includes(value.toLowerCase()) &&
      !tags.includes(t)
  );

  filtered.forEach(tag => {
    const item = document.createElement("div");
    item.className = "suggestion-item";
    item.textContent = tag;

    item.onclick = () => {
      tags.push(tag);
      renderTags();

      input.value = "";
      suggestionsBox.innerHTML = "";
    };

    suggestionsBox.appendChild(item);
  });
}

input.addEventListener("input", (e) => {
  showSuggestions(e.target.value);
});


input.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    e.preventDefault();

    const value = input.value.trim();

    if (value && TAGS.includes(value) && !tags.includes(value)) {
      tags.push(value);
      renderTags();
    }

    input.value = "";
    suggestionsBox.innerHTML = "";
  }
});

chipBox.addEventListener("click", (e) => {
  if (e.target.classList.contains("tag-remove")) {
    const index = e.target.dataset.index;
    tags.splice(index, 1);
    renderTags();
  }
});