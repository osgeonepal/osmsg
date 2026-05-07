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

function filterHashtags(query, dropdownId) {
    const ddId = dropdownId || 'hashtag-dropdown';
    const q = query.toLowerCase();
    const dd = document.getElementById(ddId);
    if (!dd) return;
    dd.querySelectorAll('.hashtag-option').forEach(item => {
        const text = item.querySelector('.hashtag-option-text')?.textContent.toLowerCase() || '';
        const label = item.querySelector('.hashtag-option-label')?.textContent.toLowerCase() || '';
        item.style.display = (text.includes(q) || label.includes(q)) ? 'flex' : 'none';
    });
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