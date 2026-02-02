let currentPeriod = 'today';
let gauge = null;
let chart = null;
let gaugeConfig = { max: 7000, green: 300, yellow: 1000, orange: 2500 };
let chartMeta = { type: 'bar', avgKwh: 0, isWeekend: [], barUnit: 'kwh', tooltips: [] };
let priceKwh = 0.226;
let T = {};

function fmt_num(num, decimals = 2) {
    const locale = T.number_locale || 'en-US';
    return num.toLocaleString(locale, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function getBarColors(data) {
    if (!data.length) return [];
    const mn = Math.min(...data);
    const mx = Math.max(...data);
    const range = mx - mn;
    if (range === 0) return data.map(() => 'rgb(0,200,80)');
    return data.map(v => {
        const t = (v - mn) / range;
        let r, g, b;
        if (t <= 0.5) {
            const s = t / 0.5;
            r = Math.round(s * 200); g = Math.round(200 + s * 55); b = Math.round(80 - s * 80);
        } else {
            const s = (t - 0.5) / 0.5;
            r = Math.round(200 + s * 55); g = Math.round(255 - s * 155); b = 0;
        }
        return `rgb(${r},${g},${b})`;
    });
}
async function loadI18n() {
    const r = await fetch('/api/i18n');
    T = await r.json();
    document.title = T.title || 'Energy Monitor';
    document.getElementById('meterLabel').textContent = T.meter_reading;
    document.getElementById('baseloadLabel').textContent = T.baseload;
    document.getElementById('baseloadTooltip').textContent = T.baseload_tooltip;
    document.getElementById('costLabel').textContent = T.cost;
    document.getElementById('currencyUnit').textContent = T.currency;
    document.querySelectorAll('.period-btns button').forEach(btn => {
        btn.textContent = T.period[btn.dataset.period] || btn.dataset.period;
    });
    const dowEl = document.getElementById('calDow');
    dowEl.innerHTML = T.calendar.weekdays_label.map(d => `<div class="cal-dow">${d}</div>`).join('');
}

async function initGauge() {
    try {
        const r = await fetch('/api/gauge-range');
        const d = await r.json();
        gaugeConfig = { max: d.gauge_max, green: d.zone_green, yellow: d.zone_yellow, orange: d.zone_orange };
    } catch (e) {}

    const section = document.querySelector('.gauge-section');
    const isMobile = window.innerWidth <= 900;
    const isLandscape = window.innerHeight < 500 && window.innerWidth > window.innerHeight;
    let size;
    if (isLandscape) size = Math.min(window.innerHeight * 0.75, section.clientWidth * 0.9);
    else if (isMobile) size = Math.min(window.innerWidth * 0.7, 300);
    else size = Math.min(section.clientHeight * 0.65, section.clientWidth * 0.85, 500);

    const ticks = [];
    let step = gaugeConfig.max <= 2000 ? 250 : gaugeConfig.max <= 5000 ? 500 : 1000;
    for (let i = 0; i <= gaugeConfig.max; i += step) ticks.push(i.toString());

    gauge = new RadialGauge({
        renderTo: 'gauge', width: size, height: size, units: 'Watt',
        minValue: 0, maxValue: gaugeConfig.max, majorTicks: ticks, minorTicks: 5,
        highlights: [
            { from: 0, to: gaugeConfig.green, color: '#0f0' },
            { from: gaugeConfig.green, to: gaugeConfig.yellow, color: '#ff0' },
            { from: gaugeConfig.yellow, to: gaugeConfig.orange, color: '#f80' },
            { from: gaugeConfig.orange, to: gaugeConfig.max, color: '#f00' }
        ],
        colorPlate: '#111', colorMajorTicks: '#fff', colorMinorTicks: '#888',
        colorNumbers: '#fff', colorNeedle: '#f00', colorNeedleEnd: '#f00',
        valueBox: true, valueBoxStroke: 0, colorValueBoxBackground: 'transparent',
        colorValueText: '#0f0', fontValueSize: 50, valueInt: 0,
        animationDuration: 500, animationRule: 'linear',
        borderShadowWidth: 0, borders: false, value: 0
    }).draw();
}
const floatingTooltip = {
    id: 'floatingTooltip',
    afterEvent(chart, args) {
        const evt = args.event;
        const tt = document.getElementById('chartTooltip');
        if (!tt) return;
        if (evt.type === 'mouseout') { tt.style.opacity = '0'; return; }
        if (evt.type !== 'mousemove' && evt.type !== 'click' && evt.type !== 'touchmove' && evt.type !== 'touchstart') return;
        const meta = chart.getDatasetMeta(0);
        if (!meta.data.length) return;
        let nearest = 0, minDist = Infinity;
        for (let i = 0; i < meta.data.length; i++) {
            const dx = Math.abs(meta.data[i].x - evt.x);
            if (dx < minDist) { minDist = dx; nearest = i; }
        }
        const val = chart.data.datasets[0].data[nearest];
        const label = chartMeta.tooltips.length ? chartMeta.tooltips[nearest] : chart.data.labels[nearest];
        const pt = meta.data[nearest];
        const chartArea = chart.chartArea;
        if (chartMeta.barUnit === 'watt') {
            tt.innerHTML = `<strong>${label}</strong><br>${T.chart?.avg || 'Avg'} ${Math.round(val)} W`;
        } else {
            const cost = (val * priceKwh).toFixed(2);
            const we = chartMeta.isWeekend[nearest] ? ' 🏠' : '';
            tt.innerHTML = `<strong>${label}${we}</strong><br>${fmt_num(val, 2)} kWh · ${fmt_num(parseFloat(cost))} ${T.currency || '€'}`;
        }
        tt.style.opacity = '1';
        const ttW = tt.offsetWidth;
        let left = pt.x - ttW / 2;
        left = Math.max(chartArea.left, Math.min(chartArea.right - ttW, left));
        tt.style.left = left + 'px';
        tt.style.top = (chartArea.top - 6) + 'px';
        tt.style.transform = 'translateY(-100%)';
    }
};
const crosshairPlugin = {
    id: 'crosshair',
    afterEvent(chart, args) {
        chart._crosshairX = (args.event.type === 'mouseout') ? null : args.event.x;
    },
    afterDraw(chart) {
        if (!chart._crosshairX) return;
        const { ctx, chartArea: { top, bottom } } = chart;
        ctx.save();
        ctx.strokeStyle = 'rgba(255,255,255,0.3)';
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(chart._crosshairX, top);
        ctx.lineTo(chart._crosshairX, bottom);
        ctx.stroke();
        ctx.restore();
    }
};

const avgLinePlugin = {
    id: 'avgLine',
    afterDraw(chart) {
        if (!chartMeta.avgKwh) return;
        const { ctx, chartArea: { left, right }, scales: { y } } = chart;
        const yPos = y.getPixelForValue(chartMeta.avgKwh);
        ctx.save();
        ctx.strokeStyle = 'rgba(255,255,255,0.5)';
        ctx.lineWidth = 1.5;
        ctx.setLineDash([6, 4]);
        ctx.beginPath();
        ctx.moveTo(left, yPos);
        ctx.lineTo(right, yPos);
        ctx.stroke();
        ctx.fillStyle = 'rgba(255,255,255,0.7)';
        ctx.font = '10px system-ui';
        ctx.textAlign = 'right';
        const avgLabel = T.chart?.avg || 'Avg';
        ctx.fillText(`${avgLabel} ${fmt_num(chartMeta.avgKwh, chartMeta.barUnit === 'watt' ? 0 : 1)}`, right, yPos - 4);
        ctx.restore();
    }
};

function initChart() {
    const ctx = document.getElementById('history').getContext('2d');
    const isDesktop = window.innerWidth > 1000;
    const isMobile = window.innerWidth <= 500;
    const tickFontSize = isDesktop ? 14 : 10;
    const titleFontSize = isDesktop ? 14 : 11;
    const maxTicks = isMobile ? 8 : isDesktop ? 15 : 10;
    chart = new Chart(ctx, {
        type: 'bar',
        data: { labels: [], datasets: [{ label: T.chart?.consumption || 'Consumption', data: [], borderRadius: 4, borderSkipped: 'bottom' }] },
        options: {
            responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
            layout: { padding: { left: 8, right: 8 } },
            interaction: { mode: 'index', intersect: false },
            events: ['mousemove', 'mouseout', 'click', 'touchstart', 'touchmove'],
            plugins: { legend: { display: false }, tooltip: { enabled: false } },
            scales: {
                x: { ticks: { color: '#888', maxTicksLimit: maxTicks, maxRotation: 45, font: { size: tickFontSize }, autoSkip: true, autoSkipPadding: 4 } },
                y: { ticks: { color: '#888', font: { size: tickFontSize } }, title: { display: true, text: 'kWh', color: '#888', font: { size: titleFontSize } }, beginAtZero: true }
            }
        },
        plugins: [floatingTooltip, crosshairPlugin, avgLinePlugin]
    });
}

async function updateLive() {
    try {
        const r = await fetch('/api/latest');
        const d = await r.json();
        if (gauge) gauge.value = d.power_watt;
        document.getElementById('totalKwh').textContent = fmt_num(d.total_kwh, 3);
        document.getElementById('lastUpdate').textContent = (d.last_seen || '--:--:--') + (T.time_suffix || '');
        const dot = document.getElementById('onlineDot');
        const statusText = document.getElementById('statusText');
        dot.classList.toggle('online', d.online);
        statusText.textContent = d.online ? 'Online' : 'Offline';
    } catch (e) {}
}

async function updateStats() {
    try {
        const r = await fetch('/api/stats');
        const d = await r.json();
        priceKwh = d.price_kwh || priceKwh;
        document.getElementById('priceInfo').textContent = `@ ${fmt_num(d.price_kwh * 100, 1)} ${T.cent_unit || 'ct/kWh'}`;
        const progBar = document.getElementById('prognoseBar');
        progBar.innerHTML = `${T.prognosis_prefix} <strong>${fmt_num(d.prognosis_month, 0)} kWh</strong> = <strong>${fmt_num(d.prognosis_cost, 2)} ${T.currency || '€'}</strong>`;
        document.getElementById('baseload').textContent = d.baseload_watt;
    } catch (e) {}
}

async function updateChart() {
    let url = `/api/history?period=${currentPeriod}`;
    if (currentPeriod === 'custom') {
        if (!calState.start || !calState.end) return;
        url += `&start=${fmt(calState.start)}&end=${fmt(calState.end)}`;
    }
    try {
        const r = await fetch(url);
        const d = await r.json();
        chartMeta.type = 'bar';
        chartMeta.avgKwh = d.avg_kwh || 0;
        chartMeta.isWeekend = d.is_weekend || [];
        chartMeta.barUnit = d.bar_unit || 'kwh';
        chartMeta.tooltips = d.tooltips || [];
        chart.config.type = 'bar';
        chart.data.labels = d.labels;
        chart.data.datasets[0].data = d.data;
        const colors = getBarColors(d.data);
        const borders = (d.is_weekend || []).map(we => we ? 'rgba(160,120,255,0.6)' : 'transparent');
        const bw = (d.is_weekend || []).map(we => we ? 2 : 0);
        Object.assign(chart.data.datasets[0], {
            label: chartMeta.barUnit === 'watt' ? `${T.chart?.avg || 'Avg'} Watt` : 'kWh',
            backgroundColor: colors, borderColor: borders, borderWidth: bw,
            borderRadius: 4, borderSkipped: 'bottom',
            hoverBackgroundColor: colors.map(c => c.replace(')', ',0.8)').replace('rgb', 'rgba'))
        });
        chart.options.scales.y.title.text = chartMeta.barUnit === 'watt' ? 'Watt' : 'kWh';
        chart.update();
        document.getElementById('chartTooltip').style.opacity = '0';
    } catch (e) {}
}

async function updateRangeStats() {
    let url = `/api/stats-range?period=${currentPeriod}`;
    if (currentPeriod === 'custom') {
        if (!calState.start || !calState.end) return;
        url += `&start=${fmt(calState.start)}&end=${fmt(calState.end)}`;
    }
    try {
        const r = await fetch(url);
        const d = await r.json();
        document.getElementById('rangeKwh').textContent = fmt_num(d.kwh, 1);
        document.getElementById('rangeCost').textContent = fmt_num(d.cost, 2);
        const changeEl = document.getElementById('rangeChange');
        if (d.change_pct !== null && d.change_pct !== undefined) {
            const pct = d.change_pct;
            const sign = pct > 0 ? '+' : '';
            changeEl.textContent = `${sign}${pct.toFixed(0)}% vs. ${d.prev_label}`;
            changeEl.className = 'change ' + (pct > 0 ? 'positive' : pct < 0 ? 'negative' : '');
        } else {
            changeEl.textContent = '';
        }
    } catch (e) {}
}
let calState = { month: null, year: null, start: null, end: null };

function renderCal() {
    const days = document.getElementById('calDays');
    const title = document.getElementById('calTitle');
    const hint = document.getElementById('calHint');
    const y = calState.year, m = calState.month;
    const months = T.calendar?.months || [];
    title.textContent = `${months[m] || ''} ${y}`;
    hint.textContent = !calState.start ? (T.calendar?.select_start || '') : !calState.end ? (T.calendar?.select_end || '') : '';
    if (calState.start && calState.end) {
        const resetLabel = T.calendar?.reset || '↺ Reset';
        hint.innerHTML = `<span class="cal-reset">${resetLabel}</span>`;
        hint.querySelector('.cal-reset').addEventListener('click', () => {
            calState.start = null; calState.end = null; renderCal();
        });
    }
    const first = new Date(y, m, 1);
    const lastDay = new Date(y, m + 1, 0).getDate();
    let dow = first.getDay() || 7;
    const today = new Date(); today.setHours(0,0,0,0);
    let html = '';
    for (let i = 1; i < dow; i++) {
        const d = new Date(y, m, 1 - (dow - i));
        html += `<div class="cal-day other" data-date="${fmt(d)}">${d.getDate()}</div>`;
    }
    for (let d = 1; d <= lastDay; d++) {
        const dt = new Date(y, m, d);
        let cls = 'cal-day';
        if (dt > today) cls += ' future';
        if (dt.getTime() === today.getTime()) cls += ' today';
        if (calState.start && dt.getTime() === calState.start.getTime()) cls += ' start';
        if (calState.end && dt.getTime() === calState.end.getTime()) cls += ' end';
        if (calState.start && calState.end && dt > calState.start && dt < calState.end) cls += ' in-range';
        html += `<div class="${cls}" data-date="${fmt(dt)}">${d}</div>`;
    }
    const totalCells = dow - 1 + lastDay;
    const rem = totalCells % 7 ? 7 - (totalCells % 7) : 0;
    for (let i = 1; i <= rem; i++) {
        const d = new Date(y, m + 1, i);
        html += `<div class="cal-day other" data-date="${fmt(d)}">${d.getDate()}</div>`;
    }
    days.innerHTML = html;
    days.querySelectorAll('.cal-day:not(.future)').forEach(el => el.addEventListener('click', onCalDay));
}

function fmt(d) { return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; }

function onCalDay(e) {
    const dt = new Date(e.target.dataset.date + 'T00:00:00');
    if (!calState.start || calState.end) {
        calState.start = dt; calState.end = null; renderCal();
    } else {
        if (dt < calState.start) { calState.end = calState.start; calState.start = dt; }
        else if (dt.getTime() === calState.start.getTime()) { calState.end = dt; }
        else { calState.end = dt; }
        renderCal();
        setTimeout(() => {
            document.getElementById('rangeCal').classList.remove('visible');
            updateChart(); updateRangeStats();
        }, 250);
    }
}

function initCal() {
    const now = new Date();
    calState.month = now.getMonth(); calState.year = now.getFullYear();
    document.getElementById('calPrev').addEventListener('click', () => {
        calState.month--; if (calState.month < 0) { calState.month = 11; calState.year--; } renderCal();
    });
    document.getElementById('calNext').addEventListener('click', () => {
        calState.month++; if (calState.month > 11) { calState.month = 0; calState.year++; } renderCal();
    });
    renderCal();
}

function setPeriod(period) {
    const cal = document.getElementById('rangeCal');
    if (period === 'custom') {
        if (cal.classList.contains('visible')) { cal.classList.remove('visible'); return; }
        currentPeriod = 'custom';
        document.querySelectorAll('.period-btns button').forEach(b => b.classList.remove('active'));
        document.querySelector('[data-period="custom"]').classList.add('active');
        if (calState.start && calState.end) {
            calState.month = calState.start.getMonth();
            calState.year = calState.start.getFullYear();
        }
        cal.classList.add('visible');
        renderCal();
    } else {
        currentPeriod = period;
        document.querySelectorAll('.period-btns button').forEach(b => b.classList.remove('active'));
        document.querySelector(`[data-period="${period}"]`).classList.add('active');
        cal.classList.remove('visible');
        updateChart(); updateRangeStats();
    }
    document.getElementById('rangeLabel').textContent = T.period?.[currentPeriod] || currentPeriod;
}

function initEventListeners() {
    document.querySelectorAll('.period-btns button').forEach(btn => btn.addEventListener('click', () => setPeriod(btn.dataset.period)));
    initCal();
    const infoIcon = document.getElementById('baseloadInfo');
    const tooltip = document.getElementById('baseloadTooltip');
    infoIcon.addEventListener('click', e => { e.stopPropagation(); tooltip.classList.toggle('visible'); });
    document.addEventListener('click', () => tooltip.classList.remove('visible'));
}

async function init() {
    await loadI18n();
    await initGauge();
    initChart();
    initEventListeners();
    document.querySelector(`[data-period="${currentPeriod}"]`).classList.add('active');
    document.getElementById('rangeLabel').textContent = T.period?.[currentPeriod] || currentPeriod;
    await Promise.all([updateLive(), updateStats(), updateChart(), updateRangeStats()]);
    setInterval(updateLive, 5000);
    setInterval(() => { if (currentPeriod === 'today') updateChart(); }, 30000);
    setInterval(updateStats, 60000);
    setInterval(async () => {
        const r = await fetch('/api/gauge-range');
        const d = await r.json();
        if (d.gauge_max !== gaugeConfig.max) location.reload();
    }, 300000);
    let resizeTimeout;
    window.addEventListener('resize', () => { clearTimeout(resizeTimeout); resizeTimeout = setTimeout(() => location.reload(), 300); });
}

document.addEventListener('DOMContentLoaded', init);
