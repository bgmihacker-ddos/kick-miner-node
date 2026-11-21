import threading
import json
import time
import os
from flask import Flask, render_template_string, jsonify, send_file
from collections import deque
from loguru import logger
from datetime import datetime

recent_logs = deque(maxlen=250)
logger.add(lambda msg: recent_logs.append(str(msg).strip()), level="DEBUG")

SERVER_START_TIME = time.time()

data_lock = threading.Lock()
_account_manager = None

shared_context = {
    "streamers": {},
    "points": {},
    "last_update": {},
    "status": "Initializing",
    "stream_status": {}
}

app = Flask(__name__)

HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>KICKMINER</title>
<style>
:root {
    --kick-green: #53fc18;
    --kick-green-dim: rgba(83, 252, 24, 0.15);
    --kick-green-border: rgba(83, 252, 24, 0.3);
    --bg-dark: #0b0e11;
    --bg-card: #191c21;
    --bg-card-hover: #1e2228;
    --bg-header: #12151a;
    --border-color: #2a2e35;
    --text-primary: #efeff1;
    --text-secondary: #adadb8;
    --text-muted: #7a7a85;
    --red: #f04747;
    --orange: #faa61a;
    --blue: #5865f2;
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
    font-family: 'Inter', 'Segoe UI', -apple-system, sans-serif;
    background: var(--bg-dark);
    color: var(--text-primary);
    min-height: 100vh;
}

.container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 20px;
}

.header {
    text-align: center;
    padding: 32px 0 24px;
}

.header h1 {
    font-size: 36px;
    font-weight: 800;
    letter-spacing: 2px;
}

.header .kick-text {
    color: var(--kick-green);
}

.header .subtitle {
    color: var(--text-muted);
    font-size: 13px;
    margin-top: 4px;
    letter-spacing: 1px;
    text-transform: uppercase;
}

.summary-bar {
    display: flex;
    justify-content: center;
    gap: 32px;
    padding: 16px 24px;
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    margin-bottom: 24px;
    flex-wrap: wrap;
}

.summary-item {
    text-align: center;
}

.summary-item .value {
    font-size: 24px;
    font-weight: 700;
    color: var(--kick-green);
}

.summary-item .label {
    font-size: 11px;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-top: 2px;
}

.status-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 600;
}

.status-active {
    background: var(--kick-green-dim);
    color: var(--kick-green);
    border: 1px solid var(--kick-green-border);
}

.status-init {
    background: rgba(250, 166, 26, 0.15);
    color: var(--orange);
    border: 1px solid rgba(250, 166, 26, 0.3);
}

.status-dot-pulse {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: currentColor;
    animation: pulse 2s infinite;
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
}

.account-section {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    margin-bottom: 20px;
    overflow: hidden;
    transition: border-color 0.2s;
}

.account-section:hover {
    border-color: #3a3e45;
}

.account-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px 20px;
    background: var(--bg-header);
    border-bottom: 1px solid var(--border-color);
    flex-wrap: wrap;
    gap: 12px;
}

.account-name {
    font-size: 18px;
    font-weight: 700;
    display: flex;
    align-items: center;
    gap: 8px;
}

.account-name .alias {
    color: var(--kick-green);
}

.account-meta {
    display: flex;
    gap: 20px;
    font-size: 13px;
    color: var(--text-secondary);
    flex-wrap: wrap;
}

.account-meta .meta-item {
    display: flex;
    align-items: center;
    gap: 4px;
}

.streamer-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
    gap: 12px;
    padding: 16px;
}

.streamer-card {
    background: var(--bg-dark);
    border: 1px solid var(--border-color);
    border-radius: 10px;
    padding: 14px 16px;
    position: relative;
    transition: all 0.2s;
    overflow: hidden;
}

.streamer-card::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 3px;
    background: var(--border-color);
    transition: background 0.2s;
}

.streamer-card.watching::before { background: var(--kick-green); }
.streamer-card.online::before { background: var(--orange); }
.streamer-card.offline::before { background: #444; }

.streamer-card:hover {
    background: var(--bg-card-hover);
    border-color: #3a3e45;
}

.card-row-top {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
}

.streamer-info-left {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
}

.s-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
}

.s-dot.watching {
    background: var(--kick-green);
    box-shadow: 0 0 8px rgba(83, 252, 24, 0.5);
    animation: pulse 2s infinite;
}

.s-dot.online { background: var(--orange); }
.s-dot.offline { background: #555; }

.s-name {
    font-weight: 600;
    font-size: 14px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.s-name a {
    color: var(--text-primary);
    text-decoration: none;
    transition: color 0.2s;
}

.s-name a:hover {
    color: var(--kick-green);
}

.s-priority {
    font-size: 10px;
    background: #2a2e35;
    color: var(--text-muted);
    padding: 2px 6px;
    border-radius: 4px;
    font-weight: 600;
    flex-shrink: 0;
}

.s-errors {
    font-size: 10px;
    background: rgba(240, 71, 71, 0.15);
    color: var(--red);
    padding: 2px 6px;
    border-radius: 4px;
    flex-shrink: 0;
}

.card-row-bottom {
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.s-points {
    font-size: 18px;
    font-weight: 700;
    color: var(--kick-green);
}

.s-time {
    font-size: 11px;
    color: var(--text-muted);
}

.watch-btn {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 4px 10px;
    background: transparent;
    border: 1px solid var(--border-color);
    border-radius: 6px;
    color: var(--text-secondary);
    text-decoration: none;
    font-size: 11px;
    font-weight: 600;
    transition: all 0.2s;
    flex-shrink: 0;
}

.watch-btn:hover {
    border-color: var(--kick-green);
    color: var(--kick-green);
    background: var(--kick-green-dim);
}

.remove-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 4px 8px;
    background: transparent;
    border: 1px solid var(--border-color);
    border-radius: 6px;
    color: var(--text-muted);
    font-size: 11px;
    cursor: pointer;
    transition: all 0.2s;
    flex-shrink: 0;
}

.remove-btn:hover {
    border-color: var(--red);
    color: var(--red);
    background: rgba(240, 71, 71, 0.15);
}

.watch-btn svg {
    width: 12px;
    height: 12px;
}

.s-status-label {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.s-status-label.watching { color: var(--kick-green); }
.s-status-label.online { color: var(--orange); }
.s-status-label.offline { color: var(--text-muted); }

.grand-total {
    text-align: center;
    padding: 24px;
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    margin: 24px 0;
}

.grand-total .label {
    color: var(--text-muted);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.grand-total .value {
    font-size: 40px;
    font-weight: 800;
    color: var(--kick-green);
    margin-top: 4px;
}

.footer {
    text-align: center;
    padding: 16px 0 24px;
    color: var(--text-muted);
    font-size: 12px;
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 8px;
}

.refresh-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--kick-green);
    animation: pulse 2s infinite;
}

.empty-state {
    text-align: center;
    padding: 60px 20px;
    color: var(--text-muted);
}

.empty-state .icon { font-size: 48px; margin-bottom: 12px; }
.empty-state .text { font-size: 16px; }

@media (max-width: 600px) {
    .header h1 { font-size: 28px; }
    .summary-bar { gap: 16px; padding: 12px 16px; }
    .summary-item .value { font-size: 18px; }
    .streamer-grid { grid-template-columns: 1fr; }
    .account-header { padding: 12px 16px; }
    .account-meta { gap: 12px; }
}
</style>
</head>
<body>
<div class="container">

    <div class="header">
        <h1><span class="kick-text">KICK</span>MINER</h1>
        <div class="subtitle">Channel Points Farmer</div>
    </div>

    <div class="summary-bar" id="summary-bar">
        <div class="summary-item">
            <div class="value" id="sum-status">—</div>
            <div class="label">Status</div>
        </div>
        <div class="summary-item">
            <div class="value" id="sum-accounts">0</div>
            <div class="label">Accounts</div>
        </div>
        <div class="summary-item">
            <div class="value" id="sum-watching">0</div>
            <div class="label">Watching</div>
        </div>
        <div class="summary-item">
            <div class="value" id="sum-online">0</div>
            <div class="label">Online</div>
        </div>
        <div class="summary-item">
            <div class="value" id="sum-total">0</div>
            <div class="label">Total Points</div>
        </div>
    </div>

    <div id="accounts-container">
        <div class="empty-state">
            <div class="icon">⏳</div>
            <div class="text">Connecting to streamers...</div>
        </div>
    </div>

    <div id="bets-container" style="margin-top: 24px; background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px; flex-wrap:wrap; gap:12px;">
            <div style="display:flex; align-items:center; gap:8px;">
                <span style="font-size:18px;">🎯</span>
                <span style="font-size:16px; font-weight:700; color:var(--text-primary);">Prediction Performance & Recent Bets</span>
            </div>
            <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap; font-size:12px;">
                <span>Total Bets: <b id="stat-total-bets" style="color:var(--text-primary);">0</b></span>
                <span>Win Rate: <b id="stat-win-rate" style="color:var(--kick-green);">0%</b></span>
                <span>Net PnL: <b id="stat-net-pnl" style="color:var(--kick-green);">+0 pts</b></span>
                <a href="/download/bets" style="display:inline-flex; align-items:center; gap:4px; padding:4px 10px; background:var(--kick-green-dim); color:var(--kick-green); border:1px solid var(--kick-green-border); border-radius:6px; text-decoration:none; font-weight:600; font-size:11px;">📥 Bets CSV</a>
                <a href="/download/learning" style="display:inline-flex; align-items:center; gap:4px; padding:4px 10px; background:rgba(88,101,242,0.15); color:var(--blue); border:1px solid rgba(88,101,242,0.3); border-radius:6px; text-decoration:none; font-weight:600; font-size:11px;">🧠 Learning JSON</a>
            </div>
        </div>

        <div id="live-bets-box" style="margin-bottom:16px; background:rgba(255,165,0,0.06); border:1px solid rgba(255,165,0,0.25); border-radius:8px; padding:12px;">
            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:8px;">
                <div style="display:flex; align-items:center; gap:8px; font-weight:700; color:var(--orange); font-size:13px;">
                    <span style="display:inline-block; width:8px; height:8px; background:var(--orange); border-radius:50%;"></span>
                    <span>🔴 LIVE IN-PLAY BETS (Awaiting Match Result)</span>
                </div>
                <span id="live-bets-count" style="background:rgba(255,165,0,0.2); color:var(--orange); font-size:11px; font-weight:bold; padding:2px 8px; border-radius:10px;">0 Active</span>
            </div>
            <div id="live-bets-grid" style="display:grid; grid-template-columns:repeat(auto-fill, minmax(280px, 1fr)); gap:10px;">
                <div style="color:var(--text-muted); font-size:12px; padding:4px 0;">No live bets in play right now. Waiting for streamers to open predictions...</div>
            </div>
        </div>

        <div style="overflow-x:auto;">
            <table style="width:100%; border-collapse:collapse; font-size:12px; text-align:left;">
                <thead>
                    <tr style="border-bottom:1px solid var(--border-color); color:var(--text-muted); font-size:11px; text-transform:uppercase;">
                        <th style="padding:8px;">Time</th>
                        <th style="padding:8px;">Account</th>
                        <th style="padding:8px;">Streamer</th>
                        <th style="padding:8px;">Prediction</th>
                        <th style="padding:8px;">Pick</th>
                        <th style="padding:8px;">Odds</th>
                        <th style="padding:8px;">Bet</th>
                        <th style="padding:8px;">Result</th>
                        <th style="padding:8px; text-align:right;">Profit/Loss</th>
                    </tr>
                </thead>
                <tbody id="bets-table-body">
                    <tr><td colspan="9" style="text-align:center; padding:16px; color:var(--text-muted);">No bets recorded yet. Waiting for predictions...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <div class="grand-total">
        <div class="label">Grand Total Points</div>
        <div class="value" id="grand-total">0</div>
    </div>

    <div class="footer">
        <span class="refresh-dot"></span>
        <span>Auto-refresh every 5s · Last update: <span id="refresh-time">--:--:--</span></span>
    </div>

</div>

<script>
const EXTERNAL_LINK_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>';

function fmtTime(v) {
    if (!v || v === "N/A" || v === "None" || v === "null") return "—";
    try {
        if (v.includes("T")) return new Date(v).toLocaleTimeString();
        return v;
    } catch(e) { return v; }
}

function fmtNumber(n) {
    return (n || 0).toLocaleString();
}

function renderAccounts(data) {
    try {
        const container = document.getElementById("accounts-container");
        const accounts = data.accounts || [];

    if (!accounts.length) {
        renderLegacy(data);
        return;
    }

    let html = "";
    let grandTotal = 0;
    let totalWatching = 0;
    let totalOnline = 0;

    accounts.forEach((acc, idx) => {
        const alias = acc.alias || ("Account " + (idx+1));
        const proxy = acc.proxy;
        const active = acc.active_count || 0;
        const limit = acc.max_concurrent || 0;
        const streamers = acc.streamers || {};
        const order = acc.streamer_order || Object.keys(streamers);

        let accTotal = 0;
        let accWatching = 0;
        let accOnline = 0;
        let cards = "";

        order.forEach(name => {
            const info = streamers[name] || {};
            const pts = info.points || 0;
            accTotal += pts;

            let cardClass = "offline";
            let dotClass = "offline";
            let statusText = "OFFLINE";

            if (info.watching) {
                cardClass = "watching";
                dotClass = "watching";
                statusText = "WATCHING";
                accWatching++;
            } else if (info.online) {
                cardClass = "online";
                dotClass = "online";
                statusText = "QUEUED";
                accOnline++;
            }

            const errHtml = info.errors > 0
                ? '<span class="s-errors">⚠ ' + info.errors + '</span>'
                : "";

            const lastUp = fmtTime(info.last_update);
            const pri = (info.priority !== undefined && info.priority !== null) ? (info.priority + 1) : "";
            const target = info.target || 100000;
            const pct = Math.min(100, Math.round(((pts || 0) / target) * 1000) / 10);
            let streakBadge = '';
            if (info.consecutive_wins > 0) {
                streakBadge = '<span style="background:rgba(83,252,24,0.15); color:var(--kick-green); padding:1px 5px; border-radius:3px; font-size:10px; font-weight:bold; margin-left:4px;">🔥' + info.consecutive_wins + 'W</span>';
            } else if (info.consecutive_losses > 0) {
                streakBadge = '<span style="background:rgba(235,87,87,0.15); color:#eb5757; padding:1px 5px; border-radius:3px; font-size:10px; font-weight:bold; margin-left:4px;">❄️' + info.consecutive_losses + 'L</span>';
            }

            const progressHtml = '<div style="margin-top:8px; background:#1e2329; border-radius:4px; height:5px; overflow:hidden;">' +
                '<div style="background:var(--kick-green); height:100%; width:' + pct + '%; transition: width 0.3s ease;"></div>' +
            '</div>' +
            '<div style="display:flex; justify-content:space-between; font-size:10px; color:#848e9c; margin-top:3px;">' +
                '<span>Goal: ' + fmtNumber(target) + ' pts</span>' +
                '<span style="color:var(--kick-green); font-weight:bold;">' + pct + '%</span>' +
            '</div>';

            cards += '<div class="streamer-card ' + cardClass + '">' +
                '<div class="card-row-top">' +
                    '<div class="streamer-info-left">' +
                        '<span class="s-dot ' + dotClass + '"></span>' +
                        '<span class="s-name"><a href="https://kick.com/' + name + '" target="_blank">' + name + '</a></span>' +
                        (pri !== "" ? '<span class="s-priority">#' + pri + '</span>' : "") +
                        errHtml +
                    '</div>' +
                    '<div style="display:flex; gap:6px; align-items:center;">' +
                        '<a href="https://kick.com/' + name + '" target="_blank" class="watch-btn">' +
                            EXTERNAL_LINK_SVG + ' Watch' +
                        '</a>' +
                        '<button class="remove-btn" title="Remove streamer from all accounts" onclick="removeStreamer(\'' + name + '\')">🗑️</button>' +
                    '</div>' +
                '</div>' +
                '<div class="card-row-bottom">' +
                    '<div>' +
                        '<span class="s-points">' + fmtNumber(pts) + ' pts</span>' +
                        streakBadge +
                    '</div>' +
                    '<div>' +
                        '<span class="s-status-label ' + cardClass + '">' + statusText + '</span>' +
                        ' <span class="s-time">' + lastUp + '</span>' +
                    '</div>' +
                '</div>' +
                progressHtml +
            '</div>';
        });

        grandTotal += accTotal;
        totalWatching += accWatching;
        totalOnline += accOnline;

        const proxyHtml = proxy ? "🔒 Proxy" : "🌐 Direct";

        html += '<div class="account-section">' +
            '<div class="account-header">' +
                '<div class="account-name">' +
                    '<span class="alias">' + alias + '</span>' +
                '</div>' +
                '<div class="account-meta">' +
                    '<span class="meta-item">' + proxyHtml + '</span>' +
                    '<span class="meta-item">👁 ' + active + '/' + limit + '</span>' +
                    '<span class="meta-item" style="color:var(--kick-green)">💰 ' + fmtNumber(accTotal) + '</span>' +
                '</div>' +
            '</div>' +
            '<div class="streamer-grid">' + cards + '</div>' +
        '</div>';
    });

    container.innerHTML = html;
    document.getElementById("grand-total").textContent = fmtNumber(grandTotal);

    document.getElementById("sum-accounts").textContent = accounts.length;
    document.getElementById("sum-watching").textContent = totalWatching;
    document.getElementById("sum-online").textContent = totalOnline + totalWatching;
    document.getElementById("sum-total").textContent = fmtNumber(grandTotal);

    const st = data.status || "Active";
    const statusEl = document.getElementById("sum-status");
    if (st === "Active") {
        statusEl.innerHTML = '<span class="status-badge status-active"><span class="status-dot-pulse"></span>Active</span>';
    } else {
        statusEl.innerHTML = '<span class="status-badge status-init">' + st + '</span>';
    }
    } catch (e) {
        console.error("renderAccounts error:", e);
    }
}

function renderLegacy(data) {
    const container = document.getElementById("accounts-container");
    const streamers = data.streamers || {};
    const points = data.points || {};
    const names = Object.keys(streamers);

    if (!names.length) {
        container.innerHTML = '<div class="empty-state"><div class="icon">📡</div><div class="text">No streamers configured</div></div>';
        return;
    }

    let html = '<div class="streamer-grid">';
    let total = 0;

    names.forEach(name => {
        const pts = points[name] || 0;
        total += pts;
        const status = (data.stream_status || {})[name] || "offline";
        const cardClass = status === "online" ? "watching" : "offline";
        const dotClass = cardClass;
        const lastUp = (data.last_update || {})[name] || "—";

        html += '<div class="streamer-card ' + cardClass + '">' +
            '<div class="card-row-top">' +
                '<div class="streamer-info-left">' +
                    '<span class="s-dot ' + dotClass + '"></span>' +
                    '<span class="s-name"><a href="https://kick.com/' + name + '" target="_blank">' + name + '</a></span>' +
                '</div>' +
                '<a href="https://kick.com/' + name + '" target="_blank" class="watch-btn">' +
                    EXTERNAL_LINK_SVG + ' Watch</a>' +
            '</div>' +
            '<div class="card-row-bottom">' +
                '<span class="s-points">' + fmtNumber(pts) + ' pts</span>' +
                '<span class="s-time">' + lastUp + '</span>' +
            '</div>' +
        '</div>';
    });

    html += '</div>';
    container.innerHTML = html;
    document.getElementById("grand-total").textContent = fmtNumber(total);
    document.getElementById("sum-total").textContent = fmtNumber(total);
    document.getElementById("sum-watching").textContent =
        names.filter(n => (data.stream_status||{})[n] === "online").length;
}

function refresh() {
    fetch("/api/data")
        .then(r => r.json())
        .then(data => {
            renderAccounts(data);
            document.getElementById("refresh-time").textContent =
                new Date().toLocaleTimeString();
        })
        .catch(err => console.error("Refresh error:", err));
}

function refreshBets() {
    fetch("/api/bets")
        .then(r => r.json())
        .then(data => {
            document.getElementById("stat-total-bets").textContent = data.total_bets || 0;
            document.getElementById("stat-win-rate").textContent = (data.win_rate || 0) + "%";
            const net = data.net_profit || 0;
            const netEl = document.getElementById("stat-net-pnl");
            netEl.textContent = (net >= 0 ? "+" : "") + fmtNumber(net) + " pts";
            netEl.style.color = net >= 0 ? "var(--kick-green)" : "var(--red)";

            // Render Live / In-Play Bets
            const liveGrid = document.getElementById("live-bets-grid");
            const liveCount = document.getElementById("live-bets-count");
            const liveList = data.live_bets || [];
            if (liveList.length > 0) {
                liveCount.textContent = liveList.length + " Active";
                liveCount.style.background = "rgba(255,165,0,0.3)";
                let lHtml = "";
                liveList.forEach(lb => {
                    lHtml += '<div style="background:#16191f; border:1px solid rgba(255,165,0,0.4); border-radius:8px; padding:10px;">' +
                        '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">' +
                            '<span style="font-weight:700; color:var(--text-primary); font-size:12px;">[' + lb.account + '] <span style="color:var(--kick-green);">' + lb.streamer + '</span></span>' +
                            '<span style="background:rgba(255,165,0,0.2); color:var(--orange); font-size:10px; font-weight:bold; padding:2px 6px; border-radius:4px;">⏳ MATCH IN PLAY</span>' +
                        '</div>' +
                        '<div style="font-size:11px; color:#cbd5e1; margin-bottom:6px; font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="' + (lb.prediction||'') + '">' + (lb.prediction || '—') + '</div>' +
                        '<div style="display:flex; justify-content:space-between; font-size:11px; color:var(--text-muted);">' +
                            '<span>Pick: <b style="color:var(--kick-green);">' + lb.outcome + '</b> (' + lb.odds + ')</span>' +
                            '<span>Bet: <b style="color:var(--text-primary);">' + fmtNumber(lb.amount) + ' pts</b></span>' +
                        '</div>' +
                    '</div>';
                });
                liveGrid.innerHTML = lHtml;
            } else {
                liveCount.textContent = "0 Active";
                liveCount.style.background = "rgba(255,165,0,0.12)";
                liveGrid.innerHTML = '<div style="color:var(--text-muted); font-size:12px; padding:4px 0;">No live bets in play right now. Waiting for streamers to open predictions...</div>';
            }

            const tbody = document.getElementById("bets-table-body");
            const bets = data.recent_bets || [];
            if (!bets.length && !liveList.length) {
                tbody.innerHTML = '<tr><td colspan="9" style="text-align:center; padding:16px; color:var(--text-muted);">No bets recorded yet. Waiting for predictions...</td></tr>';
                return;
            }
            let rows = "";
            // Prepend in-flight live bets directly into table
            liveList.forEach(lb => {
                rows += '<tr style="border-bottom:1px solid #1e2228; background:rgba(255,165,0,0.04);">' +
                    '<td style="padding:8px; color:var(--orange); font-weight:600;">' + (lb.timestamp || "Live") + '</td>' +
                    '<td style="padding:8px; font-weight:600; color:var(--text-primary);">' + (lb.account || "—") + '</td>' +
                    '<td style="padding:8px; color:var(--kick-green);">' + (lb.streamer || "—") + '</td>' +
                    '<td style="padding:8px; max-width:180px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="' + (lb.prediction||'') + '">' + (lb.prediction || "—") + '</td>' +
                    '<td style="padding:8px; font-weight:bold; color:var(--kick-green);">' + (lb.outcome || "—") + '</td>' +
                    '<td style="padding:8px; color:var(--orange);">' + (lb.odds || "—") + '</td>' +
                    '<td style="padding:8px; font-weight:bold;">' + fmtNumber(lb.amount || 0) + '</td>' +
                    '<td style="padding:8px;"><span style="background:rgba(255,165,0,0.2); color:var(--orange); padding:2px 6px; border-radius:4px; font-weight:bold; font-size:10px;">⏳ IN PLAY</span></td>' +
                    '<td style="padding:8px; text-align:right; font-weight:bold; color:var(--orange);">Awaiting result</td>' +
                '</tr>';
            });
            bets.forEach(b => {
                const isWin = (b.Result || "").toUpperCase() === "WIN";
                const badge = isWin
                    ? '<span style="background:rgba(83,252,24,0.15); color:var(--kick-green); padding:2px 6px; border-radius:4px; font-weight:bold;">WIN</span>'
                    : '<span style="background:rgba(240,71,71,0.15); color:var(--red); padding:2px 6px; border-radius:4px; font-weight:bold;">LOSS</span>';
                const pnl = parseInt(b.NetProfit || 0);
                const pnlColor = pnl >= 0 ? "var(--kick-green)" : "var(--red)";
                const pnlText = (pnl >= 0 ? "+" : "") + fmtNumber(pnl) + " pts";

                rows += '<tr style="border-bottom:1px solid #1e2228;">' +
                    '<td style="padding:8px; color:var(--text-muted);">' + (b.Timestamp || "—") + '</td>' +
                    '<td style="padding:8px; font-weight:600; color:var(--text-primary);">' + (b.Account || "—") + '</td>' +
                    '<td style="padding:8px; color:var(--kick-green);">' + (b.Streamer || "—") + '</td>' +
                    '<td style="padding:8px; max-width:180px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">' + (b.Prediction || "—") + '</td>' +
                    '<td style="padding:8px;">' + (b.Outcome || "—") + '</td>' +
                    '<td style="padding:8px; color:var(--orange);">' + (b.Odds || "—") + '</td>' +
                    '<td style="padding:8px;">' + fmtNumber(parseInt(b.BetAmount || 0)) + '</td>' +
                    '<td style="padding:8px;">' + badge + '</td>' +
                    '<td style="padding:8px; text-align:right; font-weight:bold; color:' + pnlColor + ';">' + pnlText + '</td>' +
                '</tr>';
            });
            tbody.innerHTML = rows;
        })
        .catch(err => console.error("Bets refresh error:", err));
}

function removeStreamer(name) {
    if (!name) return;
    if (!confirm('Are you sure you want to remove streamer "' + name + '" from all accounts?')) return;
    fetch('/api/streamer/remove', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ streamer: name })
    })
    .then(r => r.json())
    .then(data => {
        if (data.success) {
            refresh();
        } else {
            alert('Failed to remove streamer: ' + (data.message || 'Unknown error'));
        }
    })
    .catch(err => {
        alert('Network error removing streamer: ' + err);
    });
}

refresh();
refreshBets();
setInterval(refresh, 5000);
setInterval(refreshBets, 5000);
</script>
</body>
</html>
"""


@app.route('/')
def dashboard():
    return render_template_string(HTML_TEMPLATE)


@app.route('/api/data')
def get_data():
    global _account_manager

    if _account_manager is not None:
        try:
            accounts_status = _account_manager.get_all_status()

            flat_streamers = {}
            flat_points = {}
            flat_last_update = {}
            flat_stream_status = {}

            for acc in accounts_status:
                alias = acc.get("alias", "")
                for name, info in acc.get("streamers", {}).items():
                    if name not in flat_streamers:
                        flat_streamers[name] = alias
                    flat_points[name] = info.get("points", 0)
                    lu = info.get("last_update")
                    flat_last_update[name] = lu if lu else "N/A"

                    if info.get("watching"):
                        flat_stream_status[name] = "online"
                    elif info.get("online"):
                        flat_stream_status[name] = "online"
                    else:
                        flat_stream_status[name] = "offline"

            return jsonify({
                "streamers": flat_streamers,
                "points": flat_points,
                "last_update": flat_last_update,
                "status": "Active",
                "stream_status": flat_stream_status,
                "accounts": accounts_status,
            })

        except Exception as e:
            logger.error(f"API /api/data error: {e}")
            return jsonify({
                "streamers": {}, "points": {},
                "last_update": {}, "status": "Error",
                "stream_status": {}, "accounts": [],
            })
    else:
        with data_lock:
            clean_lu = {}
            for k, v in shared_context["last_update"].items():
                clean_lu[k] = (
                    v.strftime("%H:%M:%S")
                    if isinstance(v, datetime) else str(v)
                )
            return jsonify({
                "streamers": shared_context["streamers"],
                "points": shared_context["points"],
                "last_update": clean_lu,
                "status": shared_context["status"],
                "stream_status": shared_context["stream_status"],
                "accounts": [],
            })


@app.route('/api/accounts')
def get_accounts():
    global _account_manager
    if _account_manager is not None:
        try:
            return jsonify(_account_manager.get_all_status())
        except Exception as e:
            logger.error(f"API /api/accounts error: {e}")
    return jsonify([])


@app.route('/health')
@app.route('/healthz')
def health():
    global _account_manager
    live_streamers = []
    total_streamers = 0

    if _account_manager is not None:
        try:
            status_list = _account_manager.get_all_status()
            for acc in status_list:
                for s_name, s_info in acc.get("streamers", {}).items():
                    total_streamers += 1
                    if s_info.get("online") or s_info.get("watching"):
                        live_streamers.append(s_name)
        except Exception as e:
            logger.error(f"Health check status error: {e}")

    return jsonify({
        "status": "healthy",
        "service": "Kick Channel Points Miner",
        "uptime_seconds": int(time.time() - SERVER_START_TIME),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "live_streamers_count": len(live_streamers),
        "live_streamers": live_streamers,
        "total_streamers": total_streamers,
        "any_streamer_live": len(live_streamers) > 0
    }), 200


@app.route('/api/live')
def get_live():
    global _account_manager
    if _account_manager is not None:
        try:
            status_list = _account_manager.get_all_status()
            live = []
            offline = []
            for acc in status_list:
                alias = acc.get("alias", "")
                for s_name, s_info in acc.get("streamers", {}).items():
                    is_live = bool(s_info.get("online") or s_info.get("watching"))
                    if is_live:
                        live.append({
                            "name": s_name,
                            "account": alias,
                            "stream_id": s_info.get("stream_id"),
                            "points": s_info.get("points", 0)
                        })
                    else:
                        offline.append(s_name)

            live_names = {x["name"] for x in live}
            return jsonify({
                "any_live": len(live) > 0,
                "live_count": len(live),
                "live_streamers": live,
                "offline_streamers": list(set(offline) - live_names),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            })
        except Exception as e:
            logger.error(f"API /api/live error: {e}")
            return jsonify({"error": str(e)}), 500
    return jsonify({"any_live": False, "live_count": 0, "live_streamers": [], "offline_streamers": []})


@app.route('/api/bets')
def get_bets():
    global _account_manager
    live_bets = []
    if _account_manager is not None:
        try:
            live_bets = _account_manager.get_live_pending_bets()
        except Exception as e:
            logger.debug(f"API /api/bets live_bets error: {e}")

    csv_file = "bets_history.csv"
    if not os.path.exists(csv_file):
        return jsonify({
            "total_bets": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0,
            "net_profit": 0,
            "live_bets": live_bets,
            "recent_bets": []
        })
    try:
        import csv
        rows = []
        wins = 0
        losses = 0
        net_profit = 0
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append(r)
                res = str(r.get("Result", "")).strip().upper()
                if res == "WIN":
                    wins += 1
                elif res == "LOSS":
                    losses += 1
                try:
                    net_profit += int(r.get("NetProfit", 0))
                except Exception:
                    pass

        total_bets = wins + losses
        win_rate = round((wins / total_bets * 100), 1) if total_bets > 0 else 0
        recent_bets = rows[-30:][::-1]
        return jsonify({
            "total_bets": total_bets,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "net_profit": net_profit,
            "live_bets": live_bets,
            "recent_bets": recent_bets
        })
    except Exception as e:
        logger.error(f"API /api/bets error: {e}")
        return jsonify({"error": str(e), "live_bets": live_bets, "recent_bets": []}), 500


@app.route('/download/bets')
def download_bets():
    csv_file = "bets_history.csv"
    if os.path.exists(csv_file):
        return send_file(csv_file, as_attachment=True, download_name="bets_history.csv")
    return "No bets history recorded yet.", 404


@app.route('/download/learning')
def download_learning():
    learn_file = "learning_profile.json"
    if os.path.exists(learn_file):
        return send_file(learn_file, as_attachment=True, download_name="learning_profile.json")
    return "No learning profile recorded yet.", 404


@app.route('/api/learning')
def get_learning():
    try:
        from utils.adaptive_learning import AdaptiveLearningEngine
        engine = AdaptiveLearningEngine()
        return jsonify(engine.profiles)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/debug/logs')
def get_debug_logs():
    return jsonify(list(recent_logs))


@app.route('/api/streamer/remove', methods=['POST'])
def remove_streamer_api():
    global _account_manager
    if not _account_manager:
        return jsonify({"success": False, "message": "AccountManager not active"}), 503

    try:
        req = request.get_json(silent=True) or {}
        streamer_name = req.get("streamer", "").strip()
        if not streamer_name:
            return jsonify({"success": False, "message": "Streamer name required"}), 400

        import asyncio
        loop = _account_manager.loop if hasattr(_account_manager, "loop") and _account_manager.loop else None

        if loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_account_manager.remove_streamer(streamer_name), loop)
            success, msg = future.result(timeout=15)
        else:
            success, msg = asyncio.run(_account_manager.remove_streamer(streamer_name))

        if success:
            with data_lock:
                shared_context["streamers"].pop(streamer_name, None)
                shared_context["points"].pop(streamer_name, None)
                shared_context["last_update"].pop(streamer_name, None)
                shared_context["stream_status"].pop(streamer_name, None)

        return jsonify({"success": success, "message": msg})
    except Exception as e:
        logger.error(f"API /api/streamer/remove error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/debug/rebalance_now')
def trigger_rebalance_now():
    global _account_manager
    if not _account_manager:
        return jsonify({"error": "No account manager active"}), 500
    results = []
    import asyncio
    for w in _account_manager.workers:
        online = [n for n, s in w.state.streamers.items() if s.is_online]
        watching = [n for n, s in w.state.streamers.items() if s.is_watching]
        results.append({
            "alias": w.alias,
            "running": w._running,
            "online": online,
            "watching": watching,
            "max_concurrent": w.max_concurrent
        })
        try:
            asyncio.create_task(w._rebalance())
        except Exception as e:
            results[-1]["rebalance_err"] = str(e)
    return jsonify(results)


@app.route('/api/bet', methods=['POST'])
def handle_master_bet():
    """Allows Master Panel to command an account on this worker to place a bet."""
    from flask import request
    data = request.get_json(silent=True) or {}
    alias = data.get("alias")
    streamer = data.get("streamer")
    outcome_id = data.get("outcome_id")
    amount = data.get("amount", 0)

    if not alias or not streamer or not outcome_id or amount <= 0:
        return jsonify({"success": False, "error": "Missing required fields"}), 400

    if not _account_manager:
        return jsonify({"success": False, "error": "Account manager not initialized"}), 500

    worker = next((w for w in _account_manager.workers if w.alias == alias), None)
    if not worker:
        return jsonify({"success": False, "error": f"Account '{alias}' not found on this worker"}), 404

    try:
        from utils.kick_prediction import KickPrediction
        pred_client = KickPrediction()
        ok, res = pred_client.vote_prediction(streamer, outcome_id, amount, worker.token)
        if ok:
            st = worker.state.streamers.get(streamer)
            if st and st.points:
                st.points = max(0, st.points - amount)
            logger.success(f"[{alias}] 🎯 Master Bet: Voted {amount:,} pts on {streamer} outcome {outcome_id}")
            return jsonify({"success": True, "result": res})
        else:
            return jsonify({"success": False, "error": str(res)}), 400
    except Exception as e:
        logger.error(f"[{alias}] Error voting: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


_server_started = False
_server_port = None


def start_server(streamers_data, port=5000):
    global _account_manager, _server_started, _server_port

    port = int(os.environ.get("PORT", port))

    if hasattr(streamers_data, 'get_all_status'):
        _account_manager = streamers_data
        with data_lock:
            names = _account_manager.get_all_streamers_flat()
            shared_context["streamers"] = {s: "" for s in names}
            shared_context["points"] = {s: 0 for s in names}
            shared_context["last_update"] = {s: "N/A" for s in names}
            shared_context["stream_status"] = {s: "offline" for s in names}
            shared_context["status"] = "Active"
    elif isinstance(streamers_data, list):
        with data_lock:
            shared_context["streamers"] = {s: "" for s in streamers_data}
            shared_context["points"] = {s: 0 for s in streamers_data}
            shared_context["last_update"] = {s: "N/A" for s in streamers_data}
            shared_context["stream_status"] = {s: "offline" for s in streamers_data}
            shared_context["status"] = "Active"
    elif isinstance(streamers_data, dict):
        with data_lock:
            shared_context["streamers"] = streamers_data
            shared_context["status"] = "Active"

    if _server_started:
        logger.info(f"🌍 Web Dashboard server already active on port {_server_port or port}")
        return

    _server_started = True
    _server_port = port

    def run():
        import logging
        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        logger.info(f"🌍 Web Dashboard available at http://0.0.0.0:{port}")
        try:
            app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
        except Exception as e:
            logger.error(f"🌍 Web Dashboard server exited: {e}")

    threading.Thread(target=run, daemon=True, name="WebDashboardServer").start()


def update_streamer_info(name, points, last_update_time,
                         stream_id=None, account_alias=None):
    with data_lock:
        shared_context["points"][name] = points
        shared_context["last_update"][name] = last_update_time
        if account_alias:
            shared_context["streamers"][name] = account_alias
        shared_context["stream_status"][name] = (
            "online" if stream_id else "offline"
        )