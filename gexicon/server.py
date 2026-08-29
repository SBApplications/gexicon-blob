"""A local one-page browser UI for the blob.

    python3 -m gexicon --serve

Serves 127.0.0.1 only. The page shows the current blob in a select-all box with a
copy button, a per-symbol summary, and a button that re-fetches CBOE and rebuilds.
Everything is inlined -- no CDN, no network access from the page itself, so it
works with the laptop offline against saved payloads.

Replay is a mode of this page rather than a second page: the same blob box, the same
copy button, the same summary table, fed from the snapshot archive instead of from
CBOE. One surface that can show either is worth more than two that each show one,
and a replayed result is the same `RunResult` shape a live one is, so the rendering
does not fork either.
"""

import json
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .archive import DEFAULT_ARCHIVE_DIR
from .cboe import MAX_QUOTE_AGE_HOURS
from .nytime import NY, now_utc
from .pipeline import run
from .replay import ReplayError, archive_index, find_snapshot, replay
from .symbols import DEFAULT_SYMBOLS

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Gexicon data blob</title>
<style>
  :root {
    --bg:#0a0b0d; --panel:#121418; --panel-2:#171a1f; --line:#23272e;
    --ink:#e8eaed; --dim:#8b929c; --accent:#4da3ff; --good:#3fb950; --bad:#f85149;
  }
  * { box-sizing:border-box; }
  body {
    margin:0; background:var(--bg); color:var(--ink);
    font:14px/1.5 ui-sans-serif,-apple-system,"SF Pro Text",Segoe UI,sans-serif;
    padding:28px 22px 60px; max-width:1120px; margin-inline:auto;
  }
  header { display:flex; align-items:baseline; gap:14px; flex-wrap:wrap; margin-bottom:4px; }
  h1 { font-size:17px; font-weight:600; margin:0; letter-spacing:-.01em; }
  .meta { color:var(--dim); font-size:12.5px; }
  .card {
    background:var(--panel); border:1px solid var(--line); border-radius:12px;
    padding:16px; margin-top:18px;
  }
  .row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  button {
    font:inherit; font-weight:550; color:var(--ink); background:var(--panel-2);
    border:1px solid var(--line); border-radius:8px; padding:8px 14px; cursor:pointer;
  }
  button:hover:not(:disabled) { border-color:#38404a; background:#1d2128; }
  button:disabled { opacity:.5; cursor:default; }
  button.primary { background:var(--accent); border-color:var(--accent); color:#04122a; }
  button.primary:hover:not(:disabled) { background:#68b2ff; border-color:#68b2ff; }
  textarea {
    width:100%; min-height:150px; margin-top:12px; resize:vertical;
    background:#0d0f12; color:var(--ink); border:1px solid var(--line);
    border-radius:9px; padding:12px;
    font:12.5px/1.6 ui-monospace,SFMono-Regular,Menlo,monospace;
    white-space:pre-wrap; word-break:break-all;
  }
  table { width:100%; border-collapse:collapse; margin-top:6px; font-size:13px; }
  th { text-align:right; color:var(--dim); font-weight:500; padding:7px 10px;
       border-bottom:1px solid var(--line); font-size:12px; }
  th:first-child, td:first-child { text-align:left; }
  td { text-align:right; padding:7px 10px; border-bottom:1px solid #1a1d23;
       font-variant-numeric:tabular-nums; }
  tbody tr:last-child td { border-bottom:none; }
  .sym { font-weight:600; }
  .pos { color:var(--good); } .neg { color:var(--bad); }
  .none { color:var(--dim); }
  .banner { border-radius:9px; padding:11px 13px; margin-top:14px; font-size:13px; }
  .banner.err { background:#2a1113; border:1px solid #5c2225; color:#ffb4ae; }
  .banner.warn { background:#2a2011; border:1px solid #5c4822; color:#ffd9a0; }
  .banner.replay { background:#101c2c; border:1px solid #2b5a8f; color:#cfe4ff; }
  .banner.replay b { font-size:13.5px; letter-spacing:.03em; text-transform:uppercase; }
  .banner.replay .say { margin-top:6px; color:#9fc3ea; }
  .banner b { display:block; margin-bottom:5px; }
  .banner code { font-family:ui-monospace,Menlo,monospace; font-size:12px; }
  .hint { color:var(--dim); font-size:12.5px; margin-top:10px; }
  .dot { display:inline-block; width:7px; height:7px; border-radius:50%;
         background:var(--good); margin-right:7px; vertical-align:middle; }
  .dot.stale { background:#d29922; }
  .spin { display:inline-block; width:12px; height:12px; margin-right:8px;
          border:2px solid #3a4048; border-top-color:var(--accent);
          border-radius:50%; animation:spin .7s linear infinite; vertical-align:-2px; }
  @keyframes spin { to { transform:rotate(360deg); } }
  .seg { display:inline-flex; border:1px solid var(--line); border-radius:8px;
         overflow:hidden; }
  .seg button { border:0; border-radius:0; background:transparent; color:var(--dim);
                padding:5px 13px; font-size:12.5px; }
  .seg button.on { background:var(--panel-2); color:var(--ink); }
  .seg button + button { border-left:1px solid var(--line); }
  .field { display:inline-flex; align-items:center; gap:7px; color:var(--dim);
           font-size:12.5px; }
  select {
    font:inherit; font-size:13px; color:var(--ink); background:var(--panel-2);
    border:1px solid var(--line); border-radius:8px; padding:7px 9px;
  }
  .checks { display:flex; flex-wrap:wrap; gap:6px 14px; margin-top:12px; }
  .checks label { display:inline-flex; align-items:center; gap:6px; font-size:13px;
                  font-variant-numeric:tabular-nums; }
  .checks input { accent-color:var(--accent); }
  body.replay #refresh { display:none; }
</style>
</head>
<body>
<header>
  <h1>Gexicon data blob</h1>
  <span class="seg">
    <button id="mode-live" class="on">Live</button>
    <button id="mode-replay">Replay</button>
  </span>
  <span class="meta" id="status"><span class="spin"></span>loading</span>
</header>
<div class="meta" id="submeta"></div>

<div class="card" id="replay-panel" hidden>
  <div class="row">
    <label class="field">Session <select id="rp-date"></select></label>
    <label class="field">Snapshot <select id="rp-stamp"></select></label>
    <span class="meta" id="rp-note"></span>
  </div>
  <div class="checks" id="rp-tickers"></div>
  <div class="hint">Paste into the indicator's data input with TradingView bar replay
    positioned on that session.</div>
</div>

<div id="replay-banner"></div>

<div class="card">
  <div class="row">
    <button class="primary" id="copy">Copy blob</button>
    <button id="refresh">Fetch fresh data</button>
    <span class="meta" id="len"></span>
  </div>
  <textarea id="blob" readonly spellcheck="false"></textarea>
  <div class="hint">Paste into the Gexicon indicator's data input. If it says
    "Unrecognised GEX format" the blob is wrong and the indicator draws nothing --
    that is the designed behaviour, not a chart problem.</div>
</div>

<div id="problems"></div>

<div class="card">
  <table>
    <thead><tr>
      <th>Symbol</th><th>Spot</th><th>Flip</th><th>Net GEX</th>
      <th>Contracts</th><th>Expired dropped</th><th>Buckets</th><th>Top level</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
  <div class="hint">Open interest is a prior-night snapshot and is static intraday.
    These are not live positioning.</div>
</div>

<script>
const $ = (id) => document.getElementById(id);
let busy = false;
let mode = "live";
let archive = null;      // the archive index, fetched once when Replay is opened
let rebuildTimer = null;

function money(x) {
  const s = (x >= 0 ? "+" : "") + x.toFixed(2) + "B";
  return '<span class="' + (x >= 0 ? "pos" : "neg") + '">' + s + "</span>";
}

function esc(s) { return String(s).replace(/</g, "&lt;"); }

function render(d) {
  $("blob").value = d.blob || "";
  $("len").textContent = d.blob ? d.blob.length + " chars" : "";

  if (d.replay) {
    $("status").innerHTML = '<span class="dot"></span>' +
      (d.blob ? "replay " + d.snapshot_time + " New York" : "no blob");
    $("submeta").textContent = d.blob
      ? "session " + d.session_date + " New York  ·  snapshot " + d.snapshot_stamp +
        "  ·  header stamp " + d.effective_at +
        (d.stamp_inferred ? " (inferred)" : " (recorded)") +
        "  ·  CBOE quotes " + d.quote_range +
        "  ·  " + (d.symbols || []).length + " symbol(s) in the blob"
      : "";
  } else {
    const stale = d.age_seconds > (d.cache_max_age || 600);
    $("status").innerHTML =
      '<span class="dot' + (stale ? " stale" : "") + '"></span>' +
      (d.blob ? "built " + d.age_text : "no blob");
    $("submeta").textContent = d.blob
      ? "session " + d.session_date + " New York  ·  header stamp " + d.effective_at +
        " (when spot was true)  ·  computed " + d.computed_at +
        "  ·  CBOE quotes " + d.quote_range + " (" + d.quote_lag_text + ")" +
        "  ·  " + d.archived_total + " snapshots archived"
      : "";
  }

  $("rows").innerHTML = (d.symbols || []).map(s => `
    <tr>
      <td class="sym">${s.ticker}</td>
      <td>${s.spot.toFixed(2)}</td>
      <td>${s.flip ? s.flip.toFixed(2) : '<span class="none">none</span>'}</td>
      <td>${money(s.net)}</td>
      <td>${s.contracts.toLocaleString()}</td>
      <td>${d.replay ? '<span class="none">-</span>' : s.expired_dropped}</td>
      <td>${s.buckets || '<span class="none">none</span>'}</td>
      <td>${s.top || '<span class="none">-</span>'}</td>
    </tr>`).join("");

  // The replay banner sits directly above the blob box, because the one mistake
  // that matters here is copying archived levels while believing they are live.
  $("replay-banner").innerHTML = (d.replay && d.blob)
    ? '<div class="banner replay"><b>Replay &mdash; archived data, not live</b>' +
      'Session <b style="display:inline">' + esc(d.session_date) + '</b>, snapshot ' +
      esc(d.snapshot_time) + ' New York (CBOE quotes ' + esc(d.quote_range) + ').' +
      ' Open interest and spot are as they stood then. Put TradingView bar replay on ' +
      'that session before pasting, or the levels will not line up with the bars.' +
      (d.stamp_inferred
        ? '<div class="say">Header stamp <code>' + esc(d.effective_at) +
          '</code> is inferred, not recorded: this snapshot predates the archived ' +
          'spot timestamp, so the stamp is the CBOE publish time less the usual ' +
          'delay. On a futures chart the basis is anchored on that stamp, so it can ' +
          'sit a couple of minutes off and slide every level with it.</div>'
        : '') +
      '</div>'
    : "";

  let html = "";
  if (d.failures && d.failures.length) {
    html += '<div class="banner err"><b>' + d.failures.length +
      (d.replay ? ' symbol(s) are missing from this snapshot'
                : ' symbol(s) failed and are missing from the blob') + '</b>' +
      d.failures.map(f => '<div><code>' + f.ticker + '</code> &mdash; ' +
        esc(f.reason) + '</div>').join("") + '</div>';
  }
  if (d.error) {
    html += '<div class="banner err"><b>Run failed</b><code>' +
      d.error.replace(/</g, "&lt;") + '</code></div>';
  }
  // In replay the inferred stamp is already stated in the replay banner, in the
  // terms that matter there. Saying it twice trains the eye to skip both.
  if (!d.replay && d.spot_time_fallbacks && d.spot_time_fallbacks.length) {
    html += '<div class="banner warn"><b>Header timestamp inferred for ' +
      d.spot_time_fallbacks.length + ' symbol(s)</b>' +
      // Double-quoted on purpose. PAGE is an ordinary Python string, so a
      // backslash-escaped apostrophe collapses to a bare one on the way out and
      // ends the JS string early -- which is a syntax error in the whole inline
      // script, not just this banner: the page then loads, renders nothing, and
      // sits on "loading" forever with no clue as to why.
      "CBOE's <code>last_trade_time</code> could not be trusted, so the stamp is " +
      "the file's publish time less the usual delay. On a futures chart the basis " +
      'is anchored on that stamp, so the levels may sit slightly off.' +
      d.spot_time_fallbacks.map(f => '<div><code>' + f.ticker + '</code> &mdash; ' +
        f.reason.replace(/</g, "&lt;") + '</div>').join("") + '</div>';
  }
  if (d.quote_lag_warn) {
    html += '<div class="banner warn"><b>CBOE quotes are ' + d.quote_lag_text +
      '</b>The file itself has stopped updating, so spot is behind the market. ' +
      'The levels are still valid (open interest is a prior-night snapshot), but ' +
      'the flip and the magnitudes are priced off a stale spot.</div>';
  }
  if (d.blob && d.offline) {
    html += '<div class="banner warn"><b>Offline mode</b>' +
      'Built from saved payloads in <code>' + d.offline + '</code>, not live CBOE data.</div>';
  }
  $("problems").innerHTML = html;
}

async function fetchJSON(url, working) {
  if (busy) return null;
  busy = true;
  $("refresh").disabled = true;
  $("copy").disabled = true;
  $("status").innerHTML = '<span class="spin"></span>' + working;
  try {
    const r = await fetch(url);
    return await r.json();
  } catch (e) {
    $("status").textContent = "request failed";
    $("problems").innerHTML =
      '<div class="banner err"><b>Could not reach the local server</b>' +
      '<code>' + esc(e) + '</code></div>';
    return null;
  } finally {
    busy = false;
    $("refresh").disabled = false;
    $("copy").disabled = false;
  }
}

async function load(fresh) {
  const d = await fetchJSON("/api/blob" + (fresh ? "?fresh=1" : ""),
                            fresh ? "fetching CBOE chains" : "loading");
  if (d) render(d);
}

// --- replay ---------------------------------------------------------------

function selectedTickers() {
  return Array.from(document.querySelectorAll("#rp-tickers input:checked"))
              .map(el => el.value);
}

function currentSnapshots() {
  const day = (archive && archive.dates || []).find(
    d => d.session_date === $("rp-date").value);
  return day ? day.snapshots : [];
}

function currentSnapshot() {
  return currentSnapshots().find(s => s.stamp === $("rp-stamp").value) || null;
}

function fillStamps() {
  const snaps = currentSnapshots();
  // Earliest first, and the earliest is selected: that is the book the session
  // actually opened on, which is what a morning replay wants.
  $("rp-stamp").innerHTML = snaps.map(
    s => '<option value="' + s.stamp + '">' + esc(s.label) + '</option>').join("");
  if (snaps.length) $("rp-stamp").value = snaps[0].stamp;
  fillTickers();
}

function fillTickers() {
  const snap = currentSnapshot();
  const list = snap ? snap.tickers : [];
  // Every ticker in the snapshot, checked. The blob carries all of them and the
  // indicator picks its own record, so the boxes are for trimming a line down,
  // never a choice that has to be made.
  $("rp-tickers").innerHTML = list.map(
    t => '<label><input type="checkbox" value="' + t + '" checked>' + t +
         '</label>').join("");
  $("rp-tickers").querySelectorAll("input").forEach(
    el => el.addEventListener("change", queueReplay));
  $("rp-note").textContent = snap
    ? snap.tickers.length + " symbol(s) archived at this snapshot"
    : "";
}

function queueReplay() {
  clearTimeout(rebuildTimer);
  rebuildTimer = setTimeout(loadReplay, 350);   // one rebuild per burst of clicks
}

async function loadReplay() {
  const snap = currentSnapshot();
  if (!snap) {
    render({replay: true, blob: null, symbols: [], failures: [],
            error: "the archive holds no snapshot for that session"});
    return;
  }
  const picked = selectedTickers();
  if (!picked.length) {
    render({replay: true, blob: null, symbols: [], failures: [],
            error: "no symbols selected -- tick at least one"});
    return;
  }
  const q = "?date=" + encodeURIComponent($("rp-date").value) +
            "&stamp=" + encodeURIComponent(snap.stamp) +
            "&tickers=" + encodeURIComponent(picked.join(","));
  const d = await fetchJSON("/api/replay" + q, "rebuilding from the archive");
  if (d) render(d);
}

async function openReplay() {
  if (!archive) {
    const d = await fetchJSON("/api/archive", "reading the archive");
    if (!d) return;
    archive = d;
  }
  const dates = archive.dates || [];
  if (!dates.length) {
    render({replay: true, blob: null, symbols: [], failures: [],
            error: "no snapshots in " + archive.archive_dir +
                   " -- the archive cannot be backfilled, CBOE overwrites the file"});
    return;
  }
  // Newest session first: the most recent archived day is the likely one.
  $("rp-date").innerHTML = dates.slice().reverse().map(
    d => '<option value="' + d.session_date + '">' + d.session_date +
         '</option>').join("");
  fillStamps();
  loadReplay();
}

function setMode(next) {
  mode = next;
  $("mode-live").classList.toggle("on", mode === "live");
  $("mode-replay").classList.toggle("on", mode === "replay");
  document.body.classList.toggle("replay", mode === "replay");
  $("replay-panel").hidden = mode !== "replay";
  $("problems").innerHTML = "";
  $("replay-banner").innerHTML = "";
  if (mode === "replay") openReplay(); else load(false);
}

$("mode-live").onclick = () => { if (mode !== "live") setMode("live"); };
$("mode-replay").onclick = () => { if (mode !== "replay") setMode("replay"); };
$("rp-date").onchange = () => { fillStamps(); loadReplay(); };
$("rp-stamp").onchange = () => { fillTickers(); loadReplay(); };

$("copy").onclick = async () => {
  const text = $("blob").value;
  if (!text) return;
  try {
    await navigator.clipboard.writeText(text);
  } catch (e) {
    $("blob").select();
    document.execCommand("copy");
  }
  const b = $("copy");
  b.textContent = "Copied";
  setTimeout(() => { b.textContent = "Copy blob"; }, 1400);
};

$("refresh").onclick = () => load(true);
addEventListener("keydown", e => {
  if (e.key === "r" && (e.metaKey || e.ctrlKey)) {
    e.preventDefault();
    // In replay there is nothing to fetch; the same key rebuilds from the archive.
    if (mode === "replay") loadReplay(); else load(true);
  }
});
load(false);
</script>
</body>
</html>
"""


# How long a built blob may be served to a page load before it is rebuilt. Short
# enough that a reload never hands over yesterday's spot, long enough that opening
# the page twice in a row does not pull 60 MB twice. The button always forces.
CACHE_MAX_AGE_SECONDS = 600

# CBOE rebuilds these files continuously, so the quote stamp should sit a minute or
# two behind the wall clock. Much further and the feed itself has gone quiet, which
# is worth saying on the page rather than failing outright.
QUOTE_LAG_WARN_SECONDS = 20 * 60


class _State(object):
    """Last result, cached briefly so a page reload does not re-hammer the endpoint."""

    def __init__(self, options, max_cache_age=CACHE_MAX_AGE_SECONDS,
                 replay_dir=None):
        self.options = options
        self.max_cache_age = max_cache_age
        # Where Replay mode reads from. Separate from options["archive_dir"], which
        # is where the live path *writes*: --no-archive turns the writing off, and
        # the days already on disk are still worth replaying.
        self.replay_dir = replay_dir or options.get("archive_dir") \
            or DEFAULT_ARCHIVE_DIR
        self.lock = threading.Lock()
        self.result = None
        self.built_at = None
        self.error = None
        self.archived_total = 0

    def payload(self):
        result, built_at = self.result, self.built_at
        data = {"blob": None, "symbols": [], "failures": [], "error": self.error,
                "offline": self.options.get("offline_dir"),
                "archived_total": self.archived_total}
        if result is None or result.blob is None:
            if result is not None:
                data["failures"] = [{"ticker": t, "reason": r} for t, r in result.failures]
            return data

        age = (now_utc() - built_at).total_seconds()
        data.update({
            "blob": result.blob,
            "session_date": result.session_date.isoformat(),
            "computed_at": result.computed_at.astimezone(NY).strftime(
                "%H:%M:%S New York"),
            # The blob's own header field: when spot was true, not when we ran.
            "effective_at": result.effective_at.astimezone(NY).strftime(
                "%H:%M:%S New York"),
            "spot_time_fallbacks": [{"ticker": t, "reason": r}
                                    for t, r in result.warnings],
            "age_seconds": age,
            "age_text": _age_text(age),
            "quote_range": _quote_range(result),
            "failures": [{"ticker": t, "reason": r} for t, r in result.failures],
            "cache_max_age": self.max_cache_age,
        })

        # How far behind the wall clock CBOE's own file stamps are. This is the
        # check that catches a feed that has quietly stopped updating -- the
        # computation stamp would still look current.
        lag = max((now_utc() - c.quote_ts).total_seconds() for c in result.chains)
        data["quote_lag_seconds"] = lag
        data["quote_lag_text"] = _age_text(lag)
        data["quote_lag_warn"] = lag > QUOTE_LAG_WARN_SECONDS
        data["symbols"] = _symbol_rows(result)
        return data

    def is_stale(self):
        if self.result is None or self.built_at is None:
            return True
        return (now_utc() - self.built_at).total_seconds() >= self.max_cache_age

    def ensure(self, fresh):
        """Rebuild if asked to, or if what we hold has aged out.

        The offline path is exempt: there is nothing newer to fetch, and rebuilding
        from the same saved files on a timer would only churn.
        """
        with self.lock:
            if not fresh and not self.is_stale():
                return
            if not fresh and self.options.get("offline_dir") and self.result is not None:
                return
            self.error = None
            try:
                self.result = run(**self.options)
                if self.result.archived:
                    self.archived_total += len(self.result.archived)
                self.built_at = now_utc()
            except Exception as exc:  # a broken run must show, not 500 silently
                self.error = "%s: %s" % (type(exc).__name__, exc)


def _symbol_rows(result):
    """The per-symbol summary table. One builder, so live and replay cannot drift."""
    rows = []
    for record, chain in zip(result.records, result.chains):
        total = record.total
        top = total.levels[0] if total.levels else None
        rows.append({
            "ticker": record.ticker,
            "spot": total.spot,
            "flip": total.flip,
            "net": total.net,
            "contracts": len(chain.contracts),
            # Meaningless on a replay -- the archive stores what survived the
            # expired-contract filter, not what it removed -- and the page shows a
            # dash there rather than a zero that would read as "none were dropped".
            "expired_dropped": chain.dropped_expired,
            "buckets": ",".join(s.tag for s in record.buckets),
            "top": ("%s%g %+.2fB" % (top.right, top.price, top.magnitude))
                   if top else "",
        })
    return rows


def archive_payload(archive_dir):
    """What the archive holds: dates, each date's snapshots, each snapshot's tickers.

    Filenames only -- this never opens a snapshot, so the page can repopulate its
    selects instantly.
    """
    days = archive_index(archive_dir)
    return {
        "archive_dir": archive_dir,
        "dates": [{
            "session_date": day.session_date.isoformat(),
            "snapshots": [{
                "stamp": snapshot.stamp,
                "label": snapshot.label,
                "time_ny": snapshot.time_ny,
                "quote_range": snapshot.quote_range_ny,
                "tickers": snapshot.tickers,
            } for snapshot in day.snapshots],
        } for day in days],
    }


def replay_payload(archive_dir, session_date=None, stamp=None, tickers=None):
    """One replayed snapshot, in the same shape `_State.payload` returns.

    `replay: true` is the only field the page branches on. Everything else it
    already knows how to draw, because a replayed RunResult is a RunResult.
    """
    data = {"replay": True, "blob": None, "symbols": [], "failures": [],
            "error": None}
    try:
        # Resolved once, then replayed by its exact id, so the page is told which
        # snapshot it actually got rather than which one it asked for.
        snapshot = find_snapshot(archive_dir, session_date=session_date, stamp=stamp)
        result = replay(session_date=snapshot.session_date, stamp=snapshot.stamp,
                        tickers=tickers, archive_dir=archive_dir)
    except ReplayError as exc:
        # A replay that cannot find its data says so. It never falls back to live
        # data, and it never emits a partial blob.
        data["error"] = str(exc)
        return data

    data["failures"] = [{"ticker": t, "reason": r} for t, r in result.failures]
    data["session_date"] = (result.session_date or snapshot.session_date).isoformat()
    data["snapshot_stamp"] = snapshot.stamp
    data["snapshot_time"] = snapshot.time_ny
    data["quote_range"] = snapshot.quote_range_ny
    data["archive_tickers"] = snapshot.tickers
    if result.blob is None:
        data["error"] = ("nothing usable in the %s snapshot of %s"
                         % (snapshot.stamp, snapshot.session_date))
        return data

    data.update({
        "blob": result.blob,
        "effective_at": result.effective_at.astimezone(NY).strftime(
            "%H:%M:%S New York"),
        # Inferred rather than read means the futures basis anchor is a couple of
        # minutes out. The banner says so; this is the flag it says it from.
        "stamp_inferred": bool(result.warnings),
        "spot_time_fallbacks": [{"ticker": t, "reason": r}
                                for t, r in result.warnings],
        "symbols": _symbol_rows(result),
    })
    return data


def _age_text(seconds):
    if seconds < 90:
        return "just now"
    minutes = int(seconds // 60)
    if minutes < 60:
        return "%d min ago" % minutes
    return "%.1f h ago" % (seconds / 3600.0)


def _quote_range(result):
    stamps = [c.quote_ts.astimezone(NY).strftime("%H:%M") for c in result.chains]
    if not stamps:
        return "-"
    lo, hi = min(stamps), max(stamps)
    return lo if lo == hi else "%s-%s" % (lo, hi)


def make_handler(state):

    class Handler(BaseHTTPRequestHandler):
        server_version = "gexicon"

        def log_message(self, fmt, *args):
            pass  # the terminal is for the blob, not access logs

        def _send(self, code, body, content_type):
            raw = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/":
                self._send(200, PAGE, "text/html; charset=utf-8")
            elif parsed.path == "/api/blob":
                fresh = parse_qs(parsed.query).get("fresh", ["0"])[0] == "1"
                state.ensure(fresh)
                self._send(200, json.dumps(state.payload()),
                           "application/json; charset=utf-8")
            elif parsed.path == "/api/archive":
                self._send(200, json.dumps(archive_payload(state.replay_dir)),
                           "application/json; charset=utf-8")
            elif parsed.path == "/api/replay":
                query = parse_qs(parsed.query)
                raw = query.get("tickers", [""])[0]
                # No `tickers` at all means every symbol in the snapshot. An empty
                # value would mean the same thing, so the page never sends one --
                # it refuses to build with nothing ticked instead.
                tickers = [t for t in raw.split(",") if t.strip()] or None
                self._send(200, json.dumps(replay_payload(
                    state.replay_dir,
                    session_date=query.get("date", [None])[0],
                    stamp=query.get("stamp", [None])[0],
                    tickers=tickers)),
                    "application/json; charset=utf-8")
            elif parsed.path == "/blob.txt":
                state.ensure(False)
                blob = state.result.blob if state.result else ""
                self._send(200, (blob or "") + "\n", "text/plain; charset=utf-8")
            else:
                self._send(404, "not found", "text/plain; charset=utf-8")

    return Handler


def serve(host="127.0.0.1", port=8765, open_browser=True,
          symbols=DEFAULT_SYMBOLS, offline_dir=None,
          archive_dir=DEFAULT_ARCHIVE_DIR, max_age_hours=MAX_QUOTE_AGE_HOURS,
          timeout=60, prefetch=True, replay_dir=None):
    state = _State({"symbols": list(symbols), "offline_dir": offline_dir,
                    "archive_dir": archive_dir, "max_age_hours": max_age_hours,
                    "timeout": timeout}, replay_dir=replay_dir)
    httpd = ThreadingHTTPServer((host, port), make_handler(state))
    url = "http://%s:%d/" % (host, port)

    if prefetch:
        threading.Thread(target=state.ensure, args=(False,), daemon=True).start()
    if open_browser:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()

    print("Gexicon UI on %s  (ctrl-c to stop)" % url)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        httpd.server_close()
    return 0
