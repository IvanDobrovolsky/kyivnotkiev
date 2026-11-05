"""Monthly Reddit submissions-dump sweep: torrent/webseed -> zstd -> rg -> word-bounded filter.

PullPush went paid in 2026-08 and Arctic Shift's free API cannot keyword-search
all subreddits, so every Reddit month from 2025-05 on comes from the public
monthly dumps (Arctic Shift download_links.md -> Academic Torrents). Each month
is one RS_YYYY-MM.zst (17.8-23.8 GB for 2025-05..2026-09), streamed:

    aria2c --select-file=<RS>  (or curl from the torrent's HTTP webseed)
      -> size + torrent piece-hash check, zstd frame walk over the RS head
      -> zstd -dc --long=31 | rg --no-config -a -i -F -f <first words of the 94 terms>
      -> python -m pipeline.ingestion.reddit_dump_filter (word-bounded, the decision)
      -> RS_YYYY-MM_matches.jsonl + RS_YYYY-MM_matches.meta.json
      -> delete the .zst

Submissions only: the 2010-2025 corpus is submissions-only and Ivan decided
against comments (2026-10-15). The torrent's comments file is never selected.

    python -m pipeline.ingestion.reddit_dump_sweep plan --months 2026-01..2026-09
    python -m pipeline.ingestion.reddit_dump_sweep run  --months 2026-01..2026-09
    python -m pipeline.ingestion.reddit_dump_sweep gate --months 2025-05..2025-12 \\
        --legacy-rev f4a2f92d

Every pair defined in pairs.yaml is searched, enabled or not (see
reddit_dump_filter). The sidecar records the pair set the month was filtered with
(slugs, terms, one sha1). That is the guard zaporizhzhia needed: the 2025 sweep
ran 2026-09-05/06 over enabled pairs only, zaporizhzhia was defined but disabled
(enabled 2026-09-11), so it has no Reddit rows after 2025-05-13 and the merge had
no way to know.

Resumable per month. A month is done when its jsonl and sidecar agree (sha256,
record count, pair set). Output files from an older pair set are moved to
archive/<pairset or "nometa">/ before a re-sweep, never overwritten, because
`gate` compares against them, and the merge refuses a re-swept month until
`gate` has passed it. Each sidecar is also copied to
data/audit/reddit_dump_sidecars/, because data/raw is on one disk only.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request

from pipeline.ingestion.reddit_dump_filter import (
    MATCHER, SCOPE, compile_matcher, enabled_pairs, pair_set, prefilter_tokens, swept_pairs, text_of)

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
BF = ROOT / "data" / "raw" / "reddit" / "backfill"
DUMPS = ROOT / "data" / "raw" / "reddit" / "dumps"
LOG = ROOT / "data" / "audit" / "reddit_dump_sweep.log"
GATE_REPORT = ROOT / "data" / "audit" / "reddit_resweep_gate.json"
LINKS_URL = "https://raw.githubusercontent.com/ArthurHeitmann/arctic_shift/master/download_links.md"
TORRENT_URL = "https://academictorrents.com/download/{}.torrent"
UA = "kyivnotkiev-research/1.0 (reddit dump sweep)"

# A month whose match count falls below this fraction of the reference median
# is not published without --allow-low. The 2025-05..12 outputs held 6,825-8,457
# records a month, 0.90-1.12x their median, so half is far outside normal
# variation and a broken prefilter or a truncated dump lands well under it.
LOW_RATIO = 0.5

_LOG_PATH = LOG
_LOG_WARNED = False


# ── logging ──────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    global _LOG_WARNED
    line = f"{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  {msg}"
    print(line, flush=True)
    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_LOG_PATH, "a") as f:
            f.write(line + "\n")
    except OSError as e:
        # The log is the only durable throughput record: the 2025 sweep's lived
        # in /tmp and is gone. Say so once rather than losing it silently.
        if not _LOG_WARNED:
            print(f"WARNING: cannot append to {_LOG_PATH} ({e}); this run's log is on stdout only",
                  file=sys.stderr, flush=True)
            _LOG_WARNED = True


# ── months ───────────────────────────────────────────────────────────────────

_YM = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


def parse_months(spec: str) -> list[str]:
    """'2025-05..2025-12' or '2026-01,2026-03'.

    Both ends of a range are validated before it is expanded. The expansion
    compares months as strings, which is only sound for well-formed YYYY-MM:
    '2025-05..2025-1' used to give 2025-05..09 and '2025-12..2025-05' nothing at
    all, and an empty month list made `gate` print PASS with no month compared.
    """
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        ends = part.split("..")
        if len(ends) > 2 or not all(_YM.fullmatch(e) for e in ends):
            raise SystemExit(f"bad month spec {part!r}: want YYYY-MM or YYYY-MM..YYYY-MM")
        a, b = ends[0], ends[-1]
        if b < a:
            raise SystemExit(f"bad month range {part!r}: it ends before it starts")
        y, m = int(a[:4]), int(a[5:7])
        while f"{y:04d}-{m:02d}" <= b:
            out.append(f"{y:04d}-{m:02d}")
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    if not out:
        raise SystemExit(f"no months in {spec!r}")
    return sorted(set(out))


def month_is_closed(ym: str, today: dt.date | None = None) -> bool:
    return ym < (today or dt.date.today()).strftime("%Y-%m")


# ── bencode / torrent ────────────────────────────────────────────────────────

def _bdec(b: bytes, i: int = 0):
    c = b[i:i + 1]
    if c == b"i":
        j = b.index(b"e", i)
        return int(b[i + 1:j]), j + 1
    if c == b"l":
        i += 1
        out = []
        while b[i:i + 1] != b"e":
            v, i = _bdec(b, i)
            out.append(v)
        return out, i + 1
    if c == b"d":
        i += 1
        out = {}
        while b[i:i + 1] != b"e":
            k, i = _bdec(b, i)
            v, i = _bdec(b, i)
            out[k] = v
        return out, i + 1
    j = b.index(b":", i)
    n = int(b[i:j])
    return b[j + 1:j + 1 + n], j + 1 + n


def torrent_rs_file(raw: bytes, ym: str) -> dict:
    """Locate submissions/RS_<ym>.zst inside a monthly torrent.

    RS has been file 2 of 2 (after comments/RC) in all 17 torrents for
    2025-05..2026-09, but the index is looked up, not assumed.
    """
    d, _ = _bdec(raw)
    k = raw.index(b"4:info") + 6
    _, j = _bdec(raw, k)
    infohash = hashlib.sha1(raw[k:j]).hexdigest()
    info = d[b"info"]
    name = info[b"name"].decode()
    files = info.get(b"files") or [{b"length": info[b"length"], b"path": [name.encode()]}]
    off = 0
    hit = None
    layout = []
    for idx, f in enumerate(files, 1):
        path = "/".join(p.decode() for p in f[b"path"])
        layout.append({"index": idx, "path": path, "length": int(f[b"length"]), "offset": off})
        if path.split("/")[-1] == f"RS_{ym}.zst":
            hit = dict(layout[-1])
        off += int(f[b"length"])
    if hit is None:
        raise ValueError(f"torrent {infohash} has no RS_{ym}.zst")
    pieces = info[b"pieces"]
    ul = d.get(b"url-list")
    hit.update({
        "infohash": infohash, "name": name, "total_length": off,
        "piece_length": int(info[b"piece length"]),
        "pieces": [pieces[i:i + 20] for i in range(0, len(pieces), 20)],
        "webseeds": [u.decode() for u in ([ul] if isinstance(ul, bytes) else ul or [])],
        "creation_date": d.get(b"creation date"),
        "files": layout,
    })
    return hit


def webseed_url(t: dict, path: str) -> str:
    """BEP 19 multi-file layout: <webseed>/<torrent name>/<path in torrent>."""
    if not t["webseeds"]:
        raise RuntimeError("torrent lists no webseed")
    return t["webseeds"][0].rstrip("/") + "/" + t["name"] + "/" + path


def parse_links(md: str) -> dict[str, str]:
    """{'2026-01': infohash} for every single-month zst row that is not struck
    through. Struck rows (~~2023-10~~) are superseded releases."""
    out = {}
    for m in re.finditer(r"^\|\s*(\d{4}-\d{2})\s*\|\s*\[Academic Torrents\]"
                         r"\(https://academictorrents\.com/details/([0-9a-f]{40})\)", md, re.M):
        out[m.group(1)] = m.group(2)
    return out


def _get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def published_months(links_file: pathlib.Path | None, cache_dir: pathlib.Path) -> dict[str, str]:
    """One GET of download_links.md per run (or a local copy). A month that is
    not listed is not published: dumps have appeared 6-16 days after month end
    over the 17 releases 2025-05..2026-09."""
    if links_file:
        md = links_file.read_text()
    else:
        md = _get(LINKS_URL).decode()
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "download_links.md").write_text(md)
    return parse_links(md)


def load_torrent(ym: str, infohash: str, torrent_dir: pathlib.Path) -> tuple[pathlib.Path, dict]:
    torrent_dir.mkdir(parents=True, exist_ok=True)
    p = torrent_dir / f"{infohash}.torrent"
    alt = torrent_dir / f"{ym}.torrent"
    if not p.exists() and alt.exists():
        p = alt
    if not p.exists():
        p.write_bytes(_get(TORRENT_URL.format(infohash)))
    t = torrent_rs_file(p.read_bytes(), ym)
    if t["infohash"] != infohash:
        raise ValueError(f"{p} has infohash {t['infohash']}, download_links.md lists {infohash}")
    return p, t


# ── integrity ────────────────────────────────────────────────────────────────

def verify_pieces(path: pathlib.Path, t: dict) -> dict:
    """Size check, then SHA1 of every torrent piece that lies wholly inside the
    RS file, plus a sha256 of the file for the sidecar, in one read.

    RS starts mid-piece (it follows the comments file), so its first
    <piece_length bytes share a piece with RC and cannot be checked without RC.
    zstd covers them: RS_2026-09's frame header sets Content_Checksum_flag
    (read 2026-10-19), so `zstd -dc` fails on corrupt content and the chain
    refuses the month. aria2c already verifies pieces as it downloads; this
    repeats it so the webseed path gets the same check and the sidecar can say
    what was verified.
    """
    size = path.stat().st_size
    res = {"size": size, "expected_size": t["length"], "size_ok": size == t["length"],
           "pieces_ok": 0, "pieces_bad": [], "pieces_checked": 0}
    if not res["size_ok"]:
        return res
    P, off, L = t["piece_length"], t["offset"], t["length"]
    first = math.ceil(off / P)
    head = first * P - off                    # bytes before the first whole piece
    last_end = off + L
    sha256 = hashlib.sha256()
    t0 = time.time()
    with open(path, "rb") as f:
        b = f.read(head)
        sha256.update(b)
        i = first
        while True:
            start = i * P
            if start >= last_end:
                break
            end = min(start + P, t["total_length"])
            if end > last_end:                # piece runs into a later file
                tail = f.read()
                sha256.update(tail)
                res["unverified_tail_bytes"] = len(tail)
                break
            chunk = f.read(end - start)
            sha256.update(chunk)
            res["pieces_checked"] += 1
            if hashlib.sha1(chunk).digest() == t["pieces"][i]:
                res["pieces_ok"] += 1
            elif len(res["pieces_bad"]) < 20:
                res["pieces_bad"].append(i)
            i += 1
    res.update({"unverified_head_bytes": head, "sha256": sha256.hexdigest(),
                "seconds": round(time.time() - t0, 1)})
    res.setdefault("unverified_tail_bytes", 0)
    res["ok"] = res["pieces_checked"] > 0 and res["pieces_ok"] == res["pieces_checked"]
    return res


def sha256_file(path: pathlib.Path) -> tuple[str, int]:
    h, n = hashlib.sha256(), 0
    with open(path, "rb") as f:
        for line in f:
            h.update(line)
            n += 1
    return h.hexdigest(), n


ZSTD_MAGIC = 0xFD2FB528


def zstd_head_frames(path: pathlib.Path, upto: int) -> list[dict]:
    """Every zstd frame that starts before byte `upto`, found by walking frame
    and block headers without decoding anything.

    The RS head (2,012,982-16,493,602 B across the 17 torrents for 2025-05..
    2026-09, 16 MiB pieces) shares a piece with RC, so no piece hash covers it.
    A frame's content checksum does, but only if the frame sets it, and only
    RS_2026-09's first frame header has been read (it does). A head byte that is
    not in a checksummed frame is left to zstd's structural checks, which do not
    cover a raw (stored) block, so `verify_head` checks that piece another way.
    """
    out = []
    size = path.stat().st_size
    pos = 0
    with open(path, "rb") as f:
        while pos < min(upto, size):
            f.seek(pos)
            hdr = f.read(14)
            if len(hdr) < 8:
                raise DumpCorrupt(f"{path.name}: truncated zstd frame header at byte {pos:,}")
            magic = int.from_bytes(hdr[:4], "little")
            if magic & 0xFFFFFFF0 == 0x184D2A50:          # skippable frame: no content to decode
                out.append({"offset": pos, "skippable": True})
                pos += 8 + int.from_bytes(hdr[4:8], "little")
                continue
            if magic != ZSTD_MAGIC:
                raise DumpCorrupt(f"{path.name}: no zstd frame at byte {pos:,} (magic {magic:08x})")
            fhd = hdr[4]
            if fhd & 0x08:
                raise DumpCorrupt(f"{path.name}: reserved bit set in the frame header at byte {pos:,}")
            single = (fhd >> 5) & 1
            fcs = ((1 if single else 0), 2, 4, 8)[fhd >> 6]
            checksum = bool((fhd >> 2) & 1)
            p = pos + 5 + (0 if single else 1) + (0, 1, 2, 4)[fhd & 3] + fcs
            while True:
                if p >= upto:
                    # The frame runs past the head; its flag is all that matters,
                    # and walking the rest would touch every block of a 20 GB file.
                    out.append({"offset": pos, "checksum": checksum})
                    return out
                f.seek(p)
                bh = f.read(3)
                if len(bh) < 3:
                    raise DumpCorrupt(f"{path.name}: frame at byte {pos:,} runs past the end of the file")
                v = int.from_bytes(bh, "little")
                last, btype, bsize = v & 1, (v >> 1) & 3, v >> 3
                if btype == 3 or bsize > 128 * 1024:
                    raise DumpCorrupt(f"{path.name}: invalid block header at byte {p:,}")
                p += 3 + (1 if btype == 1 else bsize)
                if last:
                    break
            out.append({"offset": pos, "checksum": checksum})
            pos = p + (4 if checksum else 0)
    if pos > size:
        raise DumpCorrupt(f"{path.name}: zstd frames run past the end of the file")
    return out


def range_get(url: str, a: int, b: int, tries: int = 3, timeout: int = 60) -> bytes:
    """Bytes a..b inclusive, or RuntimeError. A 200 is closed unread: on
    2026-10-19 the webseed answered one of two identical range GETs with the
    whole 22.4 GB object."""
    want = b - a + 1
    last = ""
    for k in range(tries):
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Range": f"bytes={a}-{b}"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                cr = r.headers.get("Content-Range", "")
                if r.status == 206 and cr.startswith(f"bytes {a}-{b}/"):
                    data = r.read(want + 1)
                    if len(data) == want:
                        return data
                    last = f"{len(data):,} bytes, wanted {want:,}"
                else:
                    last = f"HTTP {r.status}, Content-Range {cr!r} (range ignored)"
        except OSError as e:
            last = str(e)
        time.sleep(2 * (k + 1))
    raise RuntimeError(f"range GET {url} bytes={a}-{b} failed {tries} times: {last}")


def verify_head(path: pathlib.Path, t: dict, head: int) -> dict:
    """Account for the head bytes no whole piece covers. Covered when every
    frame that starts in the head sets its content checksum (zstd -dc then
    fails on any change); otherwise the shared piece is hashed with the other
    files' bytes fetched from the webseed (at most one piece, 16 MiB). The
    torrent route already had aria2c check that piece; the webseed route did not."""
    frames = zstd_head_frames(path, head)
    data = [f for f in frames if not f.get("skippable")]
    res = {"frames": len(frames), "content_checksum": bool(data) and all(f["checksum"] for f in data)}
    if head == 0:
        res["checked_by"] = "no head: RS starts on a piece boundary"
        return res
    if res["content_checksum"]:
        res["checked_by"] = "zstd content checksum"
        return res
    P, off, L = t["piece_length"], t["offset"], t["length"]
    i = off // P
    start, end = i * P, min(i * P + P, t["total_length"])
    buf, fetched = bytearray(), 0
    for f in t["files"]:
        lo, hi = max(start, f["offset"]), min(end, f["offset"] + f["length"])
        if lo >= hi:
            continue
        if f["offset"] == off and f["length"] == L:
            with open(path, "rb") as fh:
                fh.seek(lo - off)
                buf += fh.read(hi - lo)
        else:
            buf += range_get(webseed_url(t, f["path"]), lo - f["offset"], hi - f["offset"] - 1)
            fetched += hi - lo
    res["boundary_bytes_fetched"] = fetched
    if hashlib.sha1(bytes(buf)).digest() != t["pieces"][i]:
        raise DumpCorrupt(f"{path.name}: piece {i}, shared with the file before RS, fails its hash "
                          "and the head frames carry no content checksum")
    res["checked_by"] = f"piece {i} hashed with {fetched:,} B of the preceding file from the webseed"
    return res


# ── sidecar ──────────────────────────────────────────────────────────────────

def out_path(out_dir: pathlib.Path, ym: str) -> pathlib.Path:
    return out_dir / f"RS_{ym}_matches.jsonl"


def meta_path(jsonl: pathlib.Path) -> pathlib.Path:
    return jsonl.with_name(jsonl.name.replace(".jsonl", ".meta.json"))


def read_meta(jsonl: pathlib.Path) -> dict | None:
    mp = meta_path(jsonl)
    if not mp.exists():
        return None
    return json.loads(mp.read_text())


def output_problems(jsonl: pathlib.Path, meta: dict | None, ps: dict) -> list[str]:
    """Why this output may NOT be used with pair set `ps`. Empty means usable.
    Shared with reddit_backfill_merge so the sweep and the merge cannot drift."""
    if meta is None:
        return ["no sidecar: filtered before pair-set provenance existed (2025-09-05 sweep) "
                "or interrupted; re-sweep the month"]
    probs = []
    if meta.get("kind") != "submission":
        probs.append(f"kind {meta.get('kind')!r}, not 'submission'")
    if meta.get("matcher") != MATCHER:
        probs.append("filtered with a different matcher")
    got = (meta.get("pairset") or {}).get("sha1")
    if got != ps["sha1"]:
        old = set((meta.get("pairset") or {}).get("slugs") or [])
        new = set(ps["slugs"])
        probs.append(f"pair set {str(got)[:12]} != current {ps['sha1'][:12]} "
                     f"(+{sorted(new - old)} -{sorted(old - new)}"
                     f"{', same slugs but terms differ' if old == new else ''})")
    if not jsonl.exists():
        probs.append("sidecar without its jsonl (interrupted before the rename)")
        return probs
    sha, n = sha256_file(jsonl)
    o = meta.get("output") or {}
    if sha != o.get("sha256") or n != o.get("records"):
        probs.append(f"jsonl changed after the sweep wrote it (sha256/records {sha[:12]}/{n} "
                     f"vs sidecar {str(o.get('sha256'))[:12]}/{o.get('records')})")
    return probs


def archive_output(jsonl: pathlib.Path, meta: dict | None) -> pathlib.Path:
    """Move an output that cannot be reused out of the merge's glob. Never
    overwrites: these files are what `gate` compares a re-sweep with."""
    tag = "nometa" if meta is None else (meta.get("pairset") or {}).get("sha1", "unknown")[:12]
    dest_dir = jsonl.parent / "archive" / tag
    dest_dir.mkdir(parents=True, exist_ok=True)
    for src in (jsonl, meta_path(jsonl)):
        if not src.exists():
            continue
        dst = dest_dir / src.name
        if dst.exists():
            if dst.read_bytes() != src.read_bytes():
                raise SystemExit(f"REFUSED: {dst} exists with different content; resolve by hand")
            src.unlink()
        else:
            src.rename(dst)
    return dest_dir


# ── tools ────────────────────────────────────────────────────────────────────

def tool_versions() -> dict:
    out = {}
    for tool, flag in (("zstd", "--version"), ("rg", "--version"), ("aria2c", "--version"),
                       ("curl", "--version")):
        try:
            out[tool] = subprocess.run([tool, flag], capture_output=True, text=True,
                                       timeout=10).stdout.splitlines()[0]
        except (OSError, IndexError, subprocess.SubprocessError):
            out[tool] = None
    return out


def _wait(proc: subprocess.Popen, label: str, size_of, every: int = 60) -> int:
    """Wait for proc, logging growth of `size_of()` once a minute."""
    last_t, last_b = time.time(), size_of()
    while True:
        try:
            return proc.wait(timeout=every)
        except subprocess.TimeoutExpired:
            now, b = time.time(), size_of()
            log(f"    {label}: {b / 1e9:.2f} GB, {(b - last_b) / 1e6 / (now - last_t):.1f} MB/s")
            last_t, last_b = now, b


def _size(p: pathlib.Path) -> int:
    try:
        return p.stat().st_size
    except OSError:
        return 0


def _allocated(p: pathlib.Path) -> int:
    """Bytes actually on disk. aria2c writes pieces out of order into a sparse
    file, so st_size jumps to the furthest offset written and overstates progress."""
    try:
        return p.stat().st_blocks * 512
    except OSError:
        return 0


# ── one month ────────────────────────────────────────────────────────────────

def aria2_control(bt_dir: pathlib.Path, t: dict) -> pathlib.Path:
    """aria2c keeps a multi-file torrent's control file at <dir>/<name>.aria2,
    not beside the file, and deletes it only when the download completes."""
    return bt_dir / f"{t['name']}.aria2"


class DumpCorrupt(RuntimeError):
    """The .zst itself is bad; it is deleted so a re-run downloads it again."""


def dump_file(dump_dir: pathlib.Path, t: dict) -> pathlib.Path:
    """Where aria2c puts the RS file (<dir>/<infohash>/<torrent name>/<path>);
    the webseed path writes to the same place so a month can switch route."""
    return dump_dir / t["infohash"] / t["name"] / t["path"]


def download(t: dict, torrent: pathlib.Path, bt_dir: pathlib.Path, dest: pathlib.Path,
             via: str, dl_log: pathlib.Path, bt_stop: int = 1800) -> dict:
    if via == "auto":
        # aria2c drops a webseed for the rest of the run after one response that
        # ignores Range ("errorCode=8 Invalid range header", reproduced against a
        # local webseed 2026-10-19), and the real webseed did ignore one of two
        # range GETs that day. With a thin swarm that leaves a stalled download,
        # so fall back to curl, which retries the same fault.
        try:
            return download(t, torrent, bt_dir, dest, "torrent", dl_log, bt_stop)
        except RuntimeError as e:
            log(f"    torrent route failed ({e}); falling back to the webseed")
            return download(t, torrent, bt_dir, dest, "http", dl_log, bt_stop)
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    control = aria2_control(bt_dir, t)
    if via == "torrent":
        if part.exists() and not dest.exists():
            part.rename(dest)                 # a webseed partial is sequential; aria2c re-checks it
        # --bt-stop-timeout: a download at zero speed for that long exits non-zero
        # instead of waiting forever, and `auto` moves the month to the webseed.
        cmd = ["aria2c", f"--select-file={t['index']}", "--seed-time=0", f"--bt-stop-timeout={bt_stop}",
               "--file-allocation=none", "--bt-remove-unselected-file=true",
               "--check-integrity=true", "--auto-file-renaming=false",
               "--summary-interval=0", "--console-log-level=warn",
               f"--log={dl_log}", "--log-level=notice", f"--dir={bt_dir}", str(torrent)]
        target, measure = dest, _allocated
    else:
        url = webseed_url(t, t["path"])
        if control.exists() or dest.exists():
            # Only a .part is a webseed partial. Anything at `dest` here is an
            # aria2c partial, which is sparse and can already have its full
            # st_size: curl -C - would see nothing left to fetch and keep the
            # holes. Measured 2026-10-19 on a stalled aria2c: the RS head never
            # arrived, all whole pieces verified, and only zstd caught it.
            dest.unlink(missing_ok=True)
            control.unlink(missing_ok=True)
            log("    discarded an aria2c partial before switching to the webseed")
        # The webseed does not always honour Range: on 2026-10-19 one of two
        # identical `-r 0-17` GETs got a 206 of 18 bytes, the other streamed the
        # whole 22.4 GB object. With -C -, curl refuses a 200 to a resume request
        # (exit 33) rather than appending a full body to the partial, and
        # --retry-all-errors retries it; the piece check catches anything else.
        cmd = ["curl", "-fL", "-sS", "--retry", "10", "--retry-delay", "15",
               "--retry-all-errors", "-C", "-", "-A", UA, "-o", str(part), url]
        target, measure = part, _size
    before = measure(target)
    log(f"    $ {' '.join(cmd)}")
    t0 = time.time()
    with open(dl_log.with_suffix(".out"), "ab") as out:
        proc = subprocess.Popen(cmd, stdout=out, stderr=out)
        rc = _wait(proc, f"download ({via})", lambda: measure(target))
    if rc != 0:
        raise RuntimeError(f"{cmd[0]} exited {rc}; see {dl_log}")
    got = measure(target) - before
    if target != dest:
        part.rename(dest)
    secs = time.time() - t0
    return {"via": via, "seconds": round(secs, 1), "bytes": got,
            "mb_per_s": round(got / 1e6 / secs, 1) if secs > 0 else None}


def run_filter(zst: pathlib.Path, part: pathlib.Path, pat: pathlib.Path, ps: dict,
               config: pathlib.Path | None, errlog: pathlib.Path) -> dict:
    stats_path = part.with_name(part.name + ".stats.json")
    filt = [sys.executable, "-m", "pipeline.ingestion.reddit_dump_filter", "--kind", "submission",
            "--expect-pairset", ps["sha1"], "--stats-json", str(stats_path)]
    if config:
        filt += ["--config", str(config)]
    # rg flags are load-bearing, measured 2026-10-19 on ripgrep 15.2.0:
    #   -a          reading stdin without it, one NUL byte makes rg print
    #               "binary file matches" and emit NOTHING (not even earlier
    #               lines), with exit 0.
    #   --no-config a user RIPGREP_CONFIG_PATH could add --max-columns, which
    #               replaces long lines (long selftexts) with a placeholder.
    #   -E none     no BOM sniffing, so bytes reach the filter untranscoded.
    rg = ["rg", "--no-config", "-a", "-i", "-F", "-E", "none", "--no-line-number",
          "--color", "never", "-f", str(pat)]
    t0 = time.time()
    with open(errlog, "ab") as err, open(part, "wb") as out:
        p1 = subprocess.Popen(["zstd", "-dc", "-q", "--long=31", str(zst)],
                              stdout=subprocess.PIPE, stderr=err)
        p2 = subprocess.Popen(rg, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=err)
        p1.stdout.close()
        p3 = subprocess.Popen(filt, stdin=p2.stdout, stdout=out, stderr=err, cwd=ROOT)
        p2.stdout.close()
        rc3 = _wait(p3, "filter", lambda: _size(part))
        rc2, rc1 = p2.wait(), p1.wait()
    secs = time.time() - t0
    # filter 0 = done, 2/3 = refused; rg 0 = some lines, 1 = none (legal in a
    # test, never seen on a real dump); zstd 0 = clean decode. The filter is read
    # first: when it refuses, zstd dies of SIGPIPE and that is not the dump's fault.
    rcs = f"zstd={rc1} rg={rc2} filter={rc3}; see {errlog}"
    if rc3 != 0 or rc2 not in (0, 1):
        raise RuntimeError(f"filter chain failed: {rcs}")
    if rc1 != 0:
        raise DumpCorrupt(f"zstd could not decode the dump: {rcs}")
    st = json.loads(stats_path.read_text())
    stats_path.unlink()
    if st["pairset_sha1"] != ps["sha1"]:
        raise RuntimeError("filter ran with a different pair set than the sweep planned")
    st.update({"seconds": round(secs, 1), "rc": {"zstd": rc1, "rg": rc2, "filter": rc3},
               "mb_per_s_compressed": round(zst.stat().st_size / 1e6 / secs, 1) if secs else None})
    return st


def free_bytes(p: pathlib.Path) -> int:
    p.mkdir(parents=True, exist_ok=True)
    return shutil.disk_usage(p).free


def match_reference(out_dir: pathlib.Path, ym: str, ps: dict) -> dict:
    """The count a month's matches are judged against: the median of the other
    months this pair set has produced, else of the archived outputs (for the
    first month of a re-sweep, the earlier sweep of the same months)."""
    cur = []
    for mp in sorted(out_dir.glob("RS_*_matches.meta.json")):
        if mp.name[3:10] == ym:
            continue
        try:
            m = json.loads(mp.read_text())
        except ValueError:
            continue
        if (m.get("pairset") or {}).get("sha1") == ps["sha1"]:
            cur.append(int((m.get("output") or {}).get("records") or 0))
    if cur:
        return {"reference": "other months, this pair set", "n": len(cur), "median": statistics.median(cur)}
    arch = [sha256_file(f)[1] for f in sorted((out_dir / "archive").glob("*/RS_*_matches.jsonl"))]
    if arch:
        return {"reference": "archived outputs", "n": len(arch), "median": statistics.median(arch)}
    return {"reference": None, "n": 0, "median": None}


def mirror_sidecar(mp: pathlib.Path, ym: str, log_dir: pathlib.Path) -> pathlib.Path:
    """Copy a sidecar to data/audit/reddit_dump_sidecars/. The originals sit in
    data/raw, which is on one disk only; the 2025 sweep lost its runner, logs
    and validation that way."""
    dst = log_dir / "reddit_dump_sidecars" / f"RS_{ym}.meta.json"
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists() or dst.read_bytes() != mp.read_bytes():
        shutil.copyfile(mp, dst)
    return dst


def sweep_month(ym: str, infohash: str, a, ps: dict) -> str:
    out_dir, dump_dir = a.out_dir, a.dump_dir
    jsonl = out_path(out_dir, ym)
    meta = read_meta(jsonl)
    if jsonl.exists() or meta is not None:
        probs = output_problems(jsonl, meta, ps)
        if not probs:
            log(f"=== {ym}: done (pair set {ps['sha1'][:12]}, "
                f"{meta['output']['records']:,} records) - skip")
            mirror_sidecar(meta_path(jsonl), ym, a.log_dir)
            return "skipped"
        if jsonl.exists() and meta is not None and (meta.get("pairset") or {}).get("sha1") == ps["sha1"]:
            raise SystemExit(f"REFUSED {ym}: {jsonl.name} has the current pair set but "
                             f"{'; '.join(probs)}. Resolve by hand; it was not moved.")
        if jsonl.exists():
            d = archive_output(jsonl, meta)
            log(f"=== {ym}: archived {jsonl.name} to {d.relative_to(out_dir)} ({'; '.join(probs)})")
        else:
            meta_path(jsonl).unlink()          # sidecar of an interrupted run
    torrent, t = load_torrent(ym, infohash, a.torrent_dir)
    zst = dump_file(dump_dir, t)
    bt_dir = dump_dir / t["infohash"]
    part_dl = zst.with_name(zst.name + ".part")
    present = _allocated(zst) + _size(part_dl)
    free = free_bytes(dump_dir)
    need = 2 * t["length"]
    if free + present < need:
        raise SystemExit(f"REFUSED {ym}: {free / 2**30:.1f} GiB free (+{present / 2**30:.1f} GiB "
                         f"already downloaded) < 2 x {t['length'] / 1e9:.2f} GB")
    log(f"=== {ym}: RS {t['length'] / 1e9:.2f} GB, torrent {infohash[:12]} file {t['index']}, "
        f"free {free / 2**30:.0f} GiB")

    # Size alone does not mean complete: an aria2c partial is sparse and can
    # carry its full st_size. Complete = right size, no aria2c control file, no .part.
    if (_size(zst) == t["length"] and not aria2_control(bt_dir, t).exists()
            and not part_dl.exists()):
        dl = {"via": "already-present", "seconds": 0, "bytes": 0, "mb_per_s": None}
        log(f"    dump already present: {zst}")
    else:
        dl = download(t, torrent, bt_dir, zst, a.via,
                      a.log_dir / f"download_{ym}.log", a.bt_stop_timeout)
        log(f"    downloaded {dl['bytes'] / 1e9:.2f} GB in {dl['seconds'] / 60:.1f} min "
            f"= {dl['mb_per_s']} MB/s ({dl['via']})")

    v = verify_pieces(zst, t)
    if not v.get("ok"):
        # Deleted rather than kept, so a re-run converges: a kept file of the
        # right size would be taken as present and fail the same way forever.
        shutil.rmtree(bt_dir)
        why = (f"{v['size']:,} B, torrent says {v['expected_size']:,}" if not v["size_ok"] else
               f"{len(v['pieces_bad'])}+ bad pieces {v['pieces_bad'][:5]} of {v['pieces_checked']}")
        raise DumpCorrupt(f"{zst.name}: {why}; deleted {bt_dir.name}, re-run to download it again")
    try:
        head = verify_head(zst, t, v["unverified_head_bytes"])
    except DumpCorrupt as e:
        shutil.rmtree(bt_dir)
        raise DumpCorrupt(f"{e}; deleted {bt_dir.name}, re-run to download it again")
    log(f"    verified {v['pieces_ok']}/{v['pieces_checked']} pieces in {v['seconds']}s; "
        f"head {v['unverified_head_bytes']:,} B ({head['frames']} frame(s)): {head['checked_by']}")

    pat = dump_dir / f"prefilter_{ps['sha1'][:12]}.txt"
    pat.write_text("\n".join(prefilter_tokens(ps["terms"])) + "\n")
    part = jsonl.with_name(jsonl.name + ".part")
    try:
        st = run_filter(zst, part, pat, ps, a.config, a.log_dir / f"filter_{ym}.log")
    except DumpCorrupt as e:
        shutil.rmtree(bt_dir)
        raise DumpCorrupt(f"{e}; deleted {bt_dir.name}, re-run to download it again")
    sha, n = sha256_file(part)
    if n != st["matches"]:
        raise RuntimeError(f"{part} holds {n} lines, filter reported {st['matches']}")
    log(f"    filtered {st['lines_in']:,} prefiltered lines -> {st['matches']:,} matches in "
        f"{st['seconds'] / 60:.1f} min = {st['mb_per_s_compressed']} MB/s compressed"
        f"{'; ' + str(st['json_errors']) + ' unparseable' if st['json_errors'] else ''}")
    # Piece hashes and zstd prove the dump is the published one, not that the
    # chain read all of it: rg exits 1 on no lines, and a broken prefilter or a
    # pattern file gone wrong would publish a near-empty month that draws as a
    # collapse. The dump is kept, so re-running with --allow-low costs no download.
    ref = match_reference(out_dir, ym, ps)
    ref["ratio"] = round(n / ref["median"], 3) if ref["median"] else None
    ref["allow_low"] = bool(a.allow_low)
    low = n == 0 or (ref["ratio"] is not None and ref["ratio"] < LOW_RATIO)
    if low and not a.allow_low:
        raise RuntimeError(
            f"{n:,} matches, {ref['ratio']}x the median {ref['median']} of {ref['reference']} "
            f"(floor {LOW_RATIO}x); not published. Check the dump, {pat.name} and "
            f"{a.log_dir / f'filter_{ym}.log'}, then re-run with --allow-low if the month is real")
    log(f"    {n:,} matches = " + (f"{ref['ratio']}x the median of {ref['reference']}"
                                   if ref["median"] else "no reference month yet")
        + (" (low, allowed by --allow-low)" if low else ""))

    meta = {
        "month": ym, "kind": "submission", "matcher": MATCHER,
        "pairset": ps, "pair_scope": SCOPE,
        "torrent": {k: t[k] for k in ("infohash", "index", "path", "length", "offset",
                                      "piece_length", "creation_date")},
        "download": dl,
        "verify": {**{k: v[k] for k in ("size", "pieces_ok", "pieces_checked",
                                        "unverified_head_bytes", "unverified_tail_bytes",
                                        "sha256", "seconds")}, "head": head},
        "filter": {k: st[k] for k in ("lines_in", "candidates", "json_errors", "matches",
                                      "seconds", "mb_per_s_compressed", "rc")},
        "output": {"file": jsonl.name, "records": n, "sha256": sha, "bytes": _size(part)},
        "match_floor": ref,
        "tools": tool_versions(),
        "completed_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    # Sidecar first, then the rename: a jsonl never exists without its sidecar,
    # so a bare jsonl can only be a pre-provenance output and is archived as such.
    mp = meta_path(jsonl)
    tmp = mp.with_name(mp.name + ".tmp")
    tmp.write_text(json.dumps(meta, indent=1, ensure_ascii=False))
    os.replace(tmp, mp)
    os.replace(part, jsonl)
    mirror_sidecar(mp, ym, a.log_dir)
    if a.keep_dump:
        log(f"    kept {zst}")
    else:
        shutil.rmtree(dump_dir / t["infohash"])
        log(f"    deleted {dump_dir / t['infohash']}")
    return "swept"


# ── commands ─────────────────────────────────────────────────────────────────

def _months_with_links(a) -> tuple[list[str], dict[str, str]]:
    months = parse_months(a.months)
    links = published_months(a.links_file, a.dump_dir)
    bad = [(m, "not closed") for m in months if not month_is_closed(m)]
    bad += [(m, "not in download_links.md") for m in months if month_is_closed(m) and m not in links]
    if bad:
        raise SystemExit("REFUSED: " + "; ".join(f"{m} {why}" for m, why in bad)
                         + f". Newest published month: {max(links) if links else 'none'}")
    return months, links


@contextlib.contextmanager
def _lock(out_dir: pathlib.Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / ".sweep.lock", "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"REFUSED: another sweep holds {out_dir / '.sweep.lock'}")
        yield


def cmd_plan(a) -> int:
    """Read-only: what `run` would do. Reports unpublished or open months
    instead of refusing, so it can be used to see when a month lands."""
    ps = pair_set(swept_pairs(a.config))
    months = parse_months(a.months)
    links = published_months(a.links_file, a.dump_dir)
    print(f"pair set {ps['sha1'][:12]}: {len(ps['slugs'])} pairs, {len(ps['terms'])} terms, "
          f"{len(prefilter_tokens(ps['terms']))} rg tokens; newest published month "
          f"{max(links) if links else 'none'}")
    total = 0
    for ym in months:
        if not month_is_closed(ym) or ym not in links:
            print(f"  {ym}  {'not closed' if not month_is_closed(ym) else 'not published yet'}")
            continue
        _, t = load_torrent(ym, links[ym], a.torrent_dir)
        jsonl = out_path(a.out_dir, ym)
        meta = read_meta(jsonl)
        if not jsonl.exists() and meta is None:
            state = "to sweep"
        else:
            probs = output_problems(jsonl, meta, ps)
            state = "done" if not probs else "re-sweep (" + "; ".join(probs) + ")"
        if state != "done":
            total += t["length"]
        print(f"  {ym}  RS {t['length'] / 1e9:6.2f} GB  file {t['index']}  {state}")
    free = free_bytes(a.dump_dir)
    print(f"to download {total / 1e9:.2f} GB; free {free / 2**30:.0f} GiB at {a.dump_dir}")
    # Rates measured on the 2025-05..12 sweep (2026-09-05/06, full RS+RC torrents):
    # download 35.3-43.2 MB/s in 6 of 8 months (24.9 in 2025-09, 2.6 in 2025-10),
    # RS filter 46.1-48.0 MB/s of compressed input. RS-only via --select-file is
    # unmeasured, and so is the filter with 94 terms: per candidate line the
    # filter is now faster (3,584 lines/s with 94 terms, against 2,039 for the
    # 2025 code with 48), but the extra rg tokens pass more candidates.
    if total:
        mb = total / 1e6
        print(f"projected at 2025 rates: download {mb / 43.2 / 60:.0f}-{mb / 35.3 / 60:.0f} min "
              f"({mb / 24.9 / 60:.0f} at 24.9 MB/s, {mb / 2.6 / 3600:.1f} h at 2.6), "
              f"filter {mb / 48.0 / 60:.0f}-{mb / 46.1 / 60:.0f} min")
    return 0


def cmd_run(a) -> int:
    global _LOG_PATH
    _LOG_PATH = a.log_dir / "reddit_dump_sweep.log"
    try:
        a.log_dir.mkdir(parents=True, exist_ok=True)
        with open(_LOG_PATH, "a"):
            pass
    except OSError as e:
        raise SystemExit(f"REFUSED: cannot append to {_LOG_PATH} ({e}). The log and the sidecar "
                         "copies there are the run's only durable throughput and provenance record.")
    ps = pair_set(swept_pairs(a.config))
    with _lock(a.out_dir):
        months, links = _months_with_links(a)
        log(f"### sweep {months[0]}..{months[-1]} ({len(months)} months), pair set "
            f"{ps['sha1'][:12]} ({len(ps['slugs'])} pairs), via {a.via}")
        t0 = time.time()
        done = []
        for ym in months:
            # Stop at the first failure: the merge needs contiguous months, so a
            # later month swept past a hole is wasted bandwidth until the hole closes.
            try:
                done.append((ym, sweep_month(ym, links[ym], a, ps)))
            except RuntimeError as e:
                kept = "" if isinstance(e, DumpCorrupt) else " The dump is kept."
                log(f"!!! {ym} FAILED: {e}. Nothing published for {ym}.{kept} Re-run to resume.")
                return 1
        log(f"### done in {(time.time() - t0) / 60:.1f} min: "
            + ", ".join(f"{m} {s}" for m, s in done))
    return 0


def _records(path: pathlib.Path) -> dict[str, tuple[bytes, ...]]:
    """id -> its lines. 0 of 60,711 ids repeat within a 2025 RS file, but a
    repeat must compare as a repeat rather than collapse to one line."""
    out: dict[str, list[bytes]] = {}
    with open(path, "rb") as f:
        for line in f:
            out.setdefault(json.loads(line)["id"], []).append(line)
    return {k: tuple(sorted(v)) for k, v in out.items()}


def _legacy_pairs(a) -> list[dict]:
    """The pairs a sidecar-less legacy output was filtered with. The 2025-09-05
    sweep searched ENABLED pairs, so that is what is reconstructed here."""
    if a.legacy_config:
        return enabled_pairs(a.legacy_config)
    import yaml
    doc = yaml.safe_load(subprocess.check_output(
        ["git", "show", f"{a.legacy_rev}:config/pairs.yaml"], cwd=ROOT))
    return [p for p in doc["pairs"] if p.get("enabled", True)]


def load_gate_report(path: pathlib.Path) -> dict:
    if not path.exists():
        return {"months": {}}
    rep = json.loads(path.read_text())
    if not isinstance(rep.get("months"), dict):
        raise SystemExit(f"{path} is not a gate report")
    return rep


def gate_month(ym: str, a, ps: dict, rx_cur, per_pair: dict, cli_leg: dict | None) -> dict:
    new_f, leg_f = out_path(a.out_dir, ym), out_path(a.legacy_dir, ym)
    meta = read_meta(new_f)
    errors = [f"re-sweep: {p}" for p in output_problems(new_f, meta, ps)]
    leg_meta = read_meta(leg_f)
    if not leg_f.exists():
        errors.append(f"no legacy file {leg_f}")
        leg = None
    elif leg_meta is not None:
        # A legacy output this code wrote carries its own pair set; trust only
        # that, after checking the file still matches it.
        leg = leg_meta.get("pairset") or {}
        errors += [f"legacy: {p}" for p in output_problems(leg_f, leg_meta, {**leg, "sha1": leg.get("sha1")})]
    elif cli_leg is not None:
        leg = cli_leg
    else:
        errors.append(f"{leg_f.name} has no sidecar; say which pair set it was filtered "
                      "with (--legacy-rev f4a2f92d for the 2025-09-05 sweep)")
        leg = None
    if errors:
        return {"pass": False, "error": errors}
    rx_leg = compile_matcher(leg["terms"])
    new, old = _records(new_f), _records(leg_f)
    texts = {rid: text_of(json.loads(lines[0])) for rid, lines in {**old, **new}.items()}
    m = {"legacy": len(old), "new": len(new), "unexpected": [], "content_diff": [],
         "legacy_unmatched": [], "pairs": {}}
    for rid in old:
        if not rx_leg.search(texts[rid]):
            m["legacy_unmatched"].append(rid)       # wrong legacy pair set
    for rid in old.keys() & new.keys():
        if old[rid] != new[rid]:
            m["content_diff"].append(rid)
    for rid in old.keys() - new.keys():
        if rx_cur.search(texts[rid]):
            m["unexpected"].append({"id": rid, "only_in": "legacy"})
    for rid in new.keys() - old.keys():
        if rx_leg.search(texts[rid]):
            m["unexpected"].append({"id": rid, "only_in": "new"})
    # Month level as well as per pair: a legacy-only post that no current pair
    # matches (a retired term's) appears in no per-pair row.
    m["only_legacy_ids"] = sorted(old.keys() - new.keys())
    m["only_new_ids"] = sorted(new.keys() - old.keys())
    for slug, (rua, rru) in per_pair.items():
        hit = {r for r in texts if rua.search(texts[r]) or rru.search(texts[r])}
        lo, nw = hit & old.keys(), hit & new.keys()
        m["pairs"][slug] = {"legacy": len(lo), "new": len(nw),
                            "only_legacy": sorted(lo - nw), "only_new": sorted(nw - lo)}
    m["pass"] = not (m["unexpected"] or m["content_diff"] or m["legacy_unmatched"])
    # What the merge checks before it accepts a re-swept month: this exact
    # re-sweep, compared with this exact earlier file, under this pair set.
    m.update({"new_sha256": meta["output"]["sha256"], "legacy_file": str(leg_f),
              "legacy_sha256": sha256_file(leg_f)[0], "legacy_pairset": leg["sha1"]})
    return m


def cmd_gate(a) -> int:
    """Determinism gate for a re-sweep: same dumps, same matcher, so every post
    the old sweep found under a term both pair sets search for must come back,
    byte-identical, and nothing may come back that the old terms should have found.

    An id present in only one sweep is EXPECTED only when no term of the other
    sweep matches it (zaporizhzhia's and the disabled pairs' new posts; a retired
    term's dropped ones). Any other one-sided id is a loss in one run, e.g. an
    rg that stopped at a NUL.

    Results are upserted per month into --report, which the merge reads: it
    refuses any re-swept month whose latest gate entry is missing, failed, or
    describes a different file.
    """
    cur_pairs = swept_pairs(a.config)
    ps = pair_set(cur_pairs)
    cli_leg = pair_set(_legacy_pairs(a)) if (a.legacy_rev or a.legacy_config) else None
    rx_cur = compile_matcher(ps["terms"])
    per_pair = {p["slug"]: (compile_matcher([p["ukrainian"]]), compile_matcher([p["russian"]]))
                for p in cur_pairs}
    months = parse_months(a.months)
    legacy_months = sorted(f.name[3:10] for f in a.legacy_dir.glob("RS_*_matches.jsonl"))
    uncovered = [m for m in legacy_months if m not in months]
    report = load_gate_report(a.report)
    stamp = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"current {ps['sha1'][:12]} ({len(ps['slugs'])} pairs) vs "
          f"{'legacy ' + cli_leg['sha1'][:12] if cli_leg else 'legacy sidecars'}; legacy dir {a.legacy_dir}")
    if cli_leg:
        print(f"  terms only legacy {sorted(set(cli_leg['terms']) - set(ps['terms']))}, "
              f"only current {sorted(set(ps['terms']) - set(cli_leg['terms']))}")
    tot: dict[str, list[int]] = {}
    ok_all, compared = True, 0
    for ym in months:
        m = gate_month(ym, a, ps, rx_cur, per_pair, cli_leg)
        m.update({"pairset": ps["sha1"], "gated_at": stamp})
        report["months"][ym] = m
        ok_all &= m["pass"]
        if "error" in m:
            print(f"  {ym}  CANNOT COMPARE: {'; '.join(m['error'])}")
            continue
        compared += 1
        for slug, r in m["pairs"].items():
            t = tot.setdefault(slug, [0, 0, 0, 0])
            for k, n in enumerate((r["legacy"], r["new"], len(r["only_legacy"]), len(r["only_new"]))):
                t[k] += n
        print(f"  {ym}  legacy {m['legacy']:,}  new {m['new']:,}  only-legacy {len(m['only_legacy_ids']):,}"
              f"  only-new {len(m['only_new_ids']):,}  unexpected {len(m['unexpected'])}"
              f"  content-diff {len(m['content_diff'])}  legacy-unmatched {len(m['legacy_unmatched'])}"
              f"  {'PASS' if m['pass'] else 'FAIL'}")
    if not compared:
        ok_all = False
        print("  no month compared")
    if uncovered and not a.partial:
        ok_all = False
        print(f"  legacy months not in --months: {uncovered} (pass --partial to gate a subset)")
    if tot:
        print(f"  {'pair':26s} {'legacy':>7s} {'new':>7s} {'only-leg':>8s} {'only-new':>8s}")
        for slug, (lo, nw, ol, on) in sorted(tot.items()):
            print(f"  {slug:26s} {lo:7,d} {nw:7,d} {ol:8,d} {on:8,d}")
    report["last_run"] = {"months": months, "pass": ok_all, "compared": compared,
                          "legacy_dir": str(a.legacy_dir), "uncovered_legacy_months": uncovered,
                          "pairset": ps["sha1"], "legacy_pairset": cli_leg["sha1"] if cli_leg else None,
                          "at": stamp,
                          "pair_totals": {s: dict(zip(("legacy", "new", "only_legacy", "only_new"), v))
                                          for s, v in tot.items()}}
    a.report.parent.mkdir(parents=True, exist_ok=True)
    tmp = a.report.with_name(a.report.name + ".tmp")
    tmp.write_text(json.dumps(report, indent=1))
    os.replace(tmp, a.report)
    print(f"GATE {'PASS' if ok_all else 'FAIL'} -> {a.report}")
    return 0 if ok_all else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("plan", "run", "gate"):
        s = sub.add_parser(name)
        s.add_argument("--months", required=True, help="2026-01..2026-09 or 2026-01,2026-02")
        s.add_argument("--out-dir", type=pathlib.Path, default=BF)
        s.add_argument("--config", type=pathlib.Path, default=None,
                       help="pairs.yaml (default: the repo's)")
        if name in ("plan", "run"):
            s.add_argument("--dump-dir", type=pathlib.Path, default=DUMPS,
                           help="where .zst files are downloaded (needs 2x the month free)")
            s.add_argument("--torrent-dir", type=pathlib.Path, default=None,
                           help="cached .torrent files (default <dump-dir>/torrents)")
            s.add_argument("--links-file", type=pathlib.Path, default=None,
                           help="local download_links.md instead of one GET")
        if name == "run":
            s.add_argument("--via", choices=["auto", "torrent", "http"], default="auto",
                           help="torrent: aria2c, swarm + BEP19 webseed; http: curl from the "
                                "webseed only; auto: torrent, then http if aria2c fails")
            s.add_argument("--bt-stop-timeout", type=int, default=1800,
                           help="seconds at zero download speed before aria2c gives up")
            s.add_argument("--keep-dump", action="store_true")
            s.add_argument("--allow-low", action="store_true",
                           help=f"publish a month with fewer than {LOW_RATIO}x the reference "
                                "median of matches (or none); recorded in the sidecar")
            s.add_argument("--log-dir", type=pathlib.Path, default=LOG.parent,
                           help="sweep log, per-month logs and the sidecar copies")
        if name == "gate":
            s.add_argument("--legacy-dir", type=pathlib.Path, default=BF / "archive" / "nometa")
            g = s.add_mutually_exclusive_group()
            g.add_argument("--legacy-rev", help="git revision whose config/pairs.yaml a sidecar-less "
                                                "legacy sweep read (f4a2f92d for the 2025-09-05 sweep)")
            g.add_argument("--legacy-config", type=pathlib.Path)
            s.add_argument("--partial", action="store_true",
                           help="allow --months to leave out months that --legacy-dir holds")
            s.add_argument("--report", type=pathlib.Path, default=GATE_REPORT)
    a = ap.parse_args(argv)
    if getattr(a, "torrent_dir", "unset") is None:
        a.torrent_dir = a.dump_dir / "torrents"
    return {"plan": cmd_plan, "run": cmd_run, "gate": cmd_gate}[a.cmd](a)


if __name__ == "__main__":
    raise SystemExit(main())
