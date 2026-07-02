"""
this code written by ClaudeAI and for running it you are going to need an envio hypersync token 
 export HYPERSYNC_BEARER_TOKEN=<YOUR_TOKEN>
 python3 fetch_logs.py

 after it downloads data it seperates data by token and topic0 so you are going to have per parquet file for each token
 and event type 
 
"""
#!/usr/bin/env python3
"""
Fetch all on-chain event logs for PAXG and XAUT (Ethereum mainnet) via Envio
HyperSync and write them to parquet: one folder per token, one parquet file per
event type (topic0). No decoding — raw logs with HyperSync's native snake_case
columns, plus block_timestamp joined onto every row.

Per-row columns: block_number, block_timestamp, log_index, transaction_index,
transaction_hash, block_hash, address, data, topic0, topic1, topic2, topic3.

Setup — get a FREE HyperSync API token:
    https://envio.dev  ->  app.envio.dev  ->  "API Tokens"
    export HYPERSYNC_BEARER_TOKEN=YOUR_TOKEN

Run:
    python3 fetch_logs.py              # full history (from block 0 to tip)
    python3 fetch_logs.py --test       # quick sanity run: ~50k recent blocks
    python3 fetch_logs.py --only PAXG  # one token only

Files are named by event name (resolved via the 4byte directory, verified by
re-hashing); unresolved events fall back to the topic0 hex.

Output:
    raw/PAXG/Transfer.parquet          # one file per event type, uniform schema
    raw/PAXG/Approval.parquet
    raw/PAXG/SupplyDecreased.parquet
    raw/XAUT/...
`pd.read_parquet("raw/PAXG")` reads all event files in the folder as one table.
"""
import argparse
import asyncio
import glob
import json
import os
import re
import shutil
import ssl
import sys
import time
import urllib.request

import duckdb
import pyarrow.parquet as pq
from hypersync import (ClientConfig, ColumnMapping, DataType, FieldSelection,
                       HexOutput, HypersyncClient, LogField, LogSelection,
                       BlockField, Query, StreamConfig, signature_to_topic0)

ETH_URL = "https://eth.hypersync.xyz"
BASE = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(BASE, "raw")

CONTRACTS = {
    "PAXG": "0x45804880De22913dAFE09f4980848ECE6EcbAf78",
    "XAUT": "0x68749665ff8d2d112fa859aa293f07a622782f38",
}

# Authoritative pins for the common events (no network needed for these).
PINNED = {
    signature_to_topic0("Transfer(address,address,uint256)"): "Transfer",
    signature_to_topic0("Approval(address,address,uint256)"): "Approval",
}

# 4byte lookup setup: certifi for SSL on macOS python.org builds; a normal
# User-Agent (4byte 403s the default python-urllib UA).
try:
    import certifi
    _SSL = ssl.create_default_context(cafile=certifi.where())
except Exception:
    _SSL = ssl.create_default_context()
_UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) fetch_logs"}
_NAME_CACHE: dict[str, str | None] = {}


def event_name(topic0: str):
    """Resolve a topic0 hash to its event NAME. Tries pins, then the 4byte
    directory, verifying every candidate by re-hashing it (so junk 4byte
    entries can't match). Returns None if unresolved (caller falls back to hex).
    """
    if topic0 in PINNED:
        return PINNED[topic0]
    if topic0 in _NAME_CACHE:
        return _NAME_CACHE[topic0]
    name = None
    try:
        req = urllib.request.Request(
            "https://www.4byte.directory/api/v1/event-signatures/"
            f"?hex_signature={topic0}", headers=_UA)
        with urllib.request.urlopen(req, timeout=15, context=_SSL) as r:
            results = json.load(r).get("results", [])
        for s in sorted(results, key=lambda x: x.get("id", 0)):
            sig = s["text_signature"]
            try:
                if signature_to_topic0(sig) == topic0:  # re-hash to confirm
                    name = sig.split("(")[0]
                    break
            except Exception:
                continue
    except Exception:
        name = None
    _NAME_CACHE[topic0] = name
    return name

LOG_FIELDS = [LogField.BLOCK_NUMBER, LogField.LOG_INDEX, LogField.TRANSACTION_INDEX,
              LogField.TRANSACTION_HASH, LogField.BLOCK_HASH, LogField.ADDRESS,
              LogField.DATA, LogField.TOPIC0, LogField.TOPIC1, LogField.TOPIC2,
              LogField.TOPIC3]
BLOCK_FIELDS = [BlockField.NUMBER, BlockField.TIMESTAMP]

TOPIC0_RE = re.compile(r"^0x[0-9a-fA-F]{1,64}$")


def get_token() -> str:
    tok = os.environ.get("HYPERSYNC_BEARER_TOKEN") or os.environ.get("HYPERSYNC_API_TOKEN")
    if not tok:
        sys.exit("ERROR: no API token. Get a free one at https://envio.dev, then:\n"
                 "    export HYPERSYNC_BEARER_TOKEN=<your token>")
    return tok


async def stream_to_parquet(client, name, query, cfg, stage, tip):
    """Stream the query to staging parquet (logs + blocks) with bounded memory,
    logging progress (current block / tip, rows, rate, ETA) on each batch."""
    logs_f = os.path.join(stage, "logs.parquet")
    blocks_f = os.path.join(stage, "blocks.parquet")
    lw = bw = None
    log_schema = block_schema = None
    rows = 0
    start = query.from_block
    t0 = time.time()
    last = start
    print(f"[{name}] streaming logs from block {start:,} -> tip {tip:,} ...", flush=True)

    recv = await client.stream_arrow(query, cfg)
    while True:
        res = await recv.recv()
        if res is None:                       # stream finished
            break
        d = res.data
        if d.logs is not None and d.logs.num_rows:
            if lw is None:
                log_schema = d.logs.schema
                lw = pq.ParquetWriter(logs_f, log_schema, compression="zstd")
            t = d.logs if d.logs.schema == log_schema else d.logs.cast(log_schema)
            lw.write_table(t)
            rows += t.num_rows
        if d.blocks is not None and d.blocks.num_rows:
            if bw is None:
                block_schema = d.blocks.schema
                bw = pq.ParquetWriter(blocks_f, block_schema, compression="zstd")
            b = d.blocks if d.blocks.schema == block_schema else d.blocks.cast(block_schema)
            bw.write_table(b)

        if res.archive_height:
            tip = max(tip, res.archive_height)
        last = res.next_block if res.next_block is not None else last
        elapsed = max(time.time() - t0, 1e-9)
        done, span = last - start, max(tip - start, 1)
        rate = done / elapsed
        eta = max(tip - last, 0) / rate if rate > 0 else 0
        print(f"[{name}]   {last:,} / {tip:,} blk ({100*done/span:4.1f}%) | "
              f"logs {rows:,} | {rate/1e3:,.0f}k blk/s | ETA {eta:,.0f}s", flush=True)

    if lw:
        lw.close()
    if bw:
        bw.close()
    print(f"[{name}] fetched {rows:,} logs in {time.time()-t0:,.1f}s", flush=True)
    return (logs_f if lw else None), (blocks_f if bw else None)


async def fetch_one(client, name, address, from_block, to_block, tip):
    token_dir = os.path.join(RAW, name)
    stage = os.path.join(token_dir, "_stage")
    shutil.rmtree(stage, ignore_errors=True)
    os.makedirs(stage, exist_ok=True)

    query = Query(
        from_block=from_block,
        to_block=to_block,  # None -> up to tip
        logs=[LogSelection(address=[address])],
        field_selection=FieldSelection(log=LOG_FIELDS, block=BLOCK_FIELDS),
    )
    cfg = StreamConfig(
        hex_output=HexOutput.PREFIXED,
        # Force block number/timestamp to integers (otherwise PREFIXED hex-encodes them).
        column_mapping=ColumnMapping(block={
            BlockField.NUMBER: DataType.INT64,
            BlockField.TIMESTAMP: DataType.INT64,
        }),
    )
    logs_f, blocks_f = await stream_to_parquet(client, name, query, cfg, stage, tip)
    if not logs_f:
        print(f"[{name}] no logs found in range.")
        shutil.rmtree(stage, ignore_errors=True)
        return

    con = duckdb.connect()
    # Clear previous per-event outputs (keep _stage) before rewriting.
    for f in glob.glob(os.path.join(token_dir, "*.parquet")):
        os.remove(f)

    # Attach block_timestamp to every log row (left join logs -> blocks on number).
    if blocks_f:
        ts_expr, join_clause = "b.timestamp", (
            f"LEFT JOIN read_parquet('{blocks_f}') b ON l.block_number = b.number")
    else:
        ts_expr, join_clause = "CAST(NULL AS BIGINT)", ""
    con.execute(f"""
        CREATE TEMP VIEW logs_ts AS
        SELECT l.block_number,
               {ts_expr} AS block_timestamp,
               l.log_index, l.transaction_index, l.transaction_hash, l.block_hash,
               l.address, l.data, l.topic0, l.topic1, l.topic2, l.topic3
        FROM read_parquet('{logs_f}') l
        {join_clause}
    """)

    topics = [r[0] for r in con.execute(
        "SELECT DISTINCT topic0 FROM logs_ts WHERE topic0 IS NOT NULL ORDER BY 1").fetchall()]

    total = 0
    used = {}  # filename base -> topic0, to guard against name collisions
    print(f"[{name}] {len(topics)} event type(s):")
    for t in topics:
        if not TOPIC0_RE.match(t):
            print(f"    skipping unexpected topic0 value: {t!r}")
            continue
        name_ = event_name(t)
        base = name_ or t  # fall back to the topic0 hex if unresolved
        if base in used:   # two events share a name -> disambiguate with hex
            base = f"{base}_{t[2:10]}"
        used[base] = t
        out = os.path.join(token_dir, f"{base}.parquet")
        con.execute(
            f"COPY (SELECT * FROM logs_ts WHERE topic0 = '{t}') "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        n = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
        total += n
        print(f"    {base + '.parquet':28} {name_ or '<unresolved>':16} {t}  {n:>11,} rows")

    con.close()
    shutil.rmtree(stage, ignore_errors=True)
    print(f"[{name}] done: {total:,} logs across {len(topics)} files -> {token_dir}\n")


async def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--test", action="store_true",
                    help="quick run: ~50k most recent blocks only")
    ap.add_argument("--only", choices=list(CONTRACTS), help="fetch a single token")
    args = ap.parse_args()

    token = get_token()
    client = HypersyncClient(ClientConfig(url=ETH_URL, bearer_token=token))
    tip = await client.get_height()
    print(f"chain tip block: {tip:,}")
    from_block = (tip - 50_000) if args.test else 0
    to_block = tip if args.test else None

    os.makedirs(RAW, exist_ok=True)
    targets = {args.only: CONTRACTS[args.only]} if args.only else CONTRACTS
    for name, addr in targets.items():
        await fetch_one(client, name, addr, from_block, to_block, tip)


if __name__ == "__main__":
    asyncio.run(main())
