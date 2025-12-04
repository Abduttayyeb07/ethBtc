// staking-indexer.js
import "dotenv/config";
import fs from "fs";
import fetch from "node-fetch";
import { ethers } from "ethers";
import { createObjectCsvWriter } from "csv-writer";

const CONTRACT = process.env.CONTRACT ?? "0x5BB8F95DC4ADB5C8A3b089CfA60e30ba2E8b3327";
const TOKEN_DECIMALS =
  process.env.TOKEN_DECIMALS !== undefined
    ? parseInt(process.env.TOKEN_DECIMALS, 10)
    : null;
const LOG_CHUNK = process.env.LOG_CHUNK
  ? parseInt(process.env.LOG_CHUNK, 10)
  : 10_000; // blocks per getLogs call
const LOG_CHUNK_BSC = process.env.LOG_CHUNK_BSC
  ? parseInt(process.env.LOG_CHUNK_BSC, 10)
  : null;
const LOG_CHUNK_CAP = process.env.LOG_CHUNK_CAP
  ? parseInt(process.env.LOG_CHUNK_CAP, 10)
  : 900; // max blocks per request safeguard
const FROM_BLOCK = process.env.FROM_BLOCK
  ? parseInt(process.env.FROM_BLOCK, 10)
  : null; // optional global override
const END_BLOCK_ETH = process.env.END_BLOCK_ETH
  ? parseInt(process.env.END_BLOCK_ETH, 10)
  : null;
const END_BLOCK_BSC = process.env.END_BLOCK_BSC
  ? parseInt(process.env.END_BLOCK_BSC, 10)
  : null;
const PROGRESS_FILE = process.env.PROGRESS_FILE ?? "stake_progress.json";
const USE_BSC_TXLIST = process.env.USE_BSC_TXLIST === "1";

const STAKE_TOKEN =
  process.env.STAKE_TOKEN ?? "0xb2617246d0c6c0087f18703d576831899ca94f01";
const STAKE_TOKEN_DECIMALS = process.env.STAKE_TOKEN_DECIMALS
  ? parseInt(process.env.STAKE_TOKEN_DECIMALS, 10)
  : 18;
const STAKE_TOKEN_SYMBOL = process.env.STAKE_TOKEN_SYMBOL ?? "ZIG";
const RUN_ID =
  process.env.RUN_ID ??
  new Date().toISOString().replace(/[:.]/g, "-"); // unique per run to avoid overwrites

const SLEEP = ms => new Promise(res => setTimeout(res, ms));

const networks = [
  {
    name: "ethereum",
    rpc: process.env.ETH_RPC ?? "https://ethereum.publicnode.com",
    apiBase: "https://api.etherscan.io/api",
    apiBaseV2: process.env.ETHERSCAN_API_BASE_V2 ?? "https://api.etherscan.io/v2/api",
    chainId: 1,
    apiKey: process.env.ETHERSCAN_API_KEY
  },
  {
    name: "bsc",
    rpc: process.env.BSC_RPC ?? "https://bsc-dataseed.binance.org",
    apiBase: "https://api.bscscan.com/api",
    apiBaseV2: process.env.ETHERSCAN_API_BASE_V2 ?? "https://api.etherscan.io/v2/api",
    chainId: 56,
    apiKey: process.env.ETHERSCAN_API_KEY
  }
];

// ------------------------
// FETCH ABI FROM EXPLORERS
// ------------------------
async function getAbi(network) {
  console.log(`Fetching ABI from: ${network.name}`);

  const v1 = `${network.apiBase}?module=contract&action=getabi&address=${CONTRACT}&apikey=${network.apiKey}`;
  const v2 =
    network.apiBaseV2 && network.chainId
      ? `${network.apiBaseV2}?chainid=${network.chainId}&module=contract&action=getabi&address=${CONTRACT}&apikey=${network.apiKey}`
      : null;

  const candidates = [v2, v1].filter(Boolean);

  for (const url of candidates) {
    try {
      const resp = await fetch(url);
      const j = await resp.json();

      if (j.status === "1" && j.result) {
        return JSON.parse(j.result);
      }

      console.log("ABI fetch attempt failed:", j);
    } catch (err) {
      console.log("ABI fetch error:", err.message);
    }
  }

  return null;
}

// ------------------------
// SAFE CHUNKED LOG FETCH
// ------------------------
async function fetchLogsChunked(
  provider,
  params,
  networkName,
  progress,
  chunkSize,
  onChunk
) {
  const latest =
    params.toBlock && params.toBlock !== "latest"
      ? Number(params.toBlock)
      : await provider.getBlockNumber();

  const start = params.fromBlock ? Number(params.fromBlock) : 0;
  const logs = [];

  if (!progress[networkName]) progress[networkName] = {};

  // Cap chunk size to avoid RPC limits (e.g., some BSC RPCs allow max 1000 blocks)
  const size = Math.max(1, Math.min(chunkSize ?? LOG_CHUNK, LOG_CHUNK_CAP));

  for (let from = start; from <= latest; from += size + 1) {
    const to = Math.min(from + size, latest);
    const range = { ...params, fromBlock: from, toBlock: to };

    let chunk = [];
    let attempt = 0;
    const maxAttempts = 4;

    while (attempt < maxAttempts) {
      try {
        chunk = await provider.getLogs(range);
        break;
      } catch (err) {
        attempt += 1;
        const msg = err?.error?.message || err?.message || "";
        const isRateLimit = msg.toLowerCase().includes("rate limit");
        if (isRateLimit && attempt < maxAttempts) {
          const delay = 1000 * attempt;
          console.log(
            `  rate limited on ${from}-${to}; retrying in ${delay}ms (attempt ${attempt}/${maxAttempts - 1})`
          );
          await SLEEP(delay);
          continue;
        }
        throw err;
      }
    }

    logs.push(...chunk);
    const prev = progress[networkName].lastBlock ?? -1;
    progress[networkName].lastBlock = Math.max(prev, to);
    saveProgress(progress);

    if (onChunk) {
      await onChunk(chunk);
    }

    console.log(
      `  fetched logs ${from}-${to} (${chunk.length} entries)`
    );
  }

  return logs;
}

// ------------------------
// BSC TXLIST (skip empty blocks)
// ------------------------
async function fetchBscTxList(fromBlock, toBlock) {
  const url = `https://api.etherscan.io/v2/api?chainid=56&module=account&action=txlist&address=${CONTRACT}&startblock=${fromBlock}&endblock=${toBlock ?? "99999999"}&sort=asc&apikey=${process.env.ETHERSCAN_API_KEY}`;
  const resp = await fetch(url);
  const j = await resp.json();
  if (j.status !== "1") {
    throw new Error(`bsc txlist failed: ${JSON.stringify(j)}`);
  }
  return j.result || [];
}

async function scanBscByTxList(network, abi, ctx) {
  console.log(`\n🔍 Scanning ${network.name} via txlist...`);
  const provider = new ethers.JsonRpcProvider(network.rpc);
  const iface = new ethers.Interface(abi);
  network.provider = provider;

  const progress = loadProgress();
  const resumeBlock =
    progress[network.name]?.lastBlock !== undefined
      ? progress[network.name].lastBlock + 1
      : null;

  const fromBlockExplicit =
    (process.env.FROM_BLOCK_BSC
      ? parseInt(process.env.FROM_BLOCK_BSC, 10)
      : null) ||
    (FROM_BLOCK !== null ? FROM_BLOCK : null);

  const fromBlock = resumeBlock ?? fromBlockExplicit ?? 0;
  const toBlock = END_BLOCK_BSC ?? "latest";

  console.log(`  fetching txlist from ${fromBlock} to ${toBlock}`);
  const txs = await fetchBscTxList(fromBlock, toBlock);
  console.log(`  txlist returned ${txs.length} txs`);

  let processed = 0;
  let lastBlock = fromBlock - 1;

  for (const tx of txs) {
    const txBlock = Number(tx.blockNumber);
    if (resumeBlock && txBlock < resumeBlock) continue;

    const receipt = await provider.getTransactionReceipt(tx.hash);
    lastBlock = txBlock;

    for (const log of receipt.logs) {
      if (log.address.toLowerCase() !== CONTRACT.toLowerCase()) continue;
      try {
        const event = iface.parseLog(log);
        await ctx.processEvent(network, event, log);
      } catch (_) {}
    }

    processed += 1;
    if (processed % 25 === 0) {
      progress[network.name] = { lastBlock };
      saveProgress(progress);
      await ctx.flushPartial();
      console.log(`  processed ${processed}/${txs.length} txs (up to block ${lastBlock})`);
    }
  }

  progress[network.name] = { lastBlock };
  saveProgress(progress);
  await ctx.flushPartial();

  console.log(`Finished BSC txlist scan: ${processed} txs processed`);
}

// ------------------------
// SCAN LOGS AND DECODE
// ------------------------
async function scanNetwork(network, abi, ctx) {
  console.log(`\n🔍 Scanning ${network.name}...`);

  const provider = new ethers.JsonRpcProvider(network.rpc);
  const iface = new ethers.Interface(abi);
  // attach provider so ctx can use it (tx sender lookups)
  network.provider = provider;

  // select start block: per-network override > global > 0
  const fromBlockExplicit =
    (network.name === "ethereum" && process.env.FROM_BLOCK_ETH
      ? parseInt(process.env.FROM_BLOCK_ETH, 10)
      : null) ||
    (network.name === "bsc" && process.env.FROM_BLOCK_BSC
      ? parseInt(process.env.FROM_BLOCK_BSC, 10)
      : null) ||
    (FROM_BLOCK !== null ? FROM_BLOCK : null);

  const toBlockExplicit =
    (network.name === "ethereum" && END_BLOCK_ETH !== null
      ? END_BLOCK_ETH
      : null) ||
    (network.name === "bsc" && END_BLOCK_BSC !== null ? END_BLOCK_BSC : null);

  const progress = loadProgress();
  const resumeBlock =
    progress[network.name]?.lastBlock !== undefined
      ? progress[network.name].lastBlock + 1
      : null;

  // Prefer resuming progress; fall back to explicit env or 0
  const fromBlock = resumeBlock ?? fromBlockExplicit ?? 0;
  const toBlock = toBlockExplicit ?? "latest";

  let decodedCount = 0;
  let rawCount = 0;

  await fetchLogsChunked(
    provider,
    {
      address: CONTRACT,
      fromBlock,
      toBlock
    },
    network.name,
    progress,
    network.name === "bsc" && LOG_CHUNK_BSC ? LOG_CHUNK_BSC : LOG_CHUNK,
    async chunk => {
      rawCount += chunk.length;
      for (const log of chunk) {
        try {
          const event = iface.parseLog(log);
          decodedCount += 1;
          await ctx.processEvent(network, event, log);
        } catch (_) {}
      }
      await ctx.flushPartial();
    }
  );

  console.log(`Found ${rawCount} raw logs on ${network.name}`);
  console.log(`Parsed ${decodedCount} valid events on ${network.name}`);
}

function formatAmount(amount) {
  if (TOKEN_DECIMALS === null || Number.isNaN(TOKEN_DECIMALS)) {
    return amount.toString();
  }

  // ethers.formatUnits expects BigInt or string
  return ethers.formatUnits(amount, TOKEN_DECIMALS);
}

function loadProgress() {
  try {
    const raw = fs.readFileSync(PROGRESS_FILE, "utf8");
    return JSON.parse(raw);
  } catch (_) {
    return {};
  }
}

function saveProgress(progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

async function writeOutputs(
  ledger,
  eventStats,
  addressTotals,
  txRecords,
  final = false
) {
  // event stats
  fs.writeFileSync("event_stats.json", JSON.stringify(eventStats, null, 2));
  // per-network combined outputs (events + summary) in two CSVs
  const nets = ["ethereum", "bsc"];
  for (const net of nets) {
    const events = txRecords.filter(r => r.network === net);
    const summaryRows = [];
    const ledgerNet = ledger[net] || {};
    const totalsNet = addressTotals[net] || {};
    for (const user in totalsNet) {
      const st = totalsNet[user].staked ?? 0n;
      const un = totalsNet[user].unstaked ?? 0n;
      summaryRows.push({
        type: "summary",
        network: net,
        address: user,
        txHash: "",
        blockNumber: "",
        timestamp: "",
        method: "",
        from_event: "",
        from_tx: "",
        to: "",
        token: STAKE_TOKEN_SYMBOL,
        amount_raw: "",
        amount_formatted: "",
        total_staked_raw: st.toString(),
        total_unstaked_raw: un.toString(),
        net_raw: (st - un).toString(),
        total_staked: formatAmount(st),
        total_unstaked: formatAmount(un),
        net: formatAmount(st - un)
      });
    }

    const combined = [
      ...events.map(e => ({
        type: "event",
        network: e.network,
        address: e.from_event,
        txHash: e.txHash,
        blockNumber: e.blockNumber,
        timestamp: e.timestamp ?? "",
        method: e.method,
        from_event: e.from_event,
        from_tx: e.from_tx ?? "",
        to: e.to,
        token: e.token,
        amount_raw: e.amount_raw,
        amount_formatted: e.amount_formatted,
        total_staked_raw: "",
        total_unstaked_raw: "",
        net_raw: "",
        total_staked: "",
        total_unstaked: "",
        net: ""
      })),
      ...summaryRows
    ];

    fs.writeFileSync(`${net}_output_${RUN_ID}.json`, JSON.stringify(combined, null, 2));
    const csvWriter = createObjectCsvWriter({
      path: `${net}_output_${RUN_ID}.csv`,
      header: [
        { id: "type", title: "type" },
        { id: "network", title: "network" },
        { id: "address", title: "address" },
        { id: "txHash", title: "txHash" },
        { id: "blockNumber", title: "blockNumber" },
        { id: "timestamp", title: "timestamp" },
        { id: "method", title: "method" },
        { id: "from_event", title: "from_event" },
        { id: "from_tx", title: "from_tx" },
        { id: "to", title: "to" },
        { id: "token", title: "token" },
        { id: "amount_raw", title: "amount_raw" },
        { id: "amount_formatted", title: "amount_formatted" },
        { id: "total_staked_raw", title: "total_staked_raw" },
        { id: "total_unstaked_raw", title: "total_unstaked_raw" },
        { id: "net_raw", title: "net_raw" },
        { id: "total_staked", title: "total_staked" },
        { id: "total_unstaked", title: "total_unstaked" },
        { id: "net", title: "net" }
      ]
    });
    await csvWriter.writeRecords(combined);
  }

  if (final) console.log("\n📁 JSON/CSV outputs refreshed (final)");
}

// ------------------------
// MAIN LOGIC — LEDGER
// ------------------------
async function main() {
  const ledger = { ethereum: {}, bsc: {} }; // per network user -> stake balance (BigInt)
  const eventStats = {}; // name -> { count, sampleArgs }
  const addressTotals = { ethereum: {}, bsc: {} }; // per network user -> { staked, unstaked }
  const txRecords = []; // mixed networks
  const txSenderCache = new Map(); // txHash -> from
  const blockTimeCache = new Map(); // `${network}-${blockNumber}` -> timestamp

  // optional: load ABI from local file if provided (avoids network fetch flakiness)
  const abiOverridePath = process.env.ABI_FILE;
  let abiOverride = null;
  if (abiOverridePath && fs.existsSync(abiOverridePath)) {
    const raw = fs.readFileSync(abiOverridePath, "utf8");
    abiOverride = JSON.parse(raw);
    console.log(`Using ABI from local file: ${abiOverridePath}`);
  }

  const ctx = {
    async processEvent(net, event, log) {
      const name = event.name.toLowerCase();
      const args = event.args;

      if (!eventStats[name]) {
        eventStats[name] = { count: 0, sampleArgs: Object.keys(args ?? {}) };
      }
      eventStats[name].count += 1;

      const user =
        args.user ||
        args.account ||
        args.from ||
        args.wallet ||
        args.owner ||
        args._owner ||
        args._user;

      const amount =
        args.amount ||
        args.value ||
        args._value ||
        args.tokens ||
        args._amount;

      if (!user || amount === undefined) {
        return;
      }

      const userAddr = user.toLowerCase();
      const amountValue = ethers.getBigInt(amount);

      if (!ledger[net.name][userAddr]) {
        ledger[net.name][userAddr] = 0n;
      }
      if (!addressTotals[net.name][userAddr]) {
        addressTotals[net.name][userAddr] = { staked: 0n, unstaked: 0n };
      }

      // fetch tx sender (caller) with simple cache
      let txFrom = txSenderCache.get(log.transactionHash);
      if (txFrom === undefined) {
        const tx = await net.provider.getTransaction(log.transactionHash);
        txFrom = tx?.from ? tx.from.toLowerCase() : null;
        txSenderCache.set(log.transactionHash, txFrom);
      }

      // fetch block timestamp with cache
      const blockKey = `${net.name}-${log.blockNumber}`;
      let ts = blockTimeCache.get(blockKey);
      if (ts === undefined) {
        const blk = await net.provider.getBlock(log.blockNumber);
        ts = blk?.timestamp ? Number(blk.timestamp) : null;
        blockTimeCache.set(blockKey, ts);
      }

      const method =
        name.includes("withdraw") || name.includes("unstake")
          ? "withdraw"
          : name.includes("stake") || name.includes("deposit")
          ? "stake"
          : "other";

      if (method === "stake") {
        ledger[net.name][userAddr] += amountValue;
        addressTotals[net.name][userAddr].staked += amountValue;
      } else if (method === "withdraw") {
        ledger[net.name][userAddr] -= amountValue;
        addressTotals[net.name][userAddr].unstaked += amountValue;
      }

      txRecords.push({
        network: net.name,
        txHash: log.transactionHash,
        blockNumber: Number(log.blockNumber),
        timestamp: ts,
        method: event.name,
        from_event: userAddr,
        from_tx: txFrom,
        to: CONTRACT.toLowerCase(),
        token: STAKE_TOKEN_SYMBOL,
        amount_raw: amountValue.toString(),
        amount_formatted: formatAmount(amountValue)
      });
    },
    async flushPartial() {
      await writeOutputs(ledger, eventStats, addressTotals, txRecords);
    }
  };

  for (const net of networks) {
    const abi = abiOverride || (await getAbi(net));
    if (!abi) continue;

    if (net.name === "bsc" && USE_BSC_TXLIST) {
      try {
        await scanBscByTxList(net, abi, ctx);
      } catch (err) {
        console.log(`BSC txlist failed (${err.message}); falling back to getLogs`);
        await scanNetwork(net, abi, ctx);
      }
    } else {
      await scanNetwork(net, abi, ctx);
    }
  }

  // ------------------------
  // Final flush
  // ------------------------
  await writeOutputs(ledger, eventStats, addressTotals, txRecords, true);
}

// RUN
main();