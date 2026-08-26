import { readFileSync, writeFileSync, existsSync, mkdirSync, renameSync } from 'fs';
import { gzipSync, gunzipSync } from 'zlib';
import path from 'path';
import { createClient, type Client } from '@libsql/client';
import { config } from './config.js';

// ── Turso DB Sync (Optional Cloud Persistence) ────────────────────────────────
const tursoUrl = process.env.TURSO_DATABASE_URL?.trim();
const tursoAuthToken = process.env.TURSO_AUTH_TOKEN?.trim();

let _tursoClient: Client | null = null;
if (tursoUrl) {
  try {
    _tursoClient = createClient({
      url: tursoUrl,
      authToken: tursoAuthToken || undefined,
    });
  } catch (err) {
    console.warn('[Turso] Failed to initialize client:', err);
  }
}

function syncToTurso(key: string, dataObj: unknown): void {
  if (!_tursoClient) return;
  const dataStr = typeof dataObj === 'string' ? dataObj : JSON.stringify(dataObj);
  _tursoClient.execute({
    sql: 'INSERT INTO zalo_storage (key, data, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at',
    args: [key, dataStr, Date.now()],
  }).catch((err: unknown) => {
    console.warn(`[Turso] Sync error for "${key}":`, err);
  });
}

async function initTurso(): Promise<void> {
  if (!_tursoClient) return;
  try {
    await _tursoClient.execute(`
      CREATE TABLE IF NOT EXISTS zalo_storage (
        key TEXT PRIMARY KEY,
        data TEXT,
        updated_at INTEGER
      )
    `);
    console.log('[Turso] Database connected and table zalo_storage initialized');

    const res = await _tursoClient.execute('SELECT key, data FROM zalo_storage');
    for (const row of res.rows) {
      const key = String(row.key);
      const dataStr = String(row.data);

      if (key === 'topics') {
        try {
          const parsed = JSON.parse(dataStr) as StoreData;
          if (parsed && typeof parsed.topics === 'object') {
            let changed = false;
            for (const [tId, entry] of Object.entries(parsed.topics)) {
              if (!_data.topics[tId]) {
                _data.topics[tId] = entry;
                _data.zaloIndex[`${entry.type}:${entry.zaloId}`] = entry.topicId;
                changed = true;
              }
            }
            if (changed) {
              persist(_data);
              console.log(`[Turso] Restored ${Object.keys(parsed.topics).length} topic mapping(s) from Turso`);
            }
          }
        } catch { /* ignore parse error */ }
      } else if (key === 'user_cache' && _uidToName.size === 0) {
        try {
          const parsed = JSON.parse(dataStr) as UserCacheDisk;
          for (const [uid, name] of Object.entries(parsed.u ?? {})) {
            _uidToName.set(uid, name);
            _normToUid.set(_normName(name), uid);
          }
          for (const [gid, members] of Object.entries(parsed.g ?? {})) {
            const m = new Map<string, string>();
            for (const [norm, uid] of Object.entries(members)) m.set(norm, uid);
            _groupNameToUid.set(gid, m);
          }
          for (const [gid, members] of Object.entries(parsed.gn ?? {})) {
            _groupUidToName.set(gid, new Map(Object.entries(members)));
          }
          console.log(`[Turso] Restored ${_uidToName.size} user cache entries from Turso`);
        } catch { /* ignore */ }
      } else if (key === 'polls' && _pollByZaloId.size === 0) {
        try {
          const parsed = JSON.parse(dataStr) as { entries: PollEntry[] };
          for (const entry of parsed.entries ?? []) {
            _pollByZaloId.set(entry.pollId, entry);
            _pollByTgId.set(entry.tgPollMsgId, entry);
            _pollByUUID.set(entry.tgPollUUID, entry);
          }
          console.log(`[Turso] Restored ${_pollByZaloId.size} polls from Turso`);
        } catch { /* ignore */ }
      } else if (key === 'msg_map' && _zaloToTg.size === 0) {
        try {
          const raw = JSON.parse(dataStr) as MsgMapFile;
          const parsed = _parseMsgMapRaw(raw);
          for (const [zaloId, tgId] of parsed.pairs) {
            if (!_zaloToTg.has(zaloId)) {
              _msgKeyOrder.push(zaloId);
              _tgRefCount.set(tgId, (_tgRefCount.get(tgId) ?? 0) + 1);
            }
            _zaloToTg.set(zaloId, tgId);
          }
          for (const [tgId, quote] of parsed.quotes) {
            _tgToQuote.set(tgId, quote);
          }
          console.log(`[Turso] Restored ${_zaloToTg.size} msg mappings & ${_tgToQuote.size} quotes from Turso`);
        } catch { /* ignore */ }
      } else if (key === 'sent_msg' && _sentMap.size === 0) {
        try {
          const parsed = JSON.parse(dataStr) as { entries: [number, SentMsgInfo][] };
          for (const [tgId, info] of parsed.entries ?? []) {
            _sentMap.set(tgId, info);
            _sentKeyOrder.push(tgId);
            for (const mid of info.msgIds) {
              _sentByZaloId.set(String(mid), tgId);
            }
          }
          console.log(`[Turso] Restored ${_sentMap.size} sent messages from Turso`);
        } catch { /* ignore */ }
      }
    }

    // If local has topics but Turso doesn't yet, sync local topics up
    if (Object.keys(_data.topics).length > 0) {
      syncToTurso('topics', _data);
    }
  } catch (err) {
    console.warn('[Turso] Init/sync error:', err);
  }
}

void initTurso();

/**
 * Track Zalo message IDs recently recalled from Telegram side.
 * The undo event handler checks this to avoid sending duplicate
 * "🗑 đã thu hồi" notifications for recalls we initiated ourselves.
 */
export const recentlyRecalledMsgIds = new Set<string>();
export function markRecalled(msgId: string): void {
  recentlyRecalledMsgIds.add(msgId);
  setTimeout(() => recentlyRecalledMsgIds.delete(msgId), 5_000);
}

/**
 * Track Zalo message IDs whose recall was already notified to Telegram.
 *
 * After a reconnect, the catch-up sync (requestOldMessages) replays undo
 * events that were already delivered live — without this dedupe the same
 * "🗑 đã thu hồi" notice is sent again (issue #65). A Zalo message can only
 * be recalled once, so entries never expire; the set is bounded FIFO.
 */
const RECALL_NOTIFIED_MAX = 20_000;
const _recallNotifiedMsgIds = new Set<string>();
export const recallNotifiedStore = {
  /** Returns true (and marks) if this recall was NOT notified before. */
  markIfFirst(msgId: string): boolean {
    if (_recallNotifiedMsgIds.has(msgId)) return false;
    if (_recallNotifiedMsgIds.size >= RECALL_NOTIFIED_MAX) {
      const oldest = _recallNotifiedMsgIds.values().next().value as string | undefined;
      if (oldest !== undefined) _recallNotifiedMsgIds.delete(oldest);
    }
    _recallNotifiedMsgIds.add(msgId);
    return true;
  },
  /** Unmark (e.g. when the notification send failed and may be retried). */
  unmark(msgId: string): void {
    _recallNotifiedMsgIds.delete(msgId);
  },
  stats(): { entries: number } {
    return { entries: _recallNotifiedMsgIds.size };
  },
};

// ── Types ─────────────────────────────────────────────────────────────────────

export interface TopicEntry {
  topicId: number;
  zaloId:  string;   // threadId (UID for DMs, groupId for groups)
  type:    0 | 1;    // 0 = ThreadType.User, 1 = ThreadType.Group
  name:    string;   // contact name or group name
}

interface StoreData {
  /** topicId (as string key) → entry */
  topics:    Record<string, TopicEntry>;
  /** `${type}:${zaloId}` → topicId */
  zaloIndex: Record<string, number>;
}

// ── Internal ──────────────────────────────────────────────────────────────────

const filePath = path.resolve(config.dataDir, 'topics.json');

function load(): StoreData {
  if (!existsSync(filePath)) return { topics: {}, zaloIndex: {} };
  try {
    return JSON.parse(readFileSync(filePath, 'utf8')) as StoreData;
  } catch {
    return { topics: {}, zaloIndex: {} };
  }
}

function persist(data: StoreData): void {
  mkdirSync(path.dirname(filePath), { recursive: true });
  const tmpPath = filePath + '.tmp';
  writeFileSync(tmpPath, JSON.stringify(data, null, 2), 'utf8');
  renameSync(tmpPath, filePath);
  syncToTurso('topics', data);
}

function zaloKey(zaloId: string, type: 0 | 1): string {
  return `${type}:${zaloId}`;
}

// ── Public API ────────────────────────────────────────────────────────────────

let _data: StoreData = load();

export const store = {
  /** Find an existing Telegram topic ID for a given Zalo conversation. */
  getTopicByZalo(zaloId: string, type: 0 | 1): number | undefined {
    return _data.zaloIndex[zaloKey(zaloId, type)];
  },

  /** Look up the Zalo conversation linked to a Telegram topic. */
  getEntryByTopic(topicId: number): TopicEntry | undefined {
    return _data.topics[String(topicId)];
  },

  /** Persist a new topic ↔ Zalo mapping. */
  set(entry: TopicEntry): void {
    // Keep the two indexes strictly one-to-one. Reusing a Telegram topic for a
    // different Zalo thread must remove the old reverse lookup, and remapping a
    // Zalo thread to a new topic must remove the stale topic entry.
    const previousAtTopic = _data.topics[String(entry.topicId)];
    if (previousAtTopic) {
      const previousKey = zaloKey(previousAtTopic.zaloId, previousAtTopic.type);
      if (previousKey !== zaloKey(entry.zaloId, entry.type)
        && _data.zaloIndex[previousKey] === entry.topicId) {
        delete _data.zaloIndex[previousKey];
      }
    }

    const key = zaloKey(entry.zaloId, entry.type);
    const previousTopicId = _data.zaloIndex[key];
    if (previousTopicId !== undefined && previousTopicId !== entry.topicId) {
      delete _data.topics[String(previousTopicId)];
    }

    _data.topics[String(entry.topicId)] = entry;
    _data.zaloIndex[key] = entry.topicId;
    persist(_data);
  },

  /** Update the stored display name for an existing topic mapping. */
  updateName(topicId: number, name: string): void {
    const entry = _data.topics[String(topicId)];
    if (!entry || entry.name === name) return;
    entry.name = name;
    persist(_data);
  },

  /** All entries (for diagnostics). */
  all(): TopicEntry[] {
    return Object.values(_data.topics);
  },

  /** Remove a mapping by Telegram topicId. Returns the removed entry or undefined. */
  remove(topicId: number): TopicEntry | undefined {
    const entry = _data.topics[String(topicId)];
    if (!entry) return undefined;
    delete _data.topics[String(topicId)];
    const key = zaloKey(entry.zaloId, entry.type);
    if (_data.zaloIndex[key] === topicId) {
      delete _data.zaloIndex[key];
    }
    persist(_data);
    return entry;
  },

  /** Re-read from disk (useful after external edits). */
  reload(): void {
    _data = load();
  },

  stats(): { topics: number; sizeBytes: number } {
    const raw = JSON.stringify(_data);
    return { topics: Object.keys(_data.topics).length, sizeBytes: raw.length };
  },
};

// ── Message ID mapping (in-memory, not persisted) ─────────────────────────────

/**
 * Data needed to quote a Zalo message when replying.
 * Field names match what zca-js sendMessage reads from the `quote` param.
 */
export interface ZaloQuoteData {
  msgId:    string;
  cliMsgId: string;
  uidFrom:  string;
  ts:       string;
  msgType:  string;
  content:  string | Record<string, unknown>;
  ttl:      number;
  /** The Zalo conversation ID (group ID or peer UID) this message belongs to. */
  zaloId:   string;
  /** 0 = DM, 1 = Group */
  threadType: 0 | 1;
}

const MSG_CACHE_MAX = 10000;

// ── Persistence helpers for msgStore ─────────────────────────────────────────
//
// On-disk format v2 (compact):
//   {
//     "v": 2,
//     "s": [str0, str1, ...],          ← string intern table
//     "p": [[zaloMsgId, tgMsgId], ...] ← pairs (same as v1)
//     "q": [[tgMsgId, msgId, cliMsgId, uidFromIdx, ts, msgTypeIdx,
//             content, ttl, zaloIdIdx, threadType], ...]
//   }
//   *Idx values are integer indices into s[].
//   Saves ~40-60% vs v1 by eliminating repeated field names and interning
//   zaloId / uidFrom / msgType strings (high repetition across entries).
//   Backward-compatible: v1 files without "v" field load fine.

interface MsgMapV1 {
  pairs:  [string, number][];
  quotes: [number, ZaloQuoteData][];
}
interface MsgMapV2 {
  v: 2;
  s: string[];
  p: [string, number][];
  q: [number, string, string, number, string, number, string | Record<string, unknown>, number, number, 0 | 1][];
}
type MsgMapFile = MsgMapV1 | MsgMapV2;

interface MsgMapData {
  pairs:  [string, number][];
  quotes: [number, ZaloQuoteData][];
}

const _msgMapFile = path.resolve(config.dataDir, 'msg-map.json');

function _parseMsgMapRaw(raw: MsgMapFile): MsgMapData {
  // v2 compact format
  if ('v' in raw && raw.v === 2) {
    const { s, p, q } = raw;
    // Filter out sentinel "0" / empty pairs (came from undefined realMsgId)
    const pairs = p.filter(([k]) => k && k !== '0');
    const quotes: [number, ZaloQuoteData][] = q.map(
      ([tgId, msgId, cliMsgId, uidIdx, ts, typeIdx, content, ttl, zaloIdx, threadType]) => [
        tgId,
        {
          msgId,
          cliMsgId,
          uidFrom:    s[uidIdx]!,
          ts,
          msgType:    s[typeIdx]!,
          content,
          ttl,
          zaloId:     s[zaloIdx]!,
          threadType,
        } satisfies ZaloQuoteData,
      ],
    );
    return { pairs, quotes };
  }
  // v1 legacy format — also filter zeros
  const v1 = raw as MsgMapData;
  return { pairs: v1.pairs.filter(([k]) => k && k !== '0'), quotes: v1.quotes };
}

function _loadMsgMap(): MsgMapData {
  if (!existsSync(_msgMapFile)) return { pairs: [], quotes: [] };
  try {
    let buf = readFileSync(_msgMapFile);
    // Detect gzip by magic bytes 0x1F 0x8B
    if (buf[0] === 0x1F && buf[1] === 0x8B) buf = gunzipSync(buf);
    const raw = JSON.parse(buf.toString('utf8')) as MsgMapFile;
    return _parseMsgMapRaw(raw);
  } catch { return { pairs: [], quotes: [] }; }
}

let _msgPersistTimer: ReturnType<typeof setTimeout> | null = null;
function _scheduleMsgPersist(): void {
  if (_msgPersistTimer) return;
  _msgPersistTimer = setTimeout(() => {
    _msgPersistTimer = null;
    try {
      mkdirSync(path.dirname(_msgMapFile), { recursive: true });

      // Build string intern table: collect all zaloId, uidFrom, msgType values
      const _internMap = new Map<string, number>();
      const _intern: string[] = [];
      const _idx = (s: string): number => {
        let i = _internMap.get(s);
        if (i === undefined) { i = _intern.length; _internMap.set(s, i); _intern.push(s); }
        return i;
      };

      const q: MsgMapV2['q'] = [];
      for (const [tgId, qt] of _tgToQuote) {
        q.push([
          tgId,
          qt.msgId,
          qt.cliMsgId,
          _idx(qt.uidFrom),
          qt.ts,
          _idx(qt.msgType),
          qt.content,
          qt.ttl,
          _idx(qt.zaloId),
          qt.threadType,
        ]);
      }

      const data: MsgMapV2 = {
        v: 2,
        s: _intern,
        // Skip sentinel "0" / empty keys — they carry no useful information
        p: _msgKeyOrder.filter(k => k && k !== '0').map(k => [k, _zaloToTg.get(k)!] as [string, number]),
        q,
      };
      // gzip the JSON — reduces file size ~70% with zero new deps
      const _tmpMsg = _msgMapFile + '.tmp';
      writeFileSync(_tmpMsg, gzipSync(JSON.stringify(data), { level: 9 }));
      renameSync(_tmpMsg, _msgMapFile);
      syncToTurso('msg_map', data);
    } catch (e) {
      console.warn('[msgStore] Failed to persist msg-map:', e);
    }
  }, 1000);
}

// ── In-memory state (pre-loaded from disk) ────────────────────────────────────

/** zaloMsgId → Telegram message_id (used to find TG reply target) */
const _zaloToTg = new Map<string, number>();
/** Telegram message_id → Zalo quote data (used when TG user replies) */
const _tgToQuote = new Map<number, ZaloQuoteData>();
/** Insertion-order keys for eviction */
const _msgKeyOrder: string[] = [];
/** Số lượng zaloMsgId trỏ đến mỗi tgMsgId (để tránh xoá quote sớm) */
const _tgRefCount = new Map<number, number>();

function _evictOne(): void {
  const old = _msgKeyOrder.shift();
  if (!old) return;
  const oldTg = _zaloToTg.get(old);
  _zaloToTg.delete(old);
  if (oldTg !== undefined) {
    const remaining = (_tgRefCount.get(oldTg) ?? 1) - 1;
    if (remaining <= 0) {
      _tgRefCount.delete(oldTg);
      _tgToQuote.delete(oldTg);
    } else {
      _tgRefCount.set(oldTg, remaining);
    }
  }
}

// Load persisted data immediately
{
  const saved = _loadMsgMap();
  for (const [zaloId, tgId] of saved.pairs) {
    if (!_zaloToTg.has(zaloId)) {
      _msgKeyOrder.push(zaloId);
      _tgRefCount.set(tgId, (_tgRefCount.get(tgId) ?? 0) + 1);
    }
    _zaloToTg.set(zaloId, tgId);
  }
  for (const [tgId, quote] of saved.quotes) {
    _tgToQuote.set(tgId, quote);
  }
  // Trim if over limit (file may have grown beyond MSG_CACHE_MAX)
  while (_msgKeyOrder.length > MSG_CACHE_MAX) _evictOne();
}

export const msgStore = {
  /**
   * Save a bidirectional mapping after a Zalo message is forwarded to Telegram.
   * @param tgMsgId      The Telegram message_id of the forwarded message.
   * @param zaloMsgIds   One or more Zalo IDs (msgId, realMsgId) that refer to the same message.
   * @param quote        Data needed to quote this message in future sends.
   */
  save(tgMsgId: number, zaloMsgIds: string[], quote: ZaloQuoteData): void {
    // Drop sentinel "0" and empty IDs — they are realMsgId=0 placeholders,
    // nobody ever queries getTgMsgId("0") so storing them is pure waste.
    const validIds = Array.from(new Set(zaloMsgIds.filter(id => id && id !== '0')));
    const newIdCount = validIds.reduce((count, id) => count + (_zaloToTg.has(id) ? 0 : 1), 0);
    while (_msgKeyOrder.length + newIdCount > MSG_CACHE_MAX) _evictOne();
    for (const id of validIds) {
      const previousTgId = _zaloToTg.get(id);
      if (previousTgId === undefined) {
        _tgRefCount.set(tgMsgId, (_tgRefCount.get(tgMsgId) ?? 0) + 1);
        _msgKeyOrder.push(id);
      } else if (previousTgId !== tgMsgId) {
        const previousRemaining = (_tgRefCount.get(previousTgId) ?? 1) - 1;
        if (previousRemaining <= 0) {
          _tgRefCount.delete(previousTgId);
          _tgToQuote.delete(previousTgId);
        } else {
          _tgRefCount.set(previousTgId, previousRemaining);
        }
        _tgRefCount.set(tgMsgId, (_tgRefCount.get(tgMsgId) ?? 0) + 1);
      }
      _zaloToTg.set(id, tgMsgId);
    }
    _tgToQuote.set(tgMsgId, quote);
    _scheduleMsgPersist();
  },

  /** Get the Telegram message_id for a given Zalo message ID. */
  getTgMsgId(zaloMsgId: string): number | undefined {
    return _zaloToTg.get(zaloMsgId);
  },

  /** Get the Zalo quote data for a given Telegram message_id (for TG→Zalo replies). */
  getQuote(tgMsgId: number): ZaloQuoteData | undefined {
    return _tgToQuote.get(tgMsgId);
  },

  /**
   * Patch quote metadata from a self-echo message.
   * This is crucial for TG→Zalo media messages: initial placeholder quote data
   * (msgType=webchat, content='[Voice]'...) is replaced with the real Zalo
   * payload so later replies render native quote previews in Zalo.
   */
  updateQuoteFromEcho(
    tgMsgId: number,
    patch: Partial<Pick<ZaloQuoteData, 'msgId' | 'cliMsgId' | 'msgType' | 'content' | 'ts' | 'ttl'>>,
  ): void {
    const quote = _tgToQuote.get(tgMsgId);
    if (!quote) return;
    Object.assign(quote, patch);
    _scheduleMsgPersist();
  },

  /**
   * Update the cliMsgId on an existing quote entry.
   * Used when the Zalo echo event provides the real cliMsgId after a TG→Zalo send.
   */
  updateQuoteCliMsgId(tgMsgId: number, cliMsgId: string): void {
    const quote = _tgToQuote.get(tgMsgId);
    if (quote) {
      quote.cliMsgId = cliMsgId;
      _scheduleMsgPersist();
    }
  },

  stats(): { cacheSize: number; keyOrderLen: number; quoteCount: number } {
    return { cacheSize: _zaloToTg.size, keyOrderLen: _msgKeyOrder.length, quoteCount: _tgToQuote.size };
  },
};

// ── User cache (persisted to disk, gzip compact) ──────────────────────────────
//
// On-disk format (user-cache.json.gz):
//   { "u": {"uid":"name",...}, "g": {"groupId":{"normName":"uid",...},...} }
//
// Techniques for minimum file size + maximum read speed:
//   • Flat objects (no per-entry field names) → uid/name stored once
//   • normName pre-computed at write → O(1) Map lookup at read, no re-normalize
//   • gzip level 9 → ~70% smaller (Vietnamese names compress extremely well)
//   • Debounced write (2 s) → batches rapid saves into one write
//   • In-memory Maps → all gets are O(1), disk only read on startup

/**
 * Lightweight cache of Zalo uid ↔ display name.
 * Populated automatically as messages arrive; used to resolve TG @mention text
 * back to a Zalo UID when forwarding TG → Zalo.
 */
const USER_CACHE_MAX  = 5000;
const _uidToName      = new Map<string, string>();
const _normToUid      = new Map<string, string>();
/** zaloId → (normalizedName → uid) — collision-safe per-group lookup */
const _groupNameToUid = new Map<string, Map<string, string>>();
/** zaloId → (uid → exact display name) — prevents group names polluting global contact names */
const _groupUidToName = new Map<string, Map<string, string>>();

function _normName(name: string): string {
  return name
    .toLowerCase()
    .normalize('NFD')
    .replace(/\p{Mn}/gu, '')
    .replace(/đ/g, 'd')
    .replace(/\s+/g, ' ')
    .trim();
}

// ── Persistence helpers ───────────────────────────────────────────────────────

const _userCacheFile = path.resolve(config.dataDir, 'user-cache.json.gz');

interface UserCacheDisk {
  /** uid → displayName */
  u: Record<string, string>;
  /** groupId → { normName → uid } */
  g: Record<string, Record<string, string>>;
  /** groupId → { uid → exact displayName } (added in v3; optional for migration) */
  gn?: Record<string, Record<string, string>>;
}

function _loadUserCache(): void {
  if (!existsSync(_userCacheFile)) return;
  try {
    const raw = JSON.parse(gunzipSync(readFileSync(_userCacheFile)).toString('utf8')) as UserCacheDisk;
    for (const [uid, name] of Object.entries(raw.u ?? {})) {
      _uidToName.set(uid, name);
      _normToUid.set(_normName(name), uid);
    }
    for (const [gid, members] of Object.entries(raw.g ?? {})) {
      const m = new Map<string, string>();
      for (const [norm, uid] of Object.entries(members)) m.set(norm, uid);
      _groupNameToUid.set(gid, m);
    }
    for (const [gid, members] of Object.entries(raw.gn ?? {})) {
      _groupUidToName.set(gid, new Map(Object.entries(members)));
    }
  } catch (e) {
    console.warn('[userCache] Failed to load cache:', e);
  }
}

let _userCacheDirty  = false;
let _userCacheTimer: ReturnType<typeof setTimeout> | null = null;

function _scheduleUserCachePersist(): void {
  _userCacheDirty = true;
  if (_userCacheTimer) return;
  _userCacheTimer = setTimeout(() => {
    _userCacheTimer = null;
    if (!_userCacheDirty) return;
    _userCacheDirty = false;
    try {
      mkdirSync(path.dirname(_userCacheFile), { recursive: true });
      const disk: UserCacheDisk = { u: {}, g: {}, gn: {} };
      for (const [uid, name] of _uidToName) disk.u[uid] = name;
      for (const [gid, m] of _groupNameToUid) {
        const obj: Record<string, string> = {};
        for (const [norm, uid] of m) obj[norm] = uid;
        disk.g[gid] = obj;
      }
      for (const [gid, m] of _groupUidToName) {
        const obj: Record<string, string> = {};
        for (const [uid, name] of m) obj[uid] = name;
        disk.gn![gid] = obj;
      }
      const _tmpUser = _userCacheFile + '.tmp';
      writeFileSync(_tmpUser, gzipSync(JSON.stringify(disk), { level: 9 }));
      renameSync(_tmpUser, _userCacheFile);
      syncToTurso('user_cache', disk);
    } catch (e) {
      console.warn('[userCache] Failed to persist:', e);
    }
  }, 2000);
}

// Load from disk on startup
_loadUserCache();

// ── Public API ────────────────────────────────────────────────────────────────

export const userCache = {
  /** Record a Zalo user seen in a received message. */
  save(uid: string, displayName: string): void {
    // Evict oldest only if new uid (avoid eviction on name update)
    if (!_uidToName.has(uid) && _uidToName.size >= USER_CACHE_MAX) {
      const firstUid = _uidToName.keys().next().value;
      if (firstUid) {
        const oldName = _uidToName.get(firstUid);
        _uidToName.delete(firstUid);
        if (oldName) _normToUid.delete(_normName(oldName));
        // Xoá luôn trong _groupNameToUid để tránh rò rỉ
        for (const [, nameMap] of _groupNameToUid) {
          for (const [norm, uid2] of nameMap) {
            if (uid2 === firstUid) nameMap.delete(norm);
          }
        }
        for (const [, uidMap] of _groupUidToName) uidMap.delete(firstUid);
      }
    }
    const previousName = _uidToName.get(uid);
    if (previousName && previousName !== displayName) {
      const previousNorm = _normName(previousName);
      if (_normToUid.get(previousNorm) === uid) _normToUid.delete(previousNorm);
    }
    _uidToName.set(uid, displayName);
    _normToUid.set(_normName(displayName), uid);
    _scheduleUserCachePersist();
  },

  /** Find a Zalo UID by (normalised) display name. Used for TG→Zalo mention. */
  resolveByName(rawName: string): string | undefined {
    return _normToUid.get(_normName(rawName));
  },

  /** Save display name scoped to a Zalo group for collision-safe resolution. */
  saveForGroup(uid: string, displayName: string, zaloId: string): void {
    // A group member's dName/profile name is contextual and must never replace
    // the account owner's global contact-book name for that UID.
    if (!_uidToName.has(uid)) this.save(uid, displayName);
    let m = _groupNameToUid.get(zaloId);
    if (!m) { m = new Map(); _groupNameToUid.set(zaloId, m); }
    const previousGroupName = _groupUidToName.get(zaloId)?.get(uid);
    if (previousGroupName && previousGroupName !== displayName) {
      const previousNorm = _normName(previousGroupName);
      if (m.get(previousNorm) === uid) m.delete(previousNorm);
    }
    m.set(_normName(displayName), uid);
    let names = _groupUidToName.get(zaloId);
    if (!names) { names = new Map(); _groupUidToName.set(zaloId, names); }
    names.set(uid, displayName);
    _scheduleUserCachePersist();
  },

  /** Resolve UID by name, preferring group-specific lookup over global. */
  resolveByNameInGroup(rawName: string, zaloId: string): string | undefined {
    const norm = _normName(rawName);
    return _groupNameToUid.get(zaloId)?.get(norm) ?? _normToUid.get(norm);
  },

  /** Get display name for a UID. */
  getName(uid: string): string | undefined {
    return _uidToName.get(uid);
  },

  /** Exact display name for a UID inside one group. */
  getNameInGroup(uid: string, zaloId: string): string | undefined {
    return _groupUidToName.get(zaloId)?.get(uid);
  },

  stats(): { users: number; groups: number } {
    return { users: _uidToName.size, groups: _groupNameToUid.size };
  },
};

// ── Alias cache (danh bạ nickname) ───────────────────────────────────────────

/** userId → alias (tên danh bạ người dùng tự đặt) */
const _aliasMap = new Map<string, string>();
/** normalised alias → userId (reverse lookup for mention resolution) */
const _aliasNormToUid = new Map<string, string>();

export const aliasCache = {
  /** Bulk-load from getAliasList response */
  setAll(items: Array<{ userId: string; alias: string }>): void {
    _aliasMap.clear();
    _aliasNormToUid.clear();
    this.merge(items);
  },

  /** Merge aliases/contact display names into the existing cache. */
  merge(items: Array<{ userId: string; alias?: string; displayName?: string }>): void {
    for (const { userId, alias, displayName } of items) {
      const name = (alias ?? displayName)?.trim();
      if (name) {
        const previous = _aliasMap.get(userId);
        if (previous && previous !== name) {
          const previousNorm = _normName(previous);
          if (_aliasNormToUid.get(previousNorm) === userId) {
            _aliasNormToUid.delete(previousNorm);
          }
        }
        _aliasMap.set(userId, name);
        _aliasNormToUid.set(_normName(name), userId);
      }
    }
  },

  /** Number of cached contact display names / aliases. */
  size(): number {
    return _aliasMap.size;
  },

  /** Find a Zalo UID by alias name (for TG→Zalo mention via alias). */
  resolveByAlias(rawName: string): string | undefined {
    return _aliasNormToUid.get(_normName(rawName));
  },

  /** Get alias for a userId, or undefined if not set */
  get(userId: string): string | undefined {
    return _aliasMap.get(userId);
  },

  /**
   * Build display label: "Alias (Tên thật)" if alias differs from realName,
   * otherwise just realName.
   */
  label(userId: string, realName: string): string {
    const alias = _aliasMap.get(userId);
    if (!alias || alias === realName) return realName;
    return `${alias} (${realName})`;
  },
};

// ── Friends cache (in-memory, TTL-refreshed) ──────────────────────────────────

export interface ZaloFriend {
  userId:      string;
  displayName: string;
  /** tên danh bạ (alias), nếu có */
  alias?:      string;
}

function upsertAliasFromFriend(friend: ZaloFriend): void {
  // Only explicit aliases belong in aliasCache. Copying displayName here made
  // ordinary profile/contact names masquerade as aliases and change randomly
  // whenever getAllFriends refreshed.
  const alias = friend.alias?.trim();
  if (!alias) return;
  aliasCache.merge([{ userId: friend.userId, alias }]);
}

const FRIENDS_TTL_MS = 5 * 60 * 1000; // 5 minutes

let _friends:    ZaloFriend[] = [];
let _friendsTs:  number       = 0;

export const friendsCache = {
  /** Store a fresh friends list. */
  set(list: ZaloFriend[]): void {
    _friends   = list;
    _friendsTs = Date.now();
    for (const friend of list) upsertAliasFromFriend(friend);
  },

  /**
   * Search by substring (case/diacritic-insensitive).
   * Searches alias first, falls back to displayName.
   * Returns up to `limit` results.
   */
  search(query: string, limit = 10): ZaloFriend[] {
    const q = query.toLowerCase().normalize('NFD').replace(/\p{Mn}/gu, '');
    return _friends
      .filter(f => {
        const searchName = (f.alias || f.displayName || '').toLowerCase().normalize('NFD').replace(/\p{Mn}/gu, '');
        const realName   = f.displayName.toLowerCase().normalize('NFD').replace(/\p{Mn}/gu, '');
        return searchName.includes(q) || realName.includes(q);
      })
      .slice(0, limit);
  },

  /** True if the cache is still fresh. */
  isFresh(): boolean {
    return _friends.length > 0 && Date.now() - _friendsTs < FRIENDS_TTL_MS;
  },

  get(userId: string): ZaloFriend | undefined {
    return _friends.find(f => f.userId === userId);
  },

  stats(): { count: number } {
    return { count: _friends.length };
  },
};

// ── Groups cache (in-memory, TTL-refreshed) ───────────────────────────────────

export interface ZaloGroup {
  groupId:     string;
  name:        string;
  totalMember: number;
}

const GROUPS_TTL_MS = 5 * 60 * 1000; // 5 minutes
let _groups:   ZaloGroup[] = [];
let _groupsTs: number      = 0;

export const groupsCache = {
  set(list: ZaloGroup[]): void {
    _groups   = list;
    _groupsTs = Date.now();
  },

  search(query: string, limit = 10): ZaloGroup[] {
    const q = query.toLowerCase().normalize('NFD').replace(/\p{Mn}/gu, '');
    return _groups
      .filter(g => {
        const n = g.name.toLowerCase().normalize('NFD').replace(/\p{Mn}/gu, '');
        return n.includes(q);
      })
      .slice(0, limit);
  },

  isFresh(): boolean {
    return _groups.length > 0 && Date.now() - _groupsTs < GROUPS_TTL_MS;
  },

  stats(): { count: number } {
    return { count: _groups.length };
  },
};

// ── Sent message store (TG→Zalo direction) ────────────────────────────────────

export interface SentMsgInfo {
  /** Zalo msgId(s) returned by api.sendMessage / api.sendVoice.
   *  Có thể nhiều msgId khi gửi album (mỗi file là một tin Zalo riêng). */
  msgIds:     (string | number)[];
  /** Zalo conversation ID */
  zaloId:     string;
  /** 0 = DM, 1 = Group */
  threadType: 0 | 1;
}

const _sentMap      = new Map<number, SentMsgInfo>(); // tgMsgId → info
const _sentByZaloId = new Map<string, number>();       // String(zaloMsgId) → tgMsgId

/** Insertion-order tracking for sentMap eviction (oldest first) */
const _sentKeyOrder: number[] = [];
const SENT_MAP_MAX = 5000;

/** zaloId values currently being sent by the bot (to handle echo race condition) */
const _pendingSendConvos = new Map<string, { count: number; markedAt: number }>();

const _sentMsgFile = path.resolve(config.dataDir, 'sent-msg.json');

let _sentMsgPersistTimer: ReturnType<typeof setTimeout> | null = null;
function _scheduleSentMsgPersist(): void {
  if (_sentMsgPersistTimer) return;
  _sentMsgPersistTimer = setTimeout(() => {
    _sentMsgPersistTimer = null;
    try {
      if (!existsSync(config.dataDir)) mkdirSync(config.dataDir, { recursive: true });
      const entries = Array.from(_sentMap.entries());
      const _tmp = _sentMsgFile + '.tmp';
      writeFileSync(_tmp, gzipSync(JSON.stringify({ entries }), { level: 9 }));
      renameSync(_tmp, _sentMsgFile);
      syncToTurso('sent_msg', { entries });
    } catch (e) {
      console.warn('[sentMsgStore] Failed to persist sent-msg:', e);
    }
  }, 1000);
}

function _loadSentMsg(): void {
  if (!existsSync(_sentMsgFile)) return;
  try {
    let buf = readFileSync(_sentMsgFile);
    if (buf[0] === 0x1F && buf[1] === 0x8B) buf = gunzipSync(buf);
    const parsed = JSON.parse(buf.toString('utf8')) as { entries: [number, SentMsgInfo][] };
    for (const [tgId, info] of parsed.entries ?? []) {
      _sentMap.set(tgId, info);
      _sentKeyOrder.push(tgId);
      for (const mid of info.msgIds) {
        _sentByZaloId.set(String(mid), tgId);
      }
    }
  } catch { /* ignore */ }
}

_loadSentMsg();

export const sentMsgStore = {
  /** Record a message we sent from TG→Zalo. tgMsgId is the user's TG message. */
  save(tgMsgId: number, info: SentMsgInfo): void {
    // Evict oldest entry if at capacity (only count NEW tgMsgIds)
    if (!_sentMap.has(tgMsgId)) {
      _sentKeyOrder.push(tgMsgId);
      while (_sentKeyOrder.length > SENT_MAP_MAX) {
        const oldest = _sentKeyOrder.shift()!;
        const oldInfo = _sentMap.get(oldest);
        _sentMap.delete(oldest);
        if (oldInfo) {
          for (const mid of oldInfo.msgIds) {
            _sentByZaloId.delete(String(mid));
          }
        }
      }
    }
    const previous = _sentMap.get(tgMsgId);
    if (previous) {
      for (const mid of previous.msgIds) {
        const key = String(mid);
        if (_sentByZaloId.get(key) === tgMsgId) _sentByZaloId.delete(key);
      }
    }
    _sentMap.set(tgMsgId, info);
    for (const mid of info.msgIds) {
      _sentByZaloId.set(String(mid), tgMsgId);
    }
    _scheduleSentMsgPersist();
  },

  get(tgMsgId: number): SentMsgInfo | undefined {
    return _sentMap.get(tgMsgId);
  },

  /**
   * Reverse lookup: given a Zalo msgId we sent (TG→Zalo direction),
   * return the original TG message_id. Used so Zalo replies to our
   * sent messages chain correctly on the TG side.
   */
  getByZaloMsgId(zaloMsgId: string): number | undefined {
    return _sentByZaloId.get(zaloMsgId);
  },

  /**
   * Mark a conversation (zaloId) as currently being sent to by the bot.
   * Call BEFORE api.sendMessage() to avoid race condition where Zalo echoes
   * back the message before the HTTP response (and sentMsgStore.save) arrives.
   */
  markSending(zaloId: string): void {
    const current = _pendingSendConvos.get(zaloId);
    _pendingSendConvos.set(zaloId, {
      count: (current?.count ?? 0) + 1,
      markedAt: Date.now(),
    });
  },

  /** Call AFTER sentMsgStore.save() or on send error. */
  unmarkSending(zaloId: string): void {
    const current = _pendingSendConvos.get(zaloId);
    if (!current || current.count <= 1) {
      _pendingSendConvos.delete(zaloId);
      return;
    }
    _pendingSendConvos.set(zaloId, { ...current, count: current.count - 1 });
  },

  /**
   * Returns true if the bot is currently sending to this zaloId.
   * Used to suppress isSelf echo in the Zalo listener.
   * A long safety expiry prevents a leaked marker from surviving forever;
   * normal operations remove their reference in finally blocks.
   */
  isSendingTo(zaloId: string): boolean {
    const pending = _pendingSendConvos.get(zaloId);
    if (!pending) return false;
    if (Date.now() - pending.markedAt >= 10 * 60_000) {
      _pendingSendConvos.delete(zaloId);
      return false;
    }
    return pending.count > 0;
  },

  stats(): { entries: number } {
    return { entries: _sentMap.size };
  },
};

// ── Reaction summary store (Zalo→TG reaction aggregation) ────────────────────

export interface ReactionSummaryEntry {
  summaryTgMsgId: number | null;
  lastSentText: string;
  /** emoji → actor display name → count (Zalo lets one person drop the same
   *  icon multiple times; total is a real number, e.g. ❤️ ×3) */
  reactions: Record<string, Record<string, number>>;
  debounceTimer: ReturnType<typeof setTimeout> | null;
}

const _reactionSummaries = new Map<number, ReactionSummaryEntry>(); // tgMsgId → entry

export const reactionSummaryStore = {
  /** Add or update a reaction. Returns the entry for this tgMsgId. */
  upsert(tgMsgId: number, emoji: string, actorName: string): ReactionSummaryEntry {
    if (_reactionSummaries.size >= 500) {
      const toDelete: number[] = [];
      for (const [id, e] of _reactionSummaries) {
        if (e.debounceTimer === null && e.summaryTgMsgId !== null) toDelete.push(id);
        if (toDelete.length >= 250) break;
      }
      for (const id of toDelete) _reactionSummaries.delete(id);
    }
    let entry = _reactionSummaries.get(tgMsgId);
    if (!entry) {
      entry = { summaryTgMsgId: null, lastSentText: '', reactions: {}, debounceTimer: null };
      _reactionSummaries.set(tgMsgId, entry);
    }
    if (!entry.reactions[emoji]) entry.reactions[emoji] = {};
    entry.reactions[emoji]![actorName] = (entry.reactions[emoji]![actorName] ?? 0) + 1;
    return entry;
  },

  setSummaryMsgId(tgMsgId: number, summaryMsgId: number): void {
    const entry = _reactionSummaries.get(tgMsgId);
    if (entry) entry.summaryTgMsgId = summaryMsgId;
  },

  stats(): { entries: number } {
    return { entries: _reactionSummaries.size };
  },

  buildText(entry: ReactionSummaryEntry): string {
    const parts: string[] = [];
    for (const [emoji, actors] of Object.entries(entry.reactions)) {
      for (const [name, count] of Object.entries(actors)) {
        parts.push(count > 1 ? `${emoji} ×${count} ${name}` : `${emoji} ${name}`);
      }
    }
    return parts.join('  ');
  },
};

const REACTION_ECHO_TTL_MS = 8_000;
const _pendingReactionEchoes = new Map<string, { count: number; ts: number }>();

function reactionEchoKey(zaloId: string, targetMsgId: string, icon: string): string {
  return `${zaloId}::${targetMsgId}::${icon}`;
}

function prunePendingReactionEchoes(now = Date.now()): void {
  for (const [key, entry] of _pendingReactionEchoes.entries()) {
    if (now - entry.ts > REACTION_ECHO_TTL_MS) _pendingReactionEchoes.delete(key);
  }
}

function decrementPendingReactionEcho(key: string): void {
  const entry = _pendingReactionEchoes.get(key);
  if (!entry) return;
  if (entry.count <= 1) {
    _pendingReactionEchoes.delete(key);
    return;
  }
  _pendingReactionEchoes.set(key, { ...entry, count: entry.count - 1 });
}

export const reactionEchoStore = {
  mark(zaloId: string, targetMsgId: string, icon: string): void {
    const now = Date.now();
    prunePendingReactionEchoes(now);
    const key = reactionEchoKey(zaloId, targetMsgId, icon);
    const existing = _pendingReactionEchoes.get(key);
    _pendingReactionEchoes.set(key, { count: (existing?.count ?? 0) + 1, ts: now });
  },

  consume(zaloId: string, targetMsgId: string, icon: string): boolean {
    const now = Date.now();
    prunePendingReactionEchoes(now);
    const key = reactionEchoKey(zaloId, targetMsgId, icon);
    const entry = _pendingReactionEchoes.get(key);
    if (!entry) return false;
    decrementPendingReactionEcho(key);
    return true;
  },

  cancel(zaloId: string, targetMsgId: string, icon: string): void {
    prunePendingReactionEchoes();
    const key = reactionEchoKey(zaloId, targetMsgId, icon);
    decrementPendingReactionEcho(key);
  },
};

const REACTION_EVENT_DEDUPE_TTL_MS = 15_000;
const REACTION_EVENT_DEDUPE_MAX = 20_000;
const REACTION_ACTION_MAX = 50_000;
const _recentReactionEvents = new Map<string, number>();
const _seenReactionActions = new Set<string>();

/** Long-lived dedupe for reaction actionIds: unique per action, never expires. */
function markReactionAction(actionId: string): boolean {
  if (_seenReactionActions.has(actionId)) return true;
  if (_seenReactionActions.size >= REACTION_ACTION_MAX) {
    const oldest = _seenReactionActions.values().next().value as string | undefined;
    if (oldest !== undefined) _seenReactionActions.delete(oldest);
  }
  _seenReactionActions.add(actionId);
  return false;
}

function pruneRecentReactionEvents(now = Date.now()): void {
  for (const [key, ts] of _recentReactionEvents) {
    if (now - ts > REACTION_EVENT_DEDUPE_TTL_MS) _recentReactionEvents.delete(key);
  }
  while (_recentReactionEvents.size > REACTION_EVENT_DEDUPE_MAX) {
    const oldest = _recentReactionEvents.keys().next().value as string | undefined;
    if (!oldest) break;
    _recentReactionEvents.delete(oldest);
  }
}

function markRecentReactionEvent(key: string): boolean {
  const now = Date.now();
  pruneRecentReactionEvents(now);
  const seenAt = _recentReactionEvents.get(key);
  if (seenAt !== undefined && now - seenAt <= REACTION_EVENT_DEDUPE_TTL_MS) {
    return true;
  }
  _recentReactionEvents.set(key, now);
  return false;
}

function normalizeReactionName(input: string): string {
  return input.trim().normalize('NFC').replace(/\s+/g, ' ').toLowerCase();
}

function normalizeReactionMsgIds(ids: string[]): string[] {
  return Array.from(new Set(ids.map(id => id.trim()).filter(Boolean))).sort();
}

export const reactionEventDedupeStore = {
  isDuplicateZaloInbound(input: {
    zaloId: string;
    targetMsgIds: string[];
    icon: string;
    actorUid?: string;
    actorName?: string;
    actionId?: string;
  }): boolean {
    // Prefer the server-generated actionId: it is unique per reaction *action*,
    // so repeated identical taps (the same person dropping ❤️ several times)
    // are kept and counted, while re-emits/replays of the SAME action (even
    // long after the live event, e.g. reconnect catch-up) are deduped (issue
    // #65). The set never expires because an actionId can only occur once.
    if (input.actionId?.trim()) {
      return markReactionAction(input.actionId.trim());
    }
    const targetKey = normalizeReactionMsgIds(input.targetMsgIds).join('|');
    if (!targetKey) return false;
    const actorKey = input.actorUid?.trim()
      || (input.actorName ? normalizeReactionName(input.actorName) : '')
      || 'unknown';
    const key = `zalo-in::${input.zaloId.trim()}::${targetKey}::${input.icon.trim()}::${actorKey}`;
    return markRecentReactionEvent(key);
  },

  isDuplicateTgOutbound(input: {
    chatId: number;
    messageId: number;
    actorId: string;
    emoji: string;
  }): boolean {
    const key = `tg-out::${input.chatId}::${input.messageId}::${input.actorId.trim()}::${input.emoji.trim()}`;
    return markRecentReactionEvent(key);
  },

  stats(): { entries: number } {
    pruneRecentReactionEvents();
    return { entries: _recentReactionEvents.size };
  },
};

// ── TG media group buffer (TG→Zalo album sync) ────────────────────────────────

export interface MediaGroupItem {
  fileId:    string;
  fname:     string;
  fileSize?: number;
  caption?:  string;
  captionMentions?: Array<{ pos: number; uid: string; len: number }>;
  tgMsgId?:  number;
  isVideo?:  boolean;
  videoWidth?: number;
  videoHeight?: number;
  videoDuration?: number;
}

interface MediaGroupBuffer {
  timer:      ReturnType<typeof setTimeout>;
  items:      MediaGroupItem[];
  topicId:    number;
  zaloId:     string;
  threadType: 0 | 1;
  replyToMsgId?: number;
}

const _mgBuffers = new Map<string, MediaGroupBuffer>();

export const mediaGroupStore = {
  /** Add a photo/video to an in-flight media group buffer. Returns the buffer. */
  add(
    groupId: string,
    item: MediaGroupItem,
    meta: Omit<MediaGroupBuffer, 'timer' | 'items'>,
    onFlush: (items: MediaGroupItem[], meta: Omit<MediaGroupBuffer, 'timer' | 'items'>) => void | Promise<void>,
  ): void {
    const flush = (buf: MediaGroupBuffer): void => {
      _mgBuffers.delete(groupId);
      void Promise.resolve(onFlush(buf.items, buf))
        .catch(err => console.error(`[mediaGroupStore] Flush failed (groupId=${groupId}):`, err));
    };
    // Video updates can arrive slightly after photo updates. A wider debounce
    // prevents one Telegram album from being split into multiple Zalo batches.
    const FLUSH_DELAY_MS = 1_000;
    const existing = _mgBuffers.get(groupId);
    if (existing) {
      clearTimeout(existing.timer);
      existing.items.push(item);
      existing.timer = setTimeout(() => flush(existing), FLUSH_DELAY_MS);
    } else {
      const buf: MediaGroupBuffer = {
        ...meta,
        items: [item],
        timer: setTimeout(() => flush(buf), FLUSH_DELAY_MS),
      };
      _mgBuffers.set(groupId, buf);
    }
  },
};

// ── Zalo album buffer (Zalo→TG multi-photo) ────────────────────────────────────

interface ZaloAlbumItem {
  url:    string;
  fallbackUrls: string[];
  msgIds: string[];
  zaloQuote: ZaloQuoteData | undefined;
}

interface ZaloAlbumBuffer {
  timer:      ReturnType<typeof setTimeout>;
  items:      ZaloAlbumItem[];
  senderName: string;
  topicId:    number;
  tgBase:     { message_thread_id: number; reply_parameters?: { message_id: number; allow_sending_without_reply: boolean }; disable_notification?: boolean };
  caption?:   string;
}

const _zaloAlbumBuffers = new Map<string, ZaloAlbumBuffer>(); // key = `${threadId}:${uidFrom}`

export const zaloAlbumStore = {
  add(
    key: string,
    url: string,
    msgIds: string[],
    caption: string | undefined,
    meta: Omit<ZaloAlbumBuffer, 'timer' | 'items' | 'caption'> & { zaloQuote: ZaloQuoteData | undefined },
    onFlush: (buf: Omit<ZaloAlbumBuffer, 'timer'>) => void | Promise<void>,
    _childnumber = 0,
    fallbackUrls: readonly string[] = [],
  ): void {
    const { zaloQuote, ...bufferMeta } = meta;
    const uniqueFallbackUrls = Array.from(new Set(
      fallbackUrls.map(candidate => candidate.trim()).filter(candidate => candidate && candidate !== url),
    ));
    const flush = (buf: ZaloAlbumBuffer): void => {
      _zaloAlbumBuffers.delete(key);
      void Promise.resolve(onFlush({
        items: buf.items,
        senderName: buf.senderName,
        topicId: buf.topicId,
        tgBase: buf.tgBase,
        caption: buf.caption,
      })).catch(err => console.error(`[zaloAlbumStore] Flush failed (key=${key}):`, err));
    };

    // `childnumber` is not reliable for DM albums (Zalo commonly emits 0 for
    // every image). Debounce by conversation+sender instead: photos arriving
    // close together become one album, while a pause starts a new batch.
    const FLUSH_DELAY_MS = 600;
    const existing = _zaloAlbumBuffers.get(key);
    if (existing) {
      clearTimeout(existing.timer);
      const incomingUrls = new Set([url, ...uniqueFallbackUrls]);
      const duplicateItem = existing.items.find(item =>
        [item.url, ...item.fallbackUrls].some(candidate => incomingUrls.has(candidate)));
      if (!duplicateItem) {
        existing.items.push({ url, fallbackUrls: uniqueFallbackUrls, msgIds: [...msgIds], zaloQuote });
      } else {
        console.log(`[zaloAlbumStore] Skipping duplicate URL in album buffer (key=${key}, items=${existing.items.length})`);
        duplicateItem.msgIds.push(...msgIds);
        duplicateItem.fallbackUrls = Array.from(new Set([
          ...duplicateItem.fallbackUrls,
          url,
          ...uniqueFallbackUrls,
        ].filter(candidate => candidate !== duplicateItem.url)));
      }
      if (!existing.caption && caption) existing.caption = caption;
      existing.timer = setTimeout(() => flush(existing), FLUSH_DELAY_MS);
    } else {
      const buf: ZaloAlbumBuffer = {
        ...bufferMeta,
        items: [{ url, fallbackUrls: uniqueFallbackUrls, msgIds: [...msgIds], zaloQuote }],
        caption,
        timer: setTimeout(() => flush(buf), FLUSH_DELAY_MS),
      };
      _zaloAlbumBuffers.set(key, buf);
    }
  },
};

// ── Poll store (Zalo ↔ TG native poll, persisted to disk) ─────────────────────

export interface PollEntry {
  pollId:           number;
  zaloGroupId:      string;
  tgPollMsgId:      number;    // TG message_id of the bot-owned clone poll
  tgOrigPollMsgId?: number;    // TG message_id of the user's original poll (to stopPoll on lock)
  tgPollUUID:       string;    // TG poll identifier from ctx.pollAnswer.poll_id
  tgScoreMsgId:     number;    // TG message_id of the editable vote-count text below
  tgThreadId:       number;    // Forum thread (topic) id
  options: {
    option_id: number;
    content:   string;
  }[];
}

const _pollFile = path.resolve(config.dataDir, 'polls.json.gz');

function _loadPolls(): void {
  if (!existsSync(_pollFile)) return;
  try {
    const buf = readFileSync(_pollFile);
    const raw = JSON.parse(gunzipSync(buf).toString('utf8')) as { entries: PollEntry[] };
    for (const entry of raw.entries ?? []) {
      _pollByZaloId.set(entry.pollId, entry);
      _pollByTgId.set(entry.tgPollMsgId, entry);
      _pollByUUID.set(entry.tgPollUUID, entry);
    }
    console.log(`[pollStore] Loaded ${raw.entries.length} polls from disk`);
  } catch (e) {
    console.warn('[pollStore] Failed to load polls:', e);
  }
}

let _pollPersistTimer: ReturnType<typeof setTimeout> | null = null;
function _schedulePollPersist(): void {
  if (_pollPersistTimer) return;
  _pollPersistTimer = setTimeout(() => {
    _pollPersistTimer = null;
    try {
      mkdirSync(path.dirname(_pollFile), { recursive: true });
      const entries: PollEntry[] = [];
      for (const e of _pollByZaloId.values()) entries.push(e);
      const tmp = _pollFile + '.tmp';
      writeFileSync(tmp, gzipSync(JSON.stringify({ entries }), { level: 9 }));
      renameSync(tmp, _pollFile);
      syncToTurso('polls', { entries });
    } catch (e) {
      console.warn('[pollStore] Failed to persist:', e);
    }
  }, 1500);
}

const _pollByZaloId = new Map<number, PollEntry>();       // pollId → entry
const _pollByTgId   = new Map<number, PollEntry>();       // tgPollMsgId → entry
const _pollByUUID   = new Map<string, PollEntry>();       // tgPollUUID → entry

// Load persisted polls on startup
_loadPolls();

export const pollStore = {
  save(entry: PollEntry): void {
    const previous = _pollByZaloId.get(entry.pollId);
    if (previous) {
      if (_pollByTgId.get(previous.tgPollMsgId) === previous) {
        _pollByTgId.delete(previous.tgPollMsgId);
      }
      if (_pollByUUID.get(previous.tgPollUUID) === previous) {
        _pollByUUID.delete(previous.tgPollUUID);
      }
    }
    _pollByZaloId.set(entry.pollId, entry);
    _pollByTgId.set(entry.tgPollMsgId, entry);
    _pollByUUID.set(entry.tgPollUUID, entry);
    _schedulePollPersist();
  },

  getByPollId(pollId: number): PollEntry | undefined {
    return _pollByZaloId.get(pollId);
  },

  getByTgMsgId(tgMsgId: number): PollEntry | undefined {
    return _pollByTgId.get(tgMsgId);
  },

  getByTgPollUUID(uuid: string): PollEntry | undefined {
    return _pollByUUID.get(uuid);
  },

  /** Update tgScoreMsgId after editing */
  updateScoreMsg(pollId: number, newMsgId: number): void {
    const e = _pollByZaloId.get(pollId);
    if (e) {
      e.tgScoreMsgId = newMsgId;
      _schedulePollPersist();
    }
  },
};
