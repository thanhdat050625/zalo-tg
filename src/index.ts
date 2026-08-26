import http from 'http';
import { getZaloApi, resetZaloApi } from './zalo/client.js';
import { CloseReason, ThreadType } from 'zca-js';
import { setupZaloHandler } from './zalo/handler.js';
import { tgBot, syncTelegramCommands } from './telegram/bot.js';
import { setupTelegramHandler } from './telegram/handler.js';
import { config } from './config.js';
import { startUpdateChecker } from './updater.js';
import { store, userCache } from './store.js';
import { registerShutdownHandler, requestShutdown } from './lifecycle.js';
import { terminal } from './utils/terminal.js';

const _bridgeStartTime = Date.now();

// ── Global safety net — prevent unhandled rejections from crashing ────────────
process.on('unhandledRejection', (reason) => {
  console.error('[Boot] Unhandled rejection (ignored):', reason);
});
process.on('uncaughtException', (err) => {
  console.error('[Boot] Uncaught exception:', err);
  void requestShutdown('Uncaught exception', 43);
});

// ── Module-level ref to Telegram handler's API setter (used by reconnect) ──────
let _setZaloApi: ((api: Awaited<ReturnType<typeof getZaloApi>>) => void) | null = null;
let _reconnectInProgress = false;
let _reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let _activeZaloApi: Awaited<ReturnType<typeof getZaloApi>> | null = null;
let _bridgeReadyAnnounced = false;
let _topicCleanupInterval: ReturnType<typeof setInterval> | null = null;

// ── Boot Zalo (also used when /login swaps in a fresh API) ───────────────────

/**
 * Unified Topic Cleaner:
 * 1. Checks all mapped group topics against active Zalo groups (prunes groups you left).
 * 2. Scans Telegram forum topics and deletes any orphan / duplicate topics not in store.
 * Runs automatically on boot, every 1 hour, and when /topic clean is called.
 */
export async function reconcileAndCleanTopics(
  api?: Awaited<ReturnType<typeof getZaloApi>> | null,
): Promise<{ removedFromStore: string[]; deletedOrphanTgTopics: number[]; totalChecked: number }> {
  const removedFromStore: string[] = [];
  const deletedOrphanTgTopics: number[] = [];
  const allTopics = store.all();

  try {
    // 1. Check active Zalo groups (if Zalo API is connected)
    if (api) {
      const groups = await api.getAllGroups().catch(() => undefined) as { gridVerMap?: Record<string, string> } | undefined;
      const activeGroupIds = new Set(Object.keys(groups?.gridVerMap ?? {}));

      for (const entry of allTopics) {
        if (entry.type === 1 && !activeGroupIds.has(entry.zaloId)) {
          try {
            await tgBot.telegram.deleteForumTopic(config.telegram.groupId, entry.topicId);
          } catch (tgErr) {
            const msg = (tgErr as Error)?.message ?? String(tgErr);
            if (!msg.includes('TOPIC_ID_INVALID') && !msg.includes('message thread not found')) {
              console.warn(`[TopicCleaner] Delete Telegram topic ${entry.topicId} (${entry.name}):`, msg);
            }
          }
          store.remove(entry.topicId);
          removedFromStore.push(`Nhóm: "${entry.name}" (${entry.zaloId})`);
        }
      }
    }

    // 2. Scan and purge orphan / duplicate topics from Telegram that are not in store
    const allKnownTopicIds = new Set(store.all().map(e => e.topicId));
    const highestStoreId = Math.max(0, ...Array.from(allKnownTopicIds));
    const scanLimit = Math.max(highestStoreId + 30, 300);

    for (let id = 2; id <= scanLimit; id++) {
      if (allKnownTopicIds.has(id)) continue;

      try {
        await tgBot.telegram.deleteForumTopic(config.telegram.groupId, id);
        deletedOrphanTgTopics.push(id);
        console.log(`[TopicCleaner] Deleted orphan Telegram topic #${id}`);
        await new Promise(r => setTimeout(r, 80));
      } catch {
        // Not a topic or already deleted -> skip quietly
      }
    }

    const totalCleaned = removedFromStore.length + deletedOrphanTgTopics.length;
    if (totalCleaned > 0) {
      terminal.status('topics', `cleaned ${totalCleaned} stale/orphan topic(s)`, 'warn');
      console.log(`[TopicCleaner] Cleaned ${removedFromStore.length} stale group(s) and ${deletedOrphanTgTopics.length} orphan Telegram topic(s).`);
    } else {
      console.log(`[TopicCleaner] Periodic scan complete: all ${allTopics.length} topic(s) valid & in sync.`);
    }

    return {
      removedFromStore,
      deletedOrphanTgTopics,
      totalChecked: allTopics.length,
    };
  } catch (err) {
    console.warn('[TopicCleaner] Error during topic scan & clean:', err);
    return {
      removedFromStore,
      deletedOrphanTgTopics,
      totalChecked: allTopics.length,
    };
  }
}

async function startZalo(
  api: Awaited<ReturnType<typeof getZaloApi>>,
  isReconnect = false,
): Promise<void> {
  _activeZaloApi = api;
  if (!isReconnect) void reconcileAndCleanTopics(api);

  // Setup periodic 1-hour topic scan and cleanup
  if (!_topicCleanupInterval) {
    _topicCleanupInterval = setInterval(() => {
      if (_activeZaloApi) {
        void reconcileAndCleanTopics(_activeZaloApi);
      }
    }, 60 * 60 * 1000); // 1 hour
  }

  await setupZaloHandler(api);
  if (isReconnect) {
    api.listener.once('connected', () => {
      try {
        // Recover recent gap after disconnect (messages + reactions in both DM/group).
        api.listener.requestOldMessages(ThreadType.User);
        api.listener.requestOldMessages(ThreadType.Group);
        api.listener.requestOldReactions(ThreadType.User);
        api.listener.requestOldReactions(ThreadType.Group);
        terminal.status('sync', 'catch-up requested after reconnect', 'info');
      } catch (err) {
        console.warn('[Boot] Failed to request catch-up sync:', err);
      }
    });
  }
  api.listener.start();
  terminal.status('zalo', `listener ${isReconnect ? 're' : ''}started`, 'success');
  if (!_bridgeReadyAnnounced) {
    _bridgeReadyAnnounced = true;
    terminal.status('bridge', 'ready · forwarding active', 'success');
    terminal.section('LIVE ACTIVITY');
  }

  const scheduleReconnect = (delayMs: number): void => {
    if (_reconnectTimer || _reconnectInProgress) return;
    _reconnectTimer = setTimeout(() => {
      _reconnectTimer = null;
      void (async () => {
        if (_reconnectInProgress) return;
        _reconnectInProgress = true;
        try {
          resetZaloApi();
          const newApi = await getZaloApi();
          _setZaloApi?.(newApi);
          await startZalo(newApi, true);
          tgBot.telegram.sendMessage(config.telegram.groupId, '✅ Zalo đã kết nối lại và đang đồng bộ lại tin gần đây.').catch(() => undefined);
          terminal.status('zalo', 'reconnected and syncing', 'success');
        } catch (err) {
          console.error('[Boot] Zalo reconnect failed:', err);
          tgBot.telegram.sendMessage(
            config.telegram.groupId,
            '⚠️ Kết nối lại Zalo thất bại. Hãy dùng <b>/login</b> để đăng nhập lại.',
            { parse_mode: 'HTML' },
          ).catch(() => undefined);
        } finally {
          _reconnectInProgress = false;
        }
      })();
    }, delayMs);
  };

  // Auto-reconnect only on closings that are safe to recover automatically.
  api.listener.once('disconnected', (code: CloseReason, reason: string) => {
    if (code === CloseReason.ManualClosure) return;
    if (code === CloseReason.DuplicateConnection) {
      console.warn(`[Boot] Zalo disconnected: duplicate connection (code=${code}, reason=${reason})`);
      tgBot.telegram.sendMessage(
        config.telegram.groupId,
        '⚠️ Zalo bị ngắt do đăng nhập trùng phiên (duplicate connection). Đóng phiên Zalo Web/PC khác rồi dùng <b>/login</b> nếu cần.',
        { parse_mode: 'HTML' },
      ).catch(() => undefined);
      return;
    }
    if (code === CloseReason.KickConnection) {
      console.warn(`[Boot] Zalo disconnected: kicked connection (code=${code}, reason=${reason})`);
      tgBot.telegram.sendMessage(
        config.telegram.groupId,
        '⚠️ Zalo đã ngắt phiên bridge (kick connection). Vui lòng đăng nhập lại bằng <b>/login</b>.',
        { parse_mode: 'HTML' },
      ).catch(() => undefined);
      return;
    }
    console.warn(`[Boot] Zalo disconnected (code=${code}, reason=${reason}), reconnecting in 5 s…`);
    tgBot.telegram.sendMessage(
      config.telegram.groupId,
      '⚠️ Zalo bị ngắt kết nối, đang thử kết nối lại…',
    ).catch(() => undefined);
    scheduleReconnect(5_000);
  });
}

async function main(): Promise<void> {
  await terminal.intro('1.0.0');
  terminal.installConsoleTheme();
  terminal.section('STARTUP');
  terminal.status('runtime', `${process.version} · pid ${process.pid}`, 'muted');
  terminal.status('cache', `${userCache.stats().users} users · ${store.all().length} topics restored`, 'muted');

  // ── Auto update checker — must register BEFORE setupTelegramHandler ─────────
  // bot.action() is middleware; the catch-all on('callback_query') in handler.ts
  // doesn't call next(), so ua: callbacks must be registered first in the chain.
  startUpdateChecker(tgBot);

  // ── Wire up Telegram handler BEFORE launching the bot ─────────────────────
  // setupTelegramHandler returns a setter to inject the Zalo API after auto-login.
  const setZaloApi = setupTelegramHandler(null, async (newApi) => {
    await startZalo(newApi, true);
  });
  _setZaloApi = setZaloApi;

  // ── Register bot commands for Telegram menu ───────────────────────────────
  tgBot.telegram.setMyCommands([
    { command: 'login',          description: 'Đăng nhập Zalo qua QR code' },
    { command: 'loginweb',       description: 'Đăng nhập Zalo QR (giống /login)' },
    { command: 'loginapp',       description: 'Đăng nhập Zalo qua PC App API' },
    { command: 'search',         description: 'Tìm bạn bè / nhóm Zalo để tạo topic' },
    { command: 'group_info',     description: 'Xem thông tin & thành viên nhóm Zalo hiện tại' },
    { command: 'group_infoall',  description: 'Xem toàn bộ thành viên nhóm Zalo hiện tại' },
    { command: 'addfriend',      description: 'Tìm & kết bạn Zalo theo số điện thoại' },
    { command: 'addgroup',       description: 'Tạo topic cho nhóm Zalo chưa có topic' },
    { command: 'joingroup',      description: 'Tham gia nhóm Zalo qua link' },
    { command: 'leavegroup',     description: 'Rời nhóm Zalo & đóng topic (dùng trong topic nhóm)' },
    { command: 'friendrequests', description: 'Xem lời mời kết bạn & lời mời nhóm' },
    { command: 'topic',          description: 'Quản lý topic: list / clean / info / delete' },
    { command: 'history',        description: 'Nạp lịch sử chat nhóm vào topic (dùng trong topic nhóm)' },
    { command: 'autoreply',      description: 'Tự trả lời DM khi offline: on / off / status' },
    { command: 'recall',         description: 'Thu hồi tin nhắn (reply vào tin đã gửi)' },
    { command: 'admin',          description: 'Admin panel: trạng thái, cache, tra mapping' },
    { command: 'status',         description: 'Xem trạng thái bridge: uptime, số topic, Zalo' },
    { command: 'restart',        description: 'Khởi động lại bridge (chỉ admin)' },
    { command: 'setup',          description: 'Cấu hình biến env qua wizard (chỉ admin)' },
    { command: 'update',         description: 'Kiểm tra bản cập nhật mới' },
  ]).catch(() => undefined);

  // ── Graceful shutdown/restart shared by signals, commands and polling ──────
  registerShutdownHandler(async (reason, exitCode) => {
    // Animate while listeners stop and debounced stores flush, so shutdown is
    // both visually smooth and operationally useful rather than a fixed delay.
    const outro = terminal.shutdown(`${reason} · exit ${exitCode}`);
    if (_reconnectTimer) {
      clearTimeout(_reconnectTimer);
      _reconnectTimer = null;
    }
    if (_topicCleanupInterval) {
      clearInterval(_topicCleanupInterval);
      _topicCleanupInterval = null;
    }
    try { _activeZaloApi?.listener.stop(); } catch { /* ignore */ }
    try { await tgBot.stop(reason); } catch { /* bot may not have launched yet */ }
    // Let debounced msg/user-cache persistence finish before process exit.
    await new Promise(r => setTimeout(r, 2500));
    await outro;
    process.exit(exitCode);
  });

  // ── Start Telegram bot so /login can be received immediately ───────────────
  // NOTE: tgBot.launch() runs the polling loop forever, so we must NOT await it.
  // The second argument callback fires once getMe() + deleteWebhook() succeed.
  void tgBot.launch({ allowedUpdates: ['message', 'callback_query', 'message_reaction', 'poll_answer', 'poll'] }, () => {
    terminal.status('telegram', 'polling connected', 'success');

    syncTelegramCommands()
      .then(() => terminal.status('commands', 'menu synchronized', 'success'))
      .catch((err: unknown) => console.warn('[Boot] Failed to sync Telegram commands:', err));

    // ── Attempt Zalo login in background ────────────────────────────────────
    // If credentials.json exists → connects automatically and updates currentApi.
    // If not → notifies the user to run /login.
    getZaloApi()
      .then(async (api) => {
        setZaloApi(api);   // ← inject into Telegram handler so TG→Zalo works
        await startZalo(api);
      })
      .catch((err: unknown) => {
        console.warn('[Boot] Zalo auto-login failed:', err);
        tgBot.telegram
          .sendMessage(
            config.telegram.groupId,
            '⚠️ Chưa đăng nhập Zalo. Gửi <b>/login</b> để đăng nhập.',
            { parse_mode: 'HTML' },
          )
          .catch(() => undefined);
      });
  }).catch((err: unknown) => {
    console.error('[Boot] Telegram polling stopped:', err);
    // Do not leave a half-alive Zalo-only bridge. A supervisor/run.sh can
    // restart exit code 43; a direct npm start exits visibly instead of lying.
    void requestShutdown('Telegram polling failure', 43);
  });

  terminal.status('bridge', 'starting services…', 'info');

  // ── Lightweight HTTP health-check server (for Render / Koyeb / Docker port binding) ──
  const port = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
  try {
    const server = http.createServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        service: 'zalo-tg-bridge',
        uptime: Math.floor((Date.now() - _bridgeStartTime) / 1000),
        zalo: _activeZaloApi ? 'connected' : 'waiting_auth',
        telegram: 'connected',
      }));
    });
    server.listen(port, '0.0.0.0', () => {
      terminal.status('http', `health check listening on port ${port}`, 'success');
    });
  } catch (httpErr) {
    console.warn('[Boot] Could not start HTTP server:', httpErr);
  }

  process.once('SIGINT',  () => { void requestShutdown('Received SIGINT', 0); });
  process.once('SIGTERM', () => { void requestShutdown('Received SIGTERM', 0); });
}

main().catch((err: unknown) => {
  console.error('[Boot] Fatal error:', err);
  process.exit(1);
});
