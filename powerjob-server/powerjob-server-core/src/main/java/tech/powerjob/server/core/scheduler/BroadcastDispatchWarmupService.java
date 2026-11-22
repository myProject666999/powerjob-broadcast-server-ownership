package tech.powerjob.server.core.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import tech.powerjob.common.enums.ExecuteType;
import tech.powerjob.server.common.module.WorkerInfo;
import tech.powerjob.server.remote.worker.WorkerClusterQueryService;

import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 广播任务派发预热（仅对刚接管的 application 生效）。
 * <p>
 * 背景：多 Server 按应用分片，归属由 AppInfoDO.currentServer 表示。本机刚接管某应用时，
 * 内存中的 Worker 心跳视图（WorkerClusterManagerService）尚未积累，此时到点的广播任务
 * 会因为候选集合为空而立即 NO_WORKER_AVAILABLE 失败，或只在已知的部分机器上执行（未知机器无执行记录）。
 * <p>
 * 预热期间广播实例暂缓派发、不判失败，直到候选集合稳定（连续若干检查周期不再增加）或到达有上限的预热时长。
 * 普通（非广播）任务不进入预热：候选为空仍按现有逻辑失败，稳态派发行为不变。
 * <p>
 * 触发方式：由 {@link ServerOwnershipService} 的应用归属快照差异驱动（本机上一轮没有、这一轮出现的应用），
 * 不依赖选举通知、不修改选举代码、不新增持久化字段，接受最多一个检查周期的延迟。
 * 首次发现与服务重启不做区分：重启后本机内存视图（含 Worker 心跳视图）本就丢失，
 * 全部归属应用重新预热属于有意行为。
 * <p>
 * 稳定判定的说明：
 * <ul>
 *     <li>已知地址集合只增不减（并集语义）：Worker 短暂掉线不被遗忘，集合缩小不重置稳定计数、不延长预热；</li>
 *     <li>集合稳定不代表发现了全部 Worker——没有权威总数，真正宕机的机器永远不会出现，因此预热必须有上限，
 *     结束日志只记录已知 Worker 数，不输出伪精确的覆盖比例；</li>
 *     <li>同一应用再次换手后重新计时，每次预热仍有独立上限。</li>
 * </ul>
 * 边界：不解决执行中 Worker 丢失的问题（issue#1003 行为不变，丢失节点的子任务直接失败不重试）。
 * <p>
 * 状态全部保存在本机内存，日志沿用现有 slf4j 设施，不引入指标组件。
 *
 * @author tjq
 * @since 2026/11/23
 */
@Slf4j
@Service
public class BroadcastDispatchWarmupService {

    private final WorkerClusterQueryService workerClusterQueryService;

    private final boolean enabled;

    /**
     * 单次预热时长上限，默认 20000ms（约两个 Worker 心跳周期）
     */
    private final long maxWarmupMs;

    /**
     * 稳定判定所需的连续无增长检查周期数
     */
    private final int stableRoundsRequired;

    private final Clock clock;

    /**
     * 预热状态：appId -> state，仅本机内存
     */
    private final Map<Long, WarmupState> warmupApps = new ConcurrentHashMap<>();

    @Autowired
    public BroadcastDispatchWarmupService(WorkerClusterQueryService workerClusterQueryService,
                                          @Value("${oms.broadcast.warmup.enabled:true}") boolean enabled,
                                          @Value("${oms.broadcast.warmup.max-duration-ms:20000}") long maxWarmupMs,
                                          @Value("${oms.broadcast.warmup.stable-rounds:2}") int stableRoundsRequired) {
        this(workerClusterQueryService, enabled, maxWarmupMs, stableRoundsRequired, Clock.systemUTC());
    }

    /**
     * 测试专用构造器，注入时钟
     */
    BroadcastDispatchWarmupService(WorkerClusterQueryService workerClusterQueryService,
                                   boolean enabled, long maxWarmupMs, int stableRoundsRequired, Clock clock) {
        this.workerClusterQueryService = workerClusterQueryService;
        this.enabled = enabled;
        this.maxWarmupMs = maxWarmupMs;
        this.stableRoundsRequired = stableRoundsRequired;
        this.clock = clock;
    }

    /**
     * 归属快照刷新回调：识别新接管的应用并推进所有预热状态机。
     * 由 {@link ServerOwnershipService} 在每次快照刷新时调用（约每个检查周期一次）。
     *
     * @param ownedAppIds    本机当前负责的全部应用
     * @param newlyOwnedAppIds 上一轮没有、这一轮出现的应用（刚接管；含本机重启后的首次发现）。
     *                         只有它们会开启新的预热——预热结束（稳定或到期）后仍归属本机的应用不会再次预热
     */
    public void onOwnershipSnapshot(Set<Long> ownedAppIds, Set<Long> newlyOwnedAppIds) {
        if (!enabled) {
            return;
        }
        final long now = clock.millis();
        // 失去归属的应用：移除预热状态（再次接手时会作为 newlyOwned 重新计时）
        warmupApps.keySet().removeIf(appId -> {
            if (ownedAppIds.contains(appId)) {
                return false;
            }
            log.info("[BroadcastWarmup] app({}) is no longer owned by this server, drop warmup state.", appId);
            return true;
        });
        // 新接管的应用：开始预热（重复换手时旧状态已在上一步移除，此处重新计时、重新计上限）
        for (Long appId : newlyOwnedAppIds) {
            warmupApps.computeIfAbsent(appId, id -> {
                log.info("[BroadcastWarmup] app({}) is newly taken over, start broadcast dispatch warmup (maxWarmupMs={}, stableRoundsRequired={}), current warming app count={}.",
                        id, maxWarmupMs, stableRoundsRequired, warmupApps.size() + 1);
                return new WarmupState(now);
            });
        }
        // 推进所有预热应用的状态机
        tick(now);
    }

    /**
     * 计算指定任务的广播派发还需要暂缓的毫秒数。
     *
     * @return 大于 0 表示需要暂缓的时长；0 表示无需暂缓（非广播、未启用、未在预热或预热已到上限）
     */
    public long broadcastDeferMs(Long appId, int executeType) {
        if (!enabled || ExecuteType.of(executeType) != ExecuteType.BROADCAST) {
            return 0;
        }
        WarmupState state = warmupApps.get(appId);
        if (state == null) {
            return 0;
        }
        // 即使 tick 尚未执行到，也绝不允许暂缓超过上限
        return Math.max(0, state.startTime + maxWarmupMs - clock.millis());
    }

    /**
     * 记录一次广播实例暂缓派发
     */
    public void onInstanceDeferred(Long appId, Long instanceId, long deferMs) {
        WarmupState state = warmupApps.get(appId);
        if (state != null) {
            state.deferredCount++;
        }
        log.info("[BroadcastWarmup] defer broadcast instance({}) of app({}) for {}ms (warmup remaining), knownWorkers={}.",
                instanceId, appId, deferMs, state == null ? 0 : state.knownAddresses.size());
    }

    /**
     * 推进所有预热应用的状态机：采样当前已知 Worker 地址集合，更新稳定计数，结束达到条件的预热
     */
    private void tick(long now) {
        for (Map.Entry<Long, WarmupState> entry : warmupApps.entrySet()) {
            Long appId = entry.getKey();
            WarmupState state = entry.getValue();
            Set<String> currentAddresses = sampleAliveWorkerAddresses(appId);
            // 并集语义：只关心是否出现新地址；集合缩小（Worker 掉线）不重置稳定计数、不延长预热
            if (state.knownAddresses.addAll(currentAddresses)) {
                state.stableRounds = 0;
            } else {
                state.stableRounds++;
            }
            long elapsed = now - state.startTime;
            if (state.stableRounds >= stableRoundsRequired) {
                finishWarmup(appId, state, "STABLE", elapsed);
            } else if (elapsed >= maxWarmupMs) {
                finishWarmup(appId, state, "EXPIRED", elapsed);
            }
        }
    }

    private void finishWarmup(Long appId, WarmupState state, String reason, long elapsedMs) {
        warmupApps.remove(appId);
        log.info("[BroadcastWarmup] app({}) warmup finished, reason={}, elapsed={}ms, {}, deferredInstances={}, remaining warming app count={}.",
                appId, reason, elapsedMs, coverageText(state.knownAddresses.size()), state.deferredCount, warmupApps.size());
    }

    /**
     * 覆盖信息文案：只有已知 Worker 数，没有权威总数，因此不输出伪精确的覆盖比例。
     * 若未来需要比例，必须先说明分母来源（如期望 Worker 数的配置），此处刻意不提供。
     */
    static String coverageText(int knownWorkerCount) {
        return "knownWorkers=" + knownWorkerCount + " (no authoritative total, coverage ratio unavailable)";
    }

    private Set<String> sampleAliveWorkerAddresses(Long appId) {
        try {
            List<WorkerInfo> aliveWorkers = workerClusterQueryService.getAllAliveWorkers(appId);
            if (aliveWorkers == null) {
                return new HashSet<>();
            }
            return aliveWorkers.stream().map(WorkerInfo::getAddress).collect(Collectors.toSet());
        } catch (Exception e) {
            log.warn("[BroadcastWarmup] sample workers of app({}) failed, treat as no change.", appId, e);
            return new HashSet<>();
        }
    }

    /**
     * 单个应用的预热状态（仅本机内存）
     */
    private static class WarmupState {
        private final long startTime;
        /**
         * 历史出现过的全部 Worker 地址（并集，只增不减）
         */
        private final Set<String> knownAddresses = new HashSet<>();
        private int stableRounds;
        private int deferredCount;

        private WarmupState(long startTime) {
            this.startTime = startTime;
        }
    }
}
