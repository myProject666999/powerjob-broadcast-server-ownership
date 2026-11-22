package tech.powerjob.server.core.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tech.powerjob.server.persistence.remote.repository.AppInfoRepository;
import tech.powerjob.server.remote.transporter.TransportService;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 应用归属判断（只依赖 AppInfoDO.currentServer 的应用到 Server 映射）。
 * <p>
 * 抽取背景：{@link InstanceStatusCheckService} 的四个检查原本各自重复读取本机归属应用，
 * 这里统一为带短 TTL（一个检查周期）的内存快照：稳态下每个检查周期只访问一次数据库，不新增数据库访问。
 * <p>
 * 每次刷新快照时与上一轮对比，将「上一轮没有、这一轮出现」的应用（即本机刚接管的应用）
 * 通知 {@link BroadcastDispatchWarmupService}，用于广播派发预热的触发；
 * 不修改 ServerElectionService，不新增持久化字段，接管识别最多延迟一个检查周期。
 * <p>
 * 注意：本类只服务于本机后台检查/调度循环的归属判断。TaskTracker 状态上报不受归属限制——
 * 非负责方收到上报照常落库（DB 是共享事实源，负责方的检查循环自然可见），
 * 不转发、不丢弃，也不为此新增集群通信（@DesignateServer 的远程转发机制仅保留给查询类方法）。
 *
 * @author tjq
 * @since 2026/11/23
 */
@Slf4j
@Service
public class ServerOwnershipService {

    private final AppInfoRepository appInfoRepository;

    private final TransportService transportService;

    private final BroadcastDispatchWarmupService broadcastDispatchWarmupService;

    private final Clock clock;

    /**
     * 归属快照的有效期，默认与状态检查周期一致
     */
    private final long snapshotTtlMs;

    /**
     * 本机归属应用快照（仅本机内存）
     */
    private volatile List<Long> ownedAppIds = Collections.emptyList();

    private volatile long snapshotLoadTime = -1L;

    @Autowired
    public ServerOwnershipService(AppInfoRepository appInfoRepository,
                                  TransportService transportService,
                                  BroadcastDispatchWarmupService broadcastDispatchWarmupService) {
        this(appInfoRepository, transportService, broadcastDispatchWarmupService,
                Clock.systemUTC(), InstanceStatusCheckService.CHECK_INTERVAL);
    }

    /**
     * 测试专用构造器，注入时钟与快照有效期
     */
    ServerOwnershipService(AppInfoRepository appInfoRepository,
                           TransportService transportService,
                           BroadcastDispatchWarmupService broadcastDispatchWarmupService,
                           Clock clock, long snapshotTtlMs) {
        this.appInfoRepository = appInfoRepository;
        this.transportService = transportService;
        this.broadcastDispatchWarmupService = broadcastDispatchWarmupService;
        this.clock = clock;
        this.snapshotTtlMs = snapshotTtlMs;
    }

    /**
     * 本机当前负责的全部应用（带一个检查周期的缓存，供四个状态检查共用）
     */
    public List<Long> listOwnedAppIds() {
        long now = clock.millis();
        if (snapshotExpired(now)) {
            refreshSnapshot(now);
        }
        return ownedAppIds;
    }

    /**
     * 判断指定应用当前是否由本机负责
     */
    public boolean isOwnedByCurrentServer(Long appId) {
        return listOwnedAppIds().contains(appId);
    }

    private synchronized void refreshSnapshot(long now) {
        // double-check，避免并发刷新
        if (!snapshotExpired(now)) {
            return;
        }
        final String localAddress = transportService.defaultProtocol().getAddress();
        List<Long> currentAppIds = appInfoRepository.listAppIdByCurrentServer(localAddress);
        List<Long> current = currentAppIds == null ? Collections.emptyList() : new ArrayList<>(currentAppIds);
        List<Long> previous = this.ownedAppIds;
        this.ownedAppIds = current;
        this.snapshotLoadTime = now;
        // 差异识别接管：上一轮没有、这一轮出现的应用即为刚接管（含本机重启后的首次发现，属有意行为）
        Set<Long> newlyOwned = new HashSet<>(current);
        newlyOwned.removeAll(previous);
        if (!newlyOwned.isEmpty()) {
            log.info("[ServerOwnership] this server({}) newly takes over apps: {}, total owned: {}.", localAddress, newlyOwned, current.size());
        }
        broadcastDispatchWarmupService.onOwnershipSnapshot(new HashSet<>(current), newlyOwned);
    }

    private boolean snapshotExpired(long now) {
        return snapshotLoadTime < 0 || now - snapshotLoadTime >= snapshotTtlMs;
    }
}
