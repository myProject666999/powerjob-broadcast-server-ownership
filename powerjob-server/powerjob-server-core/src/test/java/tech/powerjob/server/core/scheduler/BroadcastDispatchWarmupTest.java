package tech.powerjob.server.core.scheduler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.powerjob.common.SystemInstanceResult;
import tech.powerjob.common.enums.ExecuteType;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.enums.SwitchableStatus;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.server.common.module.WorkerInfo;
import tech.powerjob.server.core.DispatchService;
import tech.powerjob.server.core.instance.InstanceManager;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.model.JobInfoDO;
import tech.powerjob.server.persistence.remote.repository.AppInfoRepository;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;
import tech.powerjob.server.remote.transporter.ProtocolInfo;
import tech.powerjob.server.remote.transporter.TransportService;
import tech.powerjob.server.remote.worker.WorkerClusterQueryService;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 广播派发预热测试：纯 Mockito + 注入时钟，不起数据库和网络。
 * 覆盖：接管为空、逐步增长、到期不完整、非广播不受影响、暂缓不消耗重试、
 * 只处理本机归属应用、重复换手重新计时、覆盖信息文案。
 *
 * @author tjq
 * @since 2026/11/23
 */
class BroadcastDispatchWarmupTest {

    private static final long APP_ID = 10L;
    private static final long JOB_ID = 30L;
    private static final long INSTANCE_ID = 40L;
    private static final long CHECK_INTERVAL = 10_000L;

    /**
     * 可手动推进的时钟，注入被测服务，替代真实时间
     */
    private static final class MutableClock extends Clock {
        private long millis;

        private MutableClock(long initial) {
            this.millis = initial;
        }

        private void advance(long ms) {
            millis += ms;
        }

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private final MutableClock clock = new MutableClock(1_000_000L);
    private final AppInfoRepository apps = mock(AppInfoRepository.class);
    private final TransportService transport = mock(TransportService.class);
    private final WorkerClusterQueryService workers = mock(WorkerClusterQueryService.class);
    private final InstanceInfoRepository instances = mock(InstanceInfoRepository.class);
    private final InstanceManager completion = mock(InstanceManager.class);

    private BroadcastDispatchWarmupService warmup;
    private ServerOwnershipService ownership;
    private DispatchService dispatch;
    private JobInfoDO broadcastJob;
    private InstanceInfoDO instance;

    @BeforeEach
    void setUp() {
        ProtocolInfo protocol = new ProtocolInfo();
        protocol.setAddress("server-a:10010");
        when(transport.defaultProtocol()).thenReturn(protocol);
        // 大上限，避免测试内时间轮任务真实触发；到期场景通过推进时钟覆盖
        warmup = new BroadcastDispatchWarmupService(workers, true, 600_000L, 2, clock);
        ownership = new ServerOwnershipService(apps, transport, warmup, clock, CHECK_INTERVAL);
        dispatch = new DispatchService(transport, workers, completion, null, instances, null, warmup);

        broadcastJob = new JobInfoDO();
        broadcastJob.setId(JOB_ID);
        broadcastJob.setAppId(APP_ID);
        broadcastJob.setStatus(SwitchableStatus.ENABLE.getV());
        broadcastJob.setExecuteType(ExecuteType.BROADCAST.getV());
        broadcastJob.setTimeExpressionType(TimeExpressionType.CRON.getV());
        broadcastJob.setMaxInstanceNum(0);
        broadcastJob.setMaxWorkerCount(0);

        instance = new InstanceInfoDO();
        instance.setId(20L);
        instance.setAppId(APP_ID);
        instance.setJobId(JOB_ID);
        instance.setInstanceId(INSTANCE_ID);
        instance.setStatus(InstanceStatus.WAITING_DISPATCH.getV());
    }

    private void ownedApps(Long... appIds) {
        when(apps.listAppIdByCurrentServer("server-a:10010")).thenReturn(Arrays.asList(appIds));
        ownership.listOwnedAppIds();
    }

    private void advanceOneCheckCycle() {
        clock.advance(CHECK_INTERVAL);
        ownership.listOwnedAppIds();
    }

    private static WorkerInfo worker(String address) {
        WorkerInfo workerInfo = new WorkerInfo();
        workerInfo.setAddress(address);
        return workerInfo;
    }

    @Test
    void takeoverWithEmptyCandidatesDefersThenFailsByExistingPathAfterWarmup() {
        // 接管应用 10，但本机还没有任何 Worker 心跳（对应 issue 场景：S1 宕机、广播到点、S2 只收到部分心跳）
        when(workers.getAllAliveWorkers(APP_ID)).thenReturn(Collections.emptyList());
        ownedApps(APP_ID);
        assertTrue(warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()) > 0, "newly taken over app must warm up");

        // 预热期内广播实例被暂缓：不判失败、不查询候选、expectedTriggerTime 前移
        dispatch.dispatch(broadcastJob, INSTANCE_ID, Optional.of(instance), Optional.empty());
        verify(workers, never()).geAvailableWorkers(any());
        verify(instances, never()).update4TriggerFailed(anyLong(), anyInt(), anyLong(), anyLong(), any(), any(), any());
        verify(completion, never()).processFinishedInstance(anyLong(), any(), any(), any());
        ArgumentCaptor<Long> newTriggerTime = ArgumentCaptor.forClass(Long.class);
        verify(instances).updateExpectedTriggerTimeByInstanceIdAndOriginStatus(eq(INSTANCE_ID), newTriggerTime.capture(),
                any(), eq(InstanceStatus.WAITING_DISPATCH.getV()));
        assertTrue(newTriggerTime.getValue() > System.currentTimeMillis(),
                "deferred instance must be pushed out of the DISPATCH_TIMEOUT window");
        assertEquals(InstanceStatus.WAITING_DISPATCH.getV(), instance.getStatus());

        // 预热到期后候选仍为空：按现有逻辑判 NO_WORKER_AVAILABLE 失败（普通失败路径与告警不变）
        clock.advance(600_001L);
        advanceOneCheckCycle();
        assertEquals(0, warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()), "warmup must end after the capped duration");
        dispatch.dispatch(broadcastJob, INSTANCE_ID, Optional.of(instance), Optional.empty());
        verify(instances).update4TriggerFailed(eq(INSTANCE_ID), eq(InstanceStatus.FAILED.getV()), anyLong(), anyLong(), any(),
                eq(SystemInstanceResult.NO_WORKER_AVAILABLE), any());
        verify(completion).processFinishedInstance(INSTANCE_ID, null, InstanceStatus.FAILED, SystemInstanceResult.NO_WORKER_AVAILABLE);
    }

    @Test
    void growingWorkerSetResetsStabilityAndWarmupEndsAfterStableRounds() {
        List<WorkerInfo> discovered = new ArrayList<>();
        discovered.add(worker("worker-1:27777"));
        when(workers.getAllAliveWorkers(APP_ID)).thenAnswer(call -> new ArrayList<>(discovered));

        ownedApps(APP_ID); // 第 1 轮：发现 worker-1，集合增长，稳定计数归零
        assertTrue(warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()) > 0);

        discovered.add(worker("worker-2:27777"));
        advanceOneCheckCycle(); // 第 2 轮：集合继续增长，稳定计数再次归零
        assertTrue(warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()) > 0);

        advanceOneCheckCycle(); // 第 3 轮：不再增长，稳定计数 = 1
        assertTrue(warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()) > 0,
                "one stable round is not enough to finish warmup");

        advanceOneCheckCycle(); // 第 4 轮：连续 2 轮不增长，预热结束
        assertEquals(0, warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()),
                "two consecutive stable rounds must finish warmup");
    }

    @Test
    void shrinkingSetDoesNotExtendWarmup() {
        List<WorkerInfo> discovered = new ArrayList<>();
        discovered.add(worker("worker-1:27777"));
        discovered.add(worker("worker-2:27777"));
        when(workers.getAllAliveWorkers(APP_ID)).thenAnswer(call -> new ArrayList<>(discovered));

        ownedApps(APP_ID); // 第 1 轮：发现 2 台
        discovered.remove(0);
        advanceOneCheckCycle(); // 第 2 轮：集合缩小，不视为新发现，稳定计数 = 1
        assertTrue(warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()) > 0);
        advanceOneCheckCycle(); // 第 3 轮：仍无新地址，稳定计数 = 2，预热结束
        assertEquals(0, warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()),
                "shrinking must not reset the stability counter");
    }

    @Test
    void expiringWarmupWithStillGrowingSetStopsDeferring() {
        // 预热上限 20s（默认配置），候选集合每一轮都在增长：不能无限等待，到期按当时已知集合派发
        BroadcastDispatchWarmupService shortWarmup =
                new BroadcastDispatchWarmupService(workers, true, 20_000L, 2, clock);
        ServerOwnershipService shortOwnership =
                new ServerOwnershipService(apps, transport, shortWarmup, clock, CHECK_INTERVAL);
        List<WorkerInfo> discovered = new ArrayList<>();
        when(workers.getAllAliveWorkers(APP_ID)).thenAnswer(call -> {
            WorkerInfo extra = worker("worker-" + (discovered.size() + 1) + ":27777");
            discovered.add(extra);
            return new ArrayList<>(discovered);
        });
        when(apps.listAppIdByCurrentServer("server-a:10010")).thenReturn(Collections.singletonList(APP_ID));

        shortOwnership.listOwnedAppIds();
        clock.advance(CHECK_INTERVAL);
        shortOwnership.listOwnedAppIds();
        assertTrue(shortWarmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()) > 0);
        clock.advance(CHECK_INTERVAL); // 到达 20s 上限时集合仍在增长
        shortOwnership.listOwnedAppIds();
        assertEquals(0, shortWarmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()),
                "warmup must stop at the capped duration even if the set is still growing");
    }

    @Test
    void nonBroadcastJobsAreNotDeferredDuringWarmup() {
        when(workers.getAllAliveWorkers(APP_ID)).thenReturn(Collections.emptyList());
        ownedApps(APP_ID);
        assertTrue(warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()) > 0);

        // 普通任务候选为空仍按现有逻辑立即失败，不进入预热
        broadcastJob.setExecuteType(ExecuteType.STANDALONE.getV());
        dispatch.dispatch(broadcastJob, INSTANCE_ID, Optional.of(instance), Optional.empty());
        verify(instances, never()).updateExpectedTriggerTimeByInstanceIdAndOriginStatus(anyLong(), anyLong(), any(), anyInt());
        verify(instances).update4TriggerFailed(eq(INSTANCE_ID), eq(InstanceStatus.FAILED.getV()), anyLong(), anyLong(), any(),
                eq(SystemInstanceResult.NO_WORKER_AVAILABLE), any());
    }

    @Test
    void deferredInstanceDoesNotConsumeRetryOrTriggerTimeoutRedispatch() {
        when(workers.getAllAliveWorkers(APP_ID)).thenReturn(Collections.emptyList());
        ownedApps(APP_ID);

        dispatch.dispatch(broadcastJob, INSTANCE_ID, Optional.of(instance), Optional.empty());

        // 暂缓只是前移 expectedTriggerTime：实例仍处 WAITING_DISPATCH，runningTimes 不变，不发生重派
        assertNull(instance.getRunningTimes(), "deferral must not advance the retry counter");
        verify(instances, never()).updateStatusAndGmtModifiedByInstanceIdAndOriginStatus(anyLong(), anyInt(), anyInt(), any());
        verify(instances, never()).updateStatusAndGmtModifiedByInstanceIdListAndOriginStatus(anyList(), anyInt(), anyInt(), any());
        ArgumentCaptor<Long> newTriggerTime = ArgumentCaptor.forClass(Long.class);
        verify(instances).updateExpectedTriggerTimeByInstanceIdAndOriginStatus(eq(INSTANCE_ID), newTriggerTime.capture(),
                any(), eq(InstanceStatus.WAITING_DISPATCH.getV()));
        // DISPATCH_TIMEOUT(30s) 检查的条件是 expectedTriggerTime < now - 30s，
        // 前移后的触发时间在未来，检查窗口内不会被反复捞起超时重试
        long dispatchTimeoutMs = 30_000L;
        assertTrue(newTriggerTime.getValue() > System.currentTimeMillis() - dispatchTimeoutMs,
                "deferred instance must be invisible to the DISPATCH_TIMEOUT check during warmup");
    }

    @Test
    void onlyOwnedAppsAreWarmedAndOwnershipLossDropsState() {
        // 应用 10 的候选集合每轮增长（保持预热不结束），应用 20 始终无心跳
        List<WorkerInfo> app10Workers = new ArrayList<>();
        app10Workers.add(worker("worker-1:27777"));
        when(workers.getAllAliveWorkers(APP_ID)).thenAnswer(call -> new ArrayList<>(app10Workers));
        when(workers.getAllAliveWorkers(20L)).thenReturn(Collections.emptyList());

        // 应用 99 归其他 Server：从不进入预热
        ownedApps(APP_ID);
        assertEquals(0, warmup.broadcastDeferMs(99L, ExecuteType.BROADCAST.getV()));
        assertTrue(warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()) > 0);

        // 应用 20 划归本机：开始预热
        app10Workers.add(worker("worker-2:27777"));
        when(apps.listAppIdByCurrentServer("server-a:10010")).thenReturn(Arrays.asList(APP_ID, 20L));
        advanceOneCheckCycle();
        assertTrue(warmup.broadcastDeferMs(20L, ExecuteType.BROADCAST.getV()) > 0);

        // 应用 20 又划归其他 Server：预热状态被移除，本机不再暂缓其广播实例
        app10Workers.add(worker("worker-3:27777"));
        when(apps.listAppIdByCurrentServer("server-a:10010")).thenReturn(Collections.singletonList(APP_ID));
        advanceOneCheckCycle();
        assertEquals(0, warmup.broadcastDeferMs(20L, ExecuteType.BROADCAST.getV()),
                "apps no longer owned must be dropped from warmup");
        assertTrue(warmup.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()) > 0,
                "still-owned apps keep warming");
    }

    @Test
    void reTakeoverRestartsWarmupWithFreshCap() {
        when(workers.getAllAliveWorkers(anyLong())).thenReturn(Collections.emptyList());
        ownedApps(20L);
        // 失去归属，时钟走远（远超预热上限）
        when(apps.listAppIdByCurrentServer("server-a:10010")).thenReturn(Collections.emptyList());
        advanceOneCheckCycle();
        clock.advance(10_000_000L);
        // 再次接手：重新计时，预热仍有完整上限
        when(apps.listAppIdByCurrentServer("server-a:10010")).thenReturn(Collections.singletonList(20L));
        clock.advance(CHECK_INTERVAL);
        ownership.listOwnedAppIds();
        long deferMs = warmup.broadcastDeferMs(20L, ExecuteType.BROADCAST.getV());
        assertTrue(deferMs > 600_000L - CHECK_INTERVAL,
                "re-takeover must restart warmup with a fresh cap, but was: " + deferMs);
    }

    @Test
    void ownershipSnapshotIsCachedWithinOneCheckCycle() {
        when(apps.listAppIdByCurrentServer("server-a:10010")).thenReturn(Collections.singletonList(APP_ID));
        // 四个检查共用同一周期内的快照：稳态每周期仅一次 DB 读
        ownership.listOwnedAppIds();
        ownership.listOwnedAppIds();
        ownership.listOwnedAppIds();
        List<Long> owned = ownership.listOwnedAppIds();
        assertEquals(Collections.singletonList(APP_ID), owned);
        assertTrue(ownership.isOwnedByCurrentServer(APP_ID));
        assertFalse(ownership.isOwnedByCurrentServer(99L));
        verify(apps, times(1)).listAppIdByCurrentServer(anyString());
    }

    @Test
    void disabledWarmupNeverDefers() {
        BroadcastDispatchWarmupService disabled =
                new BroadcastDispatchWarmupService(workers, false, 20_000L, 2, clock);
        HashSet<Long> owned = new HashSet<>(Collections.singletonList(APP_ID));
        disabled.onOwnershipSnapshot(owned, owned);
        assertEquals(0, disabled.broadcastDeferMs(APP_ID, ExecuteType.BROADCAST.getV()));
    }

    @Test
    void coverageTextReportsKnownCountWithoutPseudoPreciseRatio() {
        String text = BroadcastDispatchWarmupService.coverageText(5);
        assertTrue(text.contains("knownWorkers=5"), "coverage text must carry the known worker count: " + text);
        assertFalse(text.contains("%"), "coverage text must not contain a pseudo-precise ratio: " + text);
    }
}
