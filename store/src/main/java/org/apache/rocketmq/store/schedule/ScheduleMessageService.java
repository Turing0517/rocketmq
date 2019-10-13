/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 定时消息是指消息发送到 Broker 后，并不立即被消费者消费而是要等到特定的时
 间后才能被消费， RocketMQ 并不支持任意的时间 精度， 如果要支持任 意 时间精度的
 定时调度，不可避免地需要在 Broker 层做消息排序（可以参考 JDK 并发包调度线程池
 ScheduledExecutorService 的实现原理），再加上持久化方面的考量，将不可避免地带来具大
 的性能消耗，所以 RocketMQ 只支持特定级别的延迟消息 。 消息延迟级别在 Broker 端通过
 messageDelayLevel 配置，默认为 ” ls 5s 10s 30s lm 2m 3m 4m 5m 6m 7m 8m 9m lOm 20rrl
 30m lh 2h，delayLevel = 1 表示1s，delayLevel =2 表示延迟5s，依次类推，说的定时任务
 上文提到的消息重试正是借助定时任务实现的，在将消息存入 commitlog 文件之前需要判
 断消息的重试次数 ，如果大于 0 ，则会将消息的 主题设置为 SCHEDULE TOPIC XXXX 。
 RocketMQ 定时消息 实现类为 org.apache . rocketmq. store.schedule. ScheduleMessageService 。
 该类的实例在 DefaultMessageStore 中创建 ，通过在 DefaultMessageStore 中调用 load 方法
 加载并调用 start 方法进行启动 。
 ScheduleMessageSerice方法的调用顺序：构造方法 --->load() --->start()

 定时消息的第二个设计关键点 ： 消息存储时如果消息的延迟级别属性 delayLevel 大
 于 0 ，则会各份原主题 、 原队列到消息属性 中，其 键分别为 PROPERTY . REAL TOPIC 、
 PROPERTY_REAL_QUEUE_ID ， 通过为不同的延迟级别创建不同的调 度任 务 ， 当时间到
 达后执行调度任务 ， 调度任 务 主要就是根据延迟拉取消息消费进度从延迟队列中拉取消息，
 然后从 comm itlog 中 加载完整消息，清除延迟级别属性并恢复原先的主题、队列，再次创
 建一条新的消息存入到 commitlog 中并转发到消息消费队列供消息消费者消费 。

 流程如下：
 1 ）消息消费者发送消息 ，如 果发送消息 的 delayLevel 大于 0 ，则改 变消息主题为
 SCHEDULE_TOPIC_XXXX ，消息队列为 delayLevel 减 l 。
 2 ）消息经由 commitlog 转发到消息消费队列 SCHED叽E TOPIC_XXXX 的消息消费队列 0。
 3 ）定时任务 Time 每隔 Is 根据上次拉取偏移量从消费 队列中取出所有消息 。
 4 ）根据消息的物理偏移量与消息大小从 CommitLog 中拉取消息 。
 5 ）根据消息 属 性 重新创 建消息，并恢复原主题 topicA 、 原队列 ID ，清除 d巳layLevel
 属性，存入 commitlog 文件 。
 6 ）转发到原主题 topicA 的消息消 费 队列，供消息消 费者消费 。
 */
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //定时消息统一主题
    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    //第一次调度时延的时间，默认为1s
    private static final long FIRST_DELAY_TIME = 1000L;
    //每一延时级别调度依次后延迟该时间间隔后再放入调度池
    private static final long DELAY_FOR_A_WHILE = 100L;
    //发送异常后延迟该时间在继续参与调度
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    /**
     * 延迟级别和延迟时间对应关系
     */
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);
    /**
     * 延迟级别消息消费进度
     */
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);
    /**
     * 默认消息存储器
     */
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Timer timer;
    private MessageStore writeMessageStore;
    //MessageStoreConfig#messageDelayLevel中最大消息延迟级别
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore
     *     the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    /**
     * start根据延迟级别穿件对应的定时任务，启动定时任务持久化延迟消息队列进度存储
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            this.timer = new Timer("ScheduleMessageTimerThread", true);
            /**
             * 根据延迟队列创建定时任务，遍历延迟级别，根据延迟级别Level从offsetTable中获取消费队列的消费进度，如果不存在，则使用0
             * 。也就是说每一个延迟级别对应一个消息消费队列。然后创建定时任务，每一个定时任务第一次启动默认延迟1s先执行一次定时任务，第二次
             * 调度开始使用相应的延迟时间。延迟级别与消息消费队列的映射关系为：消息队列ID = 延迟级别 - 1
             定时消息的第 一 个设计关键点是 ， 定时消息单独一个 主 题 ： SCHEDULE TOPIC_xxxx ， 该主题 下 队列数量等于
             MessageStoreConfig#messageDelayLevel 配置的延迟级别数量 ， 其对应关系为 queueld 等于延迟级别减 1 。
             ScheduleMessageS巳rvice 为每 一 个延迟级别创建 一个定时 Timer 根据延迟级别对应的延迟时间进行延迟调度 。
             在消息发送时 ， 如果消息的延迟级别 delayLevel 大于 0 ， 将消息的原主题名称、队列 ID 存入消息的属性中，然后
             改变消息的 主 题、队列与延迟主题与延迟 主 题所属队列 ， 消息将最终转发到延迟队列的消费队列 。
             */
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                if (timeDelay != null) {
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }
            /**
             * 创建定时任务，每隔10s持久化一次延迟队列的消息消费进度（延迟消息调进度），持久化频率可以通过fushDelayOffsetInterval
             * 配置属性进行设置
             */
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) ScheduleMessageService.this.persist();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer)
                this.timer.cancel();
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    /**
     * 该方法主要完成延迟消息消费队列消息进度的加载与delayLevelTable数据的构造，延迟队列消息消费进度默认存储路径为
     * ${ROCKET_HOME}/store/config/delayOffset.json，存储格式为：
     * {
     *     "offsetTable":{12:0,6:0,13:0,5:1,18:0,7:0,8:0,17:0,9:0,10:0,16:0,15:0,14:0,3:22,4:1}
     * }
     * @return
     */
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 解析MessageStoreConfig#messageDelayLevel定义的延迟级别转换为Map，延迟级别1，2，3等对应的延迟时间
     * @return
     */
    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    /**
     * ScheduleMessageService的start方法启动后，会为每一个延迟级别创建一个调度任务，每一个延迟级别其实对应SCHEDULE_TOPIC_XXX主题
     * 下的一个消息消费队列。定时调度任务的实现类为DeliverDelayedMessageTimerTask
     */
    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        /**
         * 定时调度类的核心
         */
        public void executeOnTimeup() {
            /**
             * 根据队列ID与延迟主题查找消息消费队列，如果未找到，说明目前并不存在该延迟级别的消息，忽略本兮任务，更具延时级别创建
             * 下一次调度任务。
             */
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                /**
                 * 根据offset从消息消费队列中获取当前队列中所有有效的消息。如果未找到，则更新一下延迟队列定时拉取进度并创建定时任务
                 * 待下一次继续尝试
                 */
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        /**
                         * 遍历ConsumeQueue，每一个标准ConsumeQueue条目为20个字节。解析出消息的物理偏移量，消息长度，消息
                         * taghashcode，为从commitlog加载具体的消息做准备
                         */
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                        tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            long countdown = deliverTimestamp - now;

                            if (countdown <= 0) {
                                /**
                                 * 根据消息物理偏移量与消息大小从CommitLog文件中查找消息。如果未找到消息，打印错误日志，根据延迟时间
                                 * 创建下一个定时器
                                 */
                                MessageExt msgExt =
                                    ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                        offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        /**
                                         * 根据消息重新构建新的消息对象，清除消息的延迟级别属性（delayLevel)、并回复消息原先的消息
                                         * 主题与消息消费队列，消息的消费次数reconsumeTime并不会丢失
                                         */
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        /**
                                         * 将消息再次存入到CommitLog，并转发到主题对应的消息队列上，供消费者再次消费。
                                         */
                                        PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.writeMessageStore
                                                .putMessage(msgInner);

                                        if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(
                                                new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                    nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me



                                         */
                                        log.error(
                                            "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                //更新延迟队列拉取进度
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown);
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        //更新延迟队列拉取进度
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                            + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
