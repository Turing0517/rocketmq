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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 首先在一次消息发送过程中，可能会多次执行选择消息队列这个方法，lastBrokerName就是上次选择的执行发送消息失败的Broker。
 * 第一次执行消息队列选择时，lastBrokerName为null，此时直接用sendWhichQueue自增在获取值，与当前路由表中消息队列个数取模，
 * 返回该位置的MessageQueue(selectOneMessageQueue()方法），如果消息发送再失败的话，下次进行消息队列选择时规避上次MessageQueue所
 * 在的Broker，否则还是很有可能再次失败。
 * 该算法在一次消息发送过程中能成功规避故障的Broker，但是如果Broker宕机，由于路由算法中的消息队列是按Broker排序的，如果上一次根据路由算法选择
 * 的是宕机Broker的第一个队列，那么随后的下次选择时宕机Broker的第二个队列，消息发送很有可能会事变，再次引发重试，带来不必要的性能损耗。
 * 首先，NameServer不会检测到Broker宕机后马上推送消息给消息生产者，而是消息生产者每个30s更新一次路由信息，所以消息生产者最快感知Broker最新
 * 路由信息是30s。故引入了故障延迟机制
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;
    /**
     * 根据CurrentLatency本次消息发送延迟，从LatencyMax尾部向前找到一个比CurrentLatency小的索引index，若干没有找到返回0
     * 然后根据这个索引从notAvailableDuration数组中取出对应时间，在这个时长内，Broker将设置为不可用。
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 1.根据对消息队列进行轮询获取一个消息对列
     * 2.验证该消息队列是否可用，latencyFaultTolerance。isAvaiable（mq.getBrokerName())是关键
     * 3.如果返回的MessageQueue可用，移除latenFaultTolerance关于该topic条目，表明Broker故障已经恢复。
     * @param tpInfo
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        /**
         *  选择消息队列由两种方式：
         * 1.sendLatencyFaultEnable = false,默认不启用Broker故障延迟机制
         * 2.sendLatencyFaultEnable = true ,启用Broker故障延迟机制
         */
        if (this.sendLatencyFaultEnable) {
            //开启sendLatencyFaultEnable，故障延迟机制
            try {
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }
                //如果所有的broker都不可用了，则选择一个least
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }
        //关闭故障延迟机制
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     *
     * @param brokerName
     * @param currentLatency
     * @param isolation 如果为true，则使用30s作为computeNotAvailableDuration的参数；如果为false，则使用本次消息的发送时延
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算因本次消息发送故障需要将Broker规避的时长，也就是接下来多久的时间内该Broker将不参与发送队列负载
     * 根据CurrentLatency本次消息发送延迟，从LatencyMax尾部向前找到一个比CurrentLatency小的索引index，若干没有找到返回0
     * 然后根据这个索引从notAvailableDuration数组中取出对应时间，在这个时长内，Broker将设置为不可用。
     *
     * @param currentLatency
     * @return
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
