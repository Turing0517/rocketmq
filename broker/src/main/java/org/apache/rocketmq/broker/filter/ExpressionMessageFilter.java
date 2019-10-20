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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 表达式消息过滤器
 */
public class ExpressionMessageFilter implements MessageFilter {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);
    //订阅信息数据
    protected final SubscriptionData subscriptionData;
    //消费过滤数据
    protected final ConsumerFilterData consumerFilterData;
    //消费过滤管理
    protected final ConsumerFilterManager consumerFilterManager;
    //布隆数据验证
    protected final boolean bloomDataValid;

    public ExpressionMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData,
        ConsumerFilterManager consumerFilterManager) {
        this.subscriptionData = subscriptionData;
        this.consumerFilterData = consumerFilterData;
        this.consumerFilterManager = consumerFilterManager;
        if (consumerFilterData == null) {
            bloomDataValid = false;
            return;
        }
        //获取布隆过滤器
        BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
        //验证消费过滤器数据里的布隆数据是否有效
        if (bloomFilter != null && bloomFilter.isValid(consumerFilterData.getBloomFilterData())) {
            bloomDataValid = true;
        } else {
            bloomDataValid = false;
        }
    }

    /**
     * 根据ConsumeQueue进行消息过滤时，只对比tag的hashcode，所以基于TAG模式消息果粒橙，还需要在消费端对消息tag进行精确匹配
     * @param tagsCode tagsCode 消息tag的hashCode
     * @param cqExtUnit extend unit of consume queue ConsumeQueue条目扩展属性
     * @return
     */
    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        /**
         * 如果订阅消息未空，返回true，不过滤
         */
        if (null == subscriptionData) {
            return true;
        }
        //如果是类过滤模式，返回true
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // by tags code.

        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            /**
             * 如果是TAG过滤模式，并且消息的tagsCode为空或tagsCode小于0,返回true，说明消息在发送时没有设置tag。
             */
            if (tagsCode == null) {
                return true;
            }

            if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
                return true;
            }
            /**
             * 如果订阅信息TAG的hashcode集合中包含消息的tagsCode，返回true
             */
            return subscriptionData.getCodeSet().contains(tagsCode.intValue());
        } else {
            //不是tag过滤
            // no expression or no bloom 没有表达式，或没有布隆
            if (consumerFilterData == null || consumerFilterData.getExpression() == null
                || consumerFilterData.getCompiledExpression() == null || consumerFilterData.getBloomFilterData() == null) {
                return true;
            }

            // message is before consumer
            if (cqExtUnit == null || !consumerFilterData.isMsgInLive(cqExtUnit.getMsgStoreTime())) {
                log.debug("Pull matched because not in live: {}, {}", consumerFilterData, cqExtUnit);
                return true;
            }
            //获取位图
            byte[] filterBitMap = cqExtUnit.getFilterBitMap();
            //获取布隆过滤器
            BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
            //位图为空，或布隆过滤器无效，或位图长度*8和布隆过滤器的位数相同，直接返回true
            if (filterBitMap == null || !this.bloomDataValid
                || filterBitMap.length * Byte.SIZE != consumerFilterData.getBloomFilterData().getBitNum()) {
                return true;
            }

            BitsArray bitsArray = null;
            try {
                bitsArray = BitsArray.create(filterBitMap);
                //进行布隆过滤器校验
                boolean ret = bloomFilter.isHit(consumerFilterData.getBloomFilterData(), bitsArray);
                log.debug("Pull {} by bit map:{}, {}, {}", ret, consumerFilterData, bitsArray, cqExtUnit);
                return ret;
            } catch (Throwable e) {
                log.error("bloom filter error, sub=" + subscriptionData
                    + ", filter=" + consumerFilterData + ", bitMap=" + bitsArray, e);
            }
        }

        return true;
    }

    /**
     * 该方法主要是为表达式模式SQL92服务的，根据消息属性实现类似于数据库SQL Where条件过滤方式。
     * @param msgBuffer message buffer in commit log, may be null if not invoked in store.
     *                  消息内容，如果为空，该方法返回true
     * @param properties message properties, should decode from buffer if null by yourself.
     * @return
     */
    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        /**
         * 如果订阅信息未空，返回true
         */
        if (subscriptionData == null) {
            return true;
        }
        /**
         * 如果类过滤模式，返回true
         */
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }
        /**
         * 如果是TAG模式，返回true
         */
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }

        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;

        // no expression
        if (realFilterData == null || realFilterData.getExpression() == null
            || realFilterData.getCompiledExpression() == null) {
            return true;
        }

        if (tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }

        Object ret = null;
        try {
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);

            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }

        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);

        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }

        return (Boolean) ret;
    }

}
