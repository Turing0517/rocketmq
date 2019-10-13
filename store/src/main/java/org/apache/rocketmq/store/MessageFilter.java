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
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 消息过滤机制
 RocketMQ 支持表达式过滤与类过滤两种模式 ，其 中 表达式又分为 TAG 和 SQL92 。 类
 过滤模式允许提交－个过滤类到 FiIterServer ，消息消费者从 FilterServer 拉取消息，消息经
 过 FilterServer 时会执行过滤逻辑 。 表达式模式分为 TAG 与 SQL92 表达式， SQL92 表达式
 以消息属性过滤上下文， 实现 SQL 条件过滤表达式而 TAG 模式就是简单为消息定义标签，
 根据消息属性 tag 进行匹配 。
 消息过滤，在订阅的时候就过滤了
 * 消息发动者在消息发送时，如果设置了消息的tag属性，存储在消息属性中，先存储在CommitLog文件中，然后转发到消息队列，消息消费队列会用
 * 8个字节存储消息tag的hashcode，之所以不直接存储tag字符串，是因为将ConsumeQueue设计为定长结构，加快消息的加载性能。在Broker端拉取
 * 消息时，遍历ConsumeQueue，只对比消息tag的HashCode，如果匹配成功则返回，否则忽略该消息。Consumer在收到消息后，同样需要先对消息过滤
 * 只是此时比较的是消息tag的值而不再是hashcode
 */
public interface MessageFilter {
    /**
     * match by tags code or filter bit map which is calculated when message received
     * and stored in consume queue ext.
     * 根据ConsumeQueue判断消息是否匹配
     * @param tagsCode tagsCode 消息tag的hashCode
     * @param cqExtUnit extend unit of consume queue ConsumeQueue条目扩展属性
     *
     */
    boolean isMatchedByConsumeQueue(final Long tagsCode,
        final ConsumeQueueExt.CqExtUnit cqExtUnit);

    /**
     * match by message content which are stored in commit log.
     * <br>{@code msgBuffer} and {@code properties} are not all null.If invoked in store,
     * {@code properties} is null;If invoked in {@code PullRequestHoldService}, {@code msgBuffer} is null.
     *  根据存储在commitlog文件中的内容判断消息是否匹配
     * @param msgBuffer message buffer in commit log, may be null if not invoked in store.
     *                  消息内容，如果为空，该方法返回true
     * @param properties message properties, should decode from buffer if null by yourself.
     *                   消息属性，主要用于表达式SQL92过滤模式
     */
    boolean isMatchedByCommitLog(final ByteBuffer msgBuffer,
        final Map<String, String> properties);
}
