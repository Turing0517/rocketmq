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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * 索引文件类
 * RocketMQ引入了Hash索引机制为消息建立索引，HashMap的设计包含两个基本点：Hash槽与hash冲突链表结构。
 * 索引文件内容：
 * IndexHeader 40字节
 * 500w个hash槽
 * 2000w个index条目
 *      hashCode 4字节
 *      phyoffset 8字节
 *      timedif 4字节
 *      pre index no 4字节
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;//hash槽数目，默认时500万
    /**
     * index条目默认时2000万条
     * index条目：
     *      hashCode:key的hashcode
     *      phyoffset : 消息对应的物理偏移量
     *      timedif:该消息存储时间与第一条消息的时间戳的差值，小于0，该消息无效
     *      preIndexNo:该条目的前一条记录的Index索引，当出现hash冲突时，构建的链表结构
     */
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    /**
     * 头部，40字节，记录该indexFile的统计信息，其结构如下：
     *      beginTimestamp:该索引文件中包含消息的最小存储时间
     *      endTimestamp:该索引文件中包含消息的最大存储时间
     *      beginPayoffset : 该索引文件中最小的物理偏移量（commitlog文件偏移量）
     *      endPayoffset:该索引文件中最大物理偏移量（commitlog文件偏移量）
     *      hashslotCount:hashslolt个数，并不是hash槽使用个数，在这里意义不大
     *      indexCount:Index 条目列表当前已使用的个数，index条目在Index条目列表中按顺序存储
     */
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 将消息索引键与消息偏移量映射关系写入IndexFile
     * @param key
     * @param phyOffset
     * @param storeTimestamp
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        /** 如果当前索引文件未写满则根据key算出可以的hashCode,然后keyHash对hash槽数量取余定位到hashCode对应的hash槽下标，
         * hashCode对应的hash槽的物理地址为IndexHeader头部（40字节）加上下标乘以每个hash槽的消息（4字节）
         */
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                /**
                 * 读取hash槽中存储的数据，如果hash槽存储的数据小于0或大于当前索引文件中的索引条目格式，则将slotValue
                 * 设置为0
                 */
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                //计算待存储消息的时间戳与第一条消息时间戳的差值，并转换成秒
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                /**
                 * 将条目信息存储在IndexFile中
                 * 1.计算新添加条目的起始物理偏移量，等于头部字节长度 + hash槽数量 * 单个hash槽大小（4字节） + 当前index条目
                 * 个数 * 单个index条目大小（20个字节）
                 * 2.依次将hashcode,消息物理偏移量，消息存储时间戳与索引文件时间戳，当前Hash槽的值存入MappedByteBuffer中。
                 * 3.将当前Index中包含的条目数量存入Hash槽中，将覆盖原先Hash槽的值。
                 *
                 * 这里Hash冲突链式解决方案的关键实现，Hash槽中存储的是该HashCode所对应的最新的Index条目下标，新的index条目的最后
                 * 4个字节存储该HashCode上一个条目的Index下标。如果Hash槽中存储的值为0或者大于当前IndexFile最大条目或小于-1，表示该
                 * Hash槽当前并没有与之对应的Index条目。值得关注的是，IndexFile条目中存储的不是消息索引key而是消息属性key的HashCode
                 * 在根据key查找时，需要根据消息物理偏移量找到消息进而再验证消息key的值，之所以只存储HashCode而不存储具体的key，是为了
                 * 将Index条目设计成定长结构，才能方便检索与定位条目。
                 */
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                /**
                 * 更新文件索引头信息。
                 */
                //如果当前文件只包含一个条目，更新beginPHYOffset与beginTimestamp
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }
                //更新endPhyOffset，endTimestamp，当前文件使用索引条目等信息。
                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            //如果当前已使用条目大于等于允许最大条目数时，则返回false，表示当前索引文件已经写满
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 根据索引key查找消息
     * @param phyOffsets 查找到的消息物理偏移量
     * @param key 索引key
     * @param maxNum 本次查找最大消息条数
     * @param begin 开始时间戳
     * @param end 结束时间戳
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            /**
             * 根据key算出key的HashCode，然后keyhash对hash槽数量取余定位到hashcode对应的hash槽下标，HashCode对应的hash槽的
             * 物理位置地址为IndexHeader头部（40字节）加上下标 * 每个hash槽的大小（4字节）
             */
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }
                /**
                 * 如果对应的hash槽中存储的数据小于1或者大于当前索引条目个数则表示该HashCode没有对应的条目直接返回
                 */
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    /**
                     * 由于会存在hash冲突，根据slotValue定位该hash槽最新的一个Item条目，将存储的物理偏移加入到phyOffset中，然后
                     * 继续验证Item条目中存储的上一个Index下标，如果大于等于1并且小于最大条目数，则继续查找，否则结束查找
                     */
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        /**
                         * 根据Index下标定位到条目的起始物理偏移量，然后依次读取HashCode，物理偏移量，时间差，上一个条目的Index下标。
                         */
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        /**
                         * 如果存储的时间差小于0，则直接结束；如果HashCode匹配并且消息存储时间介于待查找时间start、end之间则将
                         * 消息物理偏移量加入到phyOffsets，并验证条目的前一个Index索引，如果索引大于等于1并且小于index条目数，继续
                         * 查找，否则结束整个查找
                         */
                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;
                        /**
                         * 如果HashCode匹配并且消息存储时间介于待查找时间start，end之间则将消息物理偏移量加入到phyOffsets,并验证条目
                         * 的前一个index索引
                         */
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }
                        //如果索引大于等于1并且小于Index条目，则继续查找，否则结束整个查找
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;

                    }

                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
