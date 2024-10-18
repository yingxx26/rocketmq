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

import org.apache.rocketmq.client.MessageStorage;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * mq消息发送故障策略（重写覆盖了rocketmq）
 *
 * @author mobai
 */

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();

    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();
    private boolean sendLatencyFaultEnable = false;
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    private final ConcurrentHashMap<TopicPublishInfo, TopicPublishInfoCache> topicPublishInfoCacheTable = new ConcurrentHashMap<TopicPublishInfo, TopicPublishInfoCache>();

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

    private TopicPublishInfoCache checkCacheChanged(TopicPublishInfo topicPublishInfo) {
        if (topicPublishInfoCacheTable.containsKey(topicPublishInfo)) {
            return topicPublishInfoCacheTable.get(topicPublishInfo);
        }
        synchronized (this) {
            TopicPublishInfoCache cache = new TopicPublishInfoCache();
            List<MessageQueue> canaryQueues = MessageStorage.getCanaryQueues(topicPublishInfo.getMessageQueueList());
            List<MessageQueue> normalQueues = MessageStorage.getNormalQueues(topicPublishInfo.getMessageQueueList());
            Collections.sort(canaryQueues);
            Collections.sort(normalQueues);
            cache.setCanaryQueueList(canaryQueues);
            cache.setNormalQueueList(normalQueues);
            topicPublishInfoCacheTable.putIfAbsent(topicPublishInfo, cache);
        }
        return topicPublishInfoCacheTable.get(topicPublishInfo);
    }


    /**
     * 队列选择策略
     * 如果当前是灰度环境,则发送到灰度分区
     * 如果开启了故障规避,则选择一个可用的消息队列(不包含灰度分区)
     * 如果没有开启故障规避,则选择一个消息队列(不包含灰度分区)
     *
     * @param tpInfo         消息队列信息
     * @param lastBrokerName 上次发送失败的brokerName
     * @return 选择的消息队列
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
       // TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
       // List<MessageQueue> messageQueueList = tpInfo.getMessageQueueList();
        TopicPublishInfoCache topicPublishInfoCache = checkCacheChanged(tpInfo);
        //灰度的场景下,发送消息到灰度分区
        if (MessageStorage.isCanaryRelease()) {
            MessageQueue messageQueue = selectDefaultMessageQueue(tpInfo, lastBrokerName, topicPublishInfoCache.getCanaryQueueList());
            /*if (log.isDebugEnabled()) {
                log.debug("canary context,send message to canary queue:{}", messageQueue.getBrokerName() + messageQueue.getQueueId());
            }*/
            return messageQueue;
        } else {
            //开启了故障规避
            if (this.sendLatencyFaultEnable) {
                try {
                    int index = tpInfo.getSendWhichQueue().incrementAndGet();
                    int size = topicPublishInfoCache.getNormalQueueList().size();
                    for (int i = 0; i < size; i++) {
                        int pos = Math.max(Math.abs(index++) % size, 0);
                        MessageQueue mq = topicPublishInfoCache.getNormalQueueList().get(pos);
                        if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                            return mq;
                        }
                    }
                    //如果没有找到可用的消息队列,则选择一个相对可靠的broker下的消息队列
                    final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                    int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                    if (writeQueueNums > 0) {
                        final MessageQueue mq = tpInfo.selectOneMessageQueue();
                        //避免发送到灰度分区,同时又保留故障规避机制
                        if (!topicPublishInfoCache.getCanaryQueueList().contains(mq)) {
                            if (notBestBroker != null) {
                                mq.setBrokerName(notBestBroker);
                                mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                            }
                            return mq;
                        }
                    } else {
                        latencyFaultTolerance.remove(notBestBroker);
                    }
                } catch (Exception e) {
                    log.error("Error occurred when selecting message queue", e);
                }
            }

            //按递增取模的方式实现轮询选择队列
            return selectDefaultMessageQueue(tpInfo, lastBrokerName, topicPublishInfoCache.getNormalQueueList());
        }

    }


    /**
     * 默认的消息队列选择策略
     * 尽可能避开上次发送失败的brokerName
     *
     * @param topicPublishInfo 消息队列信息
     * @param lastBrokerName   上次发送失败的brokerName
     * @return 选择的消息队列
     */
    private MessageQueue selectDefaultMessageQueue(final TopicPublishInfo topicPublishInfo, final String lastBrokerName,
                                                   List<MessageQueue> queues) {
        ThreadLocalIndex sendWhichQueue = topicPublishInfo.getSendWhichQueue();
        int size = queues.size();
        if (lastBrokerName != null) {
            for (int i = 0; i < size; i++) {
                int index = sendWhichQueue.incrementAndGet();
                int pos = Math.max(Math.abs(index) % size, 0);
                MessageQueue mq = queues.get(pos);
                //如果不是上次发送失败的brokerName,则返回
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
        }
        //如果没有找到不是上次发送失败的brokerName,则随机返回一个
        int i = sendWhichQueue.incrementAndGet();
        int res = Math.max(Math.abs(i) % size, 0);
        log.debug("selectDefaultMessageQueue, lastBrokerName:{}, res:{}", lastBrokerName, topicPublishInfo.getMessageQueueList().get(res));

        return queues.get(res);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i]) {
                return this.notAvailableDuration[i];
            }
        }

        return 0;
    }


    private static class TopicPublishInfoCache {

        /**
         * 灰度消息队列列表
         */
        private List<MessageQueue> canaryQueueList;


        private List<MessageQueue> normalQueueList;


        public List<MessageQueue> getCanaryQueueList() {
            return canaryQueueList;
        }

        public void setCanaryQueueList(List<MessageQueue> canaryQueueList) {
            this.canaryQueueList = canaryQueueList;
        }

        public List<MessageQueue> getNormalQueueList() {
            return normalQueueList;
        }

        public void setNormalQueueList(List<MessageQueue> normalQueueList) {
            this.normalQueueList = normalQueueList;
        }

    }
}