package org.apache.rocketmq.client.consumer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.MessageStorage;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CanaryAllocateMessageQueueStrategyImpl implements AllocateMessageQueueStrategy {


    /**
     * 负载均衡策略
     * 若存在客户端为灰度客户端，则按照灰度客户端进行分配
     * 若所有客户端不存在灰度客户端，则按照平均分配策略进行分配
     *
     * @param consumerGroup current consumer group
     * @param currentCID    current consumer id
     * @param mqAll         message queue set in current topic
     * @param cidAll        consumer set in current consumer group
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return Collections.emptyList();
        }
        //重试的消息队列不参与灰度负载均衡,走默认的rocketmq策略
        //因为重试的消息是由客户端重新发送回broker的,走的不是默认的send逻辑,写到的是group的retry topic，写到哪个队列我们也不知道，所以无法进行灰度负载均衡
        if (mqAll.stream().anyMatch(mq -> mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))) {
            return allocateByAvg(consumerGroup, currentCID, mqAll, cidAll);
        }
        //如果不存在灰度服务，则按照平均分配策略进行分配
        if (!MessageStorage.hasCanaryRelease(cidAll)) {
            List<MessageQueue> allocate = allocateByAvg(consumerGroup, currentCID, mqAll, cidAll);
            /*if (log.isDebugEnabled()) {
                log.debug("topic:{} reBalance, no canary release client, allocate {} message queue by average strategy.\n" +
                                "current cid:{}\n" +
                                "result:\n{}",
                        mqAll.get(0).getTopic(),
                        allocate.size(),
                        currentCID,
                        allocate.stream()
                                .collect(Collectors.groupingBy(MessageQueue::getBrokerName))
                                .entrySet().stream()
                                .map(e -> e.getKey() + ": " + e.getValue().stream()
                                        .map(m -> String.valueOf(m.getQueueId()))
                                        .collect(Collectors.joining(", ")))
                                .collect(Collectors.joining("\n")));
            }*/
            return allocate;
        }
        //如果当前group只有灰度客户端,说明当前这个订阅关系(包括group)是新加的，那么则不走灰度逻辑(线上没有这层订阅关系)，不然会导致消息大量堆积
        if (MessageStorage.allCanaryRelease(cidAll)) {
           // List<MessageQueue> messageQueues = super.balanceAllocate(consumerGroup, currentCID, mqAll, cidAll);
            /*log.info("[canary allocate]: group:{} sub topic:{} has all canary release client,maybe the sub is new,use the default avg strategy.\n" +
                            "current cid:{}\n" +
                            "allocate total {} message queue\n" +
                            "result:\n{}",
                    consumerGroup,
                    mqAll.get(0).getTopic(),
                    messageQueues.size(),
                    currentCID,
                    MessageStorage.joinMessageQueue(messageQueues));*/
            return null;
        }
        //说明当前存在灰度客户端，则让灰度客户端瓜分灰度队列，其他客户端按照平均分配策略进行分配非灰度队列
        List<String> canaryCids = MessageStorage.getCanaryCids(cidAll);
        List<String> normalCids = MessageStorage.getNormalCids(cidAll);
        List<MessageQueue> canaryQueues = MessageStorage.getCanaryQueues(mqAll);
        List<MessageQueue> normalQueues = MessageStorage.getNormalQueues(mqAll);
        Collections.sort(canaryCids);
        Collections.sort(normalCids);
        Collections.sort(normalQueues);
        Collections.sort(canaryQueues);
        List<MessageQueue> result = null;
        if (canaryCids.contains(currentCID)) {
            result = allocateByAvg(consumerGroup, currentCID, canaryQueues, canaryCids);
        } else {
            result = allocateByAvg(consumerGroup, currentCID, normalQueues, normalCids);
        }
        /*if (log.isDebugEnabled()) {
            log.debug("topic:{} reBalance, has canary release client, allocate {} message queue by canary release strategy.\n" +
                            "current cid:{}\n" +
                            "result:\n{}",
                    mqAll.get(0).getTopic(),
                    result.size(),
                    currentCID,
                    result.stream()
                            .collect(Collectors.groupingBy(MessageQueue::getBrokerName))
                            .entrySet().stream()
                            .map(e -> e.getKey() + ": " + e.getValue().stream()
                                    .map(m -> String.valueOf(m.getQueueId()))
                                    .collect(Collectors.joining(", ")))
                            .collect(Collectors.joining("\n")));
        }*/
        return result;

    }


    /**
     * 按照平均策略分配消息队列给消费者。这个方法的目标是尽可能平均地将所有的消息队列分配给所有的消费者。
     * <p>
     * 如果消息队列的数量少于或等于消费者的数量，那么每个消费者将获得一个消息队列。如果消息队列的数量多于消费者的数量，那么每个消费者将获得消息队列总数除以消费者数的商个消息队列，余数会从前到后分配给消费者。
     * <p>
     * 例如，如果有10个消息队列和3个消费者，那么每个消费者将获得3个消息队列，剩下的1个消息队列会分配给第一个消费者。
     *
     * @param consumerGroup 消费者组名
     * @param currentCID    当前消费者的ID
     * @param mqAll         所有的消息队列
     * @param cidAll        所有消费者的ID列表
     * @return 分配给当前消费者的消息队列列表
     */
    private List<MessageQueue> allocateByAvg(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                             List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();
        int averageSize =
                mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                        + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "CANARY";
    }


    public boolean check(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                         List<String> cidAll) {
        if (StringUtils.isEmpty(currentCID)) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (CollectionUtils.isEmpty(mqAll)) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (CollectionUtils.isEmpty(cidAll)) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        if (!cidAll.contains(currentCID)) {
           /* log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                    consumerGroup,
                    currentCID,
                    cidAll);*/
            return false;
        }

        return true;
    }
}