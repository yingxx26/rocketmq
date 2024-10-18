package org.apache.rocketmq.client;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MessageStorage {

    //List<MessageQueue> canaryQueues = MessageStorage.getCanaryQueues(topicPublishInfo.getMessageQueueList());
    //List<MessageQueue> normalQueues = MessageStorage.getNormalQueues(topicPublishInfo.getMessageQueueList());

    //灰度开关
    public static boolean isCanaryRelease() {
        return true;
    }

    public static boolean allCanaryRelease(List<String> cidAll) {
        return false;
    }

    public static boolean hasCanaryRelease(List<String> cidAll) {
        return true;
    }


    public static List<MessageQueue>   getCanaryQueues(List<MessageQueue> mqAll){
        int size = mqAll.size();
        MessageQueue messageQueue = mqAll.get(size - 1);
        List<MessageQueue> res=new ArrayList<MessageQueue>();
        res.add(messageQueue);
        return res;
    }
    public static List<MessageQueue> getNormalQueues(List<MessageQueue> mqAll) {
        int size = mqAll.size();
        MessageQueue messageQueue = mqAll.get(size - 1);
        mqAll.remove(messageQueue);
        return mqAll;
    }

    public static List<String> getCanaryCids(List<String> cidAll) {
        List<String> canaryCids = cidAll.stream().filter(
                e -> e.endsWith("@canary")).collect(Collectors.toList());
        return canaryCids;
    }

    public static List<String> getNormalCids(List<String> cidAll) {
        List<String> normalCids = cidAll.stream().filter(
                e -> e.endsWith("@default")).collect(Collectors.toList());
        return normalCids;
    }


}
