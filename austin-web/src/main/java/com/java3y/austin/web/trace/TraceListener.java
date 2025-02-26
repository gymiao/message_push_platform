package com.java3y.austin.web.trace;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.text.StrPool;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Throwables;
import com.java3y.austin.common.constant.AustinConstant;
import com.java3y.austin.common.domain.AnchorInfo;
import com.java3y.austin.common.domain.SimpleAnchorInfo;
import com.java3y.austin.handler.receiver.MessageReceiver;
import com.java3y.austin.support.constans.MessageQueuePipeline;
import com.java3y.austin.support.utils.RedisUtils;
import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * @author 3y
 * 消费MQ的消息austin.business.log.topic.name
 */
@Slf4j
@Service
public class TraceListener{


    @Autowired
    private RedisUtils redisUtils;

    /**
     * 追踪消息
     *
     * @param consumerRecord
     */
    @KafkaListener(topics = "#{'${austin.business.log.topic.name}'}", groupId = "#{'${austin.business.trace.group.name}'}")
    public void trace(ConsumerRecord<?, String> consumerRecord) {
        Optional<String> kafkaMessage = Optional.ofNullable(consumerRecord.value());
        if (kafkaMessage.isPresent()) {
            // Object anchor = JSON.parse(kafkaMessage.get(), AnchorInfo.class);
            AnchorInfo info = JSON.parseObject(kafkaMessage.get(), AnchorInfo.class);
            // List<AnchorInfo> anchorInfoLists = JSON.parseArray(kafkaMessage.get(), AnchorInfo.class);

            realTimeData(info);


            System.out.println("kafaka_日志");
            System.out.println(info.getIds());
            System.out.println(info.toString());
            System.out.println("kafaka处理完毕");
        }
    }

    private void realTimeData(AnchorInfo info) {

        /**
         * 0.构建messageId维度的链路信息 数据结构list:{key,list}
         * key:Austin:MessageId:{messageId},listValue:[{timestamp,state,businessId},{timestamp,state,businessId}]
         */
        String redisMessageKey = CharSequenceUtil.join(StrPool.COLON, AustinConstant.CACHE_KEY_PREFIX, AustinConstant.MESSAGE_ID, info.getMessageId());
        SimpleAnchorInfo messageAnchorInfo = SimpleAnchorInfo.builder().businessId(info.getBusinessId()).state(info.getState()).timestamp(info.getLogTimestamp()).build();

        redisUtils.lPush(redisMessageKey, JSON.toJSONString(messageAnchorInfo), Duration.ofDays(3).toMillis() / 1000);


        /**
         * 1.构建userId维度的链路信息 数据结构list:{key,list}
         * key:userId,listValue:[{timestamp,state,businessId},{timestamp,state,businessId}]
         */
        SimpleAnchorInfo userAnchorInfo = SimpleAnchorInfo.builder().businessId(info.getBusinessId()).state(info.getState()).timestamp(info.getLogTimestamp()).build();
        for (String id : info.getIds()) {
            redisUtils.lPush(id, JSON.toJSONString(userAnchorInfo), (DateUtil.endOfDay(new Date()).getTime() - DateUtil.current()) / 1000);
        }

        /**
         * 2.构建消息模板维度的链路信息 数据结构hash:{key,hash}
         * key:businessId,hashValue:{state,stateCount}
         */
        redisUtils.hincrby(String.valueOf(info.getBusinessId()), String.valueOf(info.getState()), info.getIds().size(),
                ((DateUtil.offsetDay(new Date(), 30).getTime()) / 1000) - DateUtil.currentSeconds());

    }

}
