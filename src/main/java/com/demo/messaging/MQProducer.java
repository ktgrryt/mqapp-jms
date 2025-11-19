package com.demo.messaging;

import jakarta.annotation.Resource;
import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionAttribute;
import jakarta.ejb.TransactionAttributeType;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;

import jakarta.json.Json;
import jakarta.json.JsonObject;

@Stateless
public class MQProducer {

    // CMT 参加のコンテナ管理 JMSContext（ローカル送信用は従来どおり使用）
    @Inject
    @JMSConnectionFactory("jms/wmqCF")
    JMSContext context;

    // アプリ管理のコンテキストを作るために ConnectionFactory も取得
    @Resource(lookup = "jms/wmqCF")
    ConnectionFactory cf;

    @Resource(lookup = "jms/queue1")
    Queue queue;

    @Resource(lookup = "jms/remote1")
    Queue remoteQueue;

    // ---- ローカルキュー送信（従来どおり：CMT/トランザクション参加） ----
    public String sendLocalMessage(String message) throws Exception {
        try {
            TextMessage textMessage = context.createTextMessage();
            textMessage.setText(message);
            context.createProducer().send(queue, textMessage);

            JsonObject headers = buildHeaders(textMessage);
            return Json.createObjectBuilder()
                    .add("message", message)
                    .add("headers", headers)
                    .build()
                    .toString();
        } catch (Exception e) {
            throw new Exception("ローカルキューへの送信に失敗しました: " + e.getMessage(), e);
        }
    }

    // ---- リモートキュー送信（JTAに参加させない：即時PUT） ----
    // PoCでは NOT_SUPPORTED 推奨。JTAのcommit時エラーによるロールバックを回避。
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public String sendRemoteMessage(String message) throws Exception {
        // アプリ管理の JMSContext を使う（JTA非参加）。AUTO_ACKNOWLEDGE でOK。
        try (JMSContext appCtx = cf.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {

            // 診断ログ（必要に応じて削除可）
            System.out.println("[DEBUG] remoteQueue JNDI lookup OK");
            System.out.println("[DEBUG] remoteQueue.getQueueName()=" + getQueueNameQuietly(remoteQueue));

            TextMessage textMessage = appCtx.createTextMessage();
            textMessage.setText(message);

            appCtx.createProducer().send(remoteQueue, textMessage);

            System.out.println("[DEBUG] sent to remoteQueue OK, JMSMessageID=" + textMessage.getJMSMessageID());

            JsonObject headers = buildHeaders(textMessage);
            return Json.createObjectBuilder()
                    .add("message", message)
                    .add("headers", headers)
                    .build()
                    .toString();

        } catch (JMSRuntimeException e) {
            // ここに MQ の詳細（MQRC）がぶら下がることが多い
            System.err.println("[ERROR] JMSRuntimeException: " + e.getMessage());
            if (e.getLinkedException() != null) {
                System.err.println("[ERROR] Linked exception: " + e.getLinkedException());
            }
            throw new Exception("リモートキューへの送信に失敗しました: " + e.getMessage(), e);

        } catch (Exception e) {
            throw new Exception("リモートキューへの送信に失敗しました: " + e.getMessage(), e);
        }
    }

    // ---- 共通: JMSヘッダーをJSON化 ----
    private static JsonObject buildHeaders(TextMessage textMessage) throws JMSException {
        return Json.createObjectBuilder()
                .add("JMSMessageID", nvl(textMessage.getJMSMessageID()))
                .add("JMSTimestamp", textMessage.getJMSTimestamp())
                .add("JMSCorrelationID", nvl(textMessage.getJMSCorrelationID()))
                .add("JMSDestination", textMessage.getJMSDestination() != null ? textMessage.getJMSDestination().toString() : "")
                .add("JMSDeliveryMode", textMessage.getJMSDeliveryMode())
                .add("JMSExpiration", textMessage.getJMSExpiration())
                .add("JMSPriority", textMessage.getJMSPriority())
                .add("JMSReplyTo", textMessage.getJMSReplyTo() != null ? textMessage.getJMSReplyTo().toString() : "")
                .add("JMSType", nvl(textMessage.getJMSType()))
                .build();
    }

    private static String nvl(String s) {
        return (s == null) ? "" : s;
    }

    // Queue#getQueueName() は JMSException を投げうるので安全にラップ
    private static String getQueueNameQuietly(Queue q) {
        try {
            return q.getQueueName();
        } catch (JMSException e) {
            return "UNKNOWN(" + q + ")";
        }
    }
}
