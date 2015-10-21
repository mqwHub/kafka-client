package zx.soft.kafka.consumer;

import java.util.List;

/**
 * 消息处理类
 * @author donglei
 *
 */
public interface MessageHandler {

	public void handleMessage(byte[] message);

	public void handleMessage(List<byte[]> messages);


}
