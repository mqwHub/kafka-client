package zx.soft.kafka.consumer;


/**
 * 消息处理类
 * @author donglei
 *
 */
public interface MessageHandler {

	public void handleMessage(byte[] message);

}
