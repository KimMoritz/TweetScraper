package hashtagstorm.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.storm.jms.JmsProvider;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;

public class BoltJmsProvider implements JmsProvider {
    private String brokerUrl;
    private String queueName;

    public BoltJmsProvider(String brokerUrl, String queueName){
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
    }

    @Override
    public ConnectionFactory connectionFactory() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(this.brokerUrl);
        return connectionFactory;
    }

    @Override
    public Destination destination() throws Exception {
        Destination destination = connectionFactory()
                .createConnection()
                .createSession(false, Session.AUTO_ACKNOWLEDGE)
                .createQueue(queueName);
        return destination;
    }
}
