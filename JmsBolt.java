package storm;

import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.util.Map;

public class JmsBolt extends BaseRichBolt {

    private boolean autoAck = true;

    // javax.jms objects
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;
    // JMS options
    private boolean jmsTransactional = false;
    private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
    private JmsProvider jmsProvider;
    private JmsMessageProducer producer;
    private OutputCollector collector;

    public void setJmsProvider(JmsProvider provider){
        this.jmsProvider = provider;
    }

    public void setAutoAck(boolean autoAck){
        this.autoAck = autoAck;
    }

    public void setJmsMessageProducer(JmsMessageProducer producer){
        this.producer = producer;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {

        if(this.jmsProvider == null || this.producer == null){
            try {
                throw new IllegalStateException("JMS Provider and MessageProducer not set.");
            } catch (IllegalStateException e) {
                e.printStackTrace();
            }
        }
        this.collector = collector;
        try {
            ConnectionFactory cf = this.jmsProvider.connectionFactory();
            Destination dest = this.jmsProvider.destination();
            this.connection = cf.createConnection();
            this.session = connection.createSession(this.jmsTransactional,
                    this.jmsAcknowledgeMode);
            this.messageProducer = session.createProducer(dest);

            connection.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple input) {

        // write the tuple to a JMS destination...
        try {
            Message msg = this.producer.toMessage(this.session, (ITuple) input);
            if(msg != null){
                if (msg.getJMSDestination() != null) {
                    this.messageProducer.send(msg.getJMSDestination(), msg);
                } else {
                    this.messageProducer.send(msg);
                }
            }
            if(this.autoAck){
                this.collector.ack(input);
            }
        } catch (JMSException e) {
            // failed to send the JMS message, fail the tuple fast
            this.collector.fail(input);
            e.printStackTrace();
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup(){
        try {
            this.session.close();
            this.connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
