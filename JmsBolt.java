package storm;

import org.apache.camel.CamelContext;
import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.util.Map;

public class JmsBolt extends BaseRichBolt {

    private boolean autoAck = true;
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;
    private boolean jmsTransactional = false;
    private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
    private JmsProvider jmsProvider;
    private JmsMessageProducer producer;
    private CamelContext producerTemplate;
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
        if(this.jmsProvider == null){
            try {
                throw new IllegalStateException("JMS Provider not set.");
            } catch (IllegalStateException e) {
                e.printStackTrace();
            }
        }
        if (this.producer == null){
            try {
                throw new IllegalAccessException("prouducer not set.");
            }catch (IllegalAccessException e){
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
        try {
            Message msg = this.producer.toMessage(this.session, input);
            if(msg != null){
                if (msg.getJMSDestination() != null) {
                    this.messageProducer.send(msg.getJMSDestination(), msg);
                } else {
                    this.messageProducer.send(msg);
                    System.out.println(input);
                }
                System.out.println("Message: " + msg.toString() +" sent to: " + msg.getJMSDestination());
            }
            if(this.autoAck){
                this.collector.ack(input);
            }
        } catch (JMSException e) {
            this.collector.fail(input);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

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
