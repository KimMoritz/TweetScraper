package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.jms.JmsProvider;

import javax.jms.*;

public class TwitterHashtagStorm {

    public static void main(String[] args) throws Exception{
        String consumerKey = "sKLbkYsWC8C2dydfQtWnaPJv2";
        String consumerSecret = "YyWaH1KWixsqV9XZGyIK2yzeEJYMDfNw0mzOiMik4aFHpcDgws";
        String accessToken = "874981062123900929-4l9YD0ApKesnvJKhFCkNJ2wxZOwqbVg";
        String accessTokenSecret = "LWwFocfRbUrjGf4eldm8Cw7fJRkzTKO8N5XZSvNQmxAuE";
        String[] keyWords = {"trump", "weekend"};

        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();

        JmsBolt jmsBolt = new JmsBolt();
        JmsProvider jmsProvider = new JmsProvider() {
            @Override
            public ConnectionFactory connectionFactory() throws Exception {
                return null;
            }

            @Override
            public Destination destination() throws Exception {
                return null;
            }
        };
        jmsBolt.setJmsProvider(jmsProvider);
        jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
            @Override
            public Message toMessage(Session session, ITuple input) throws JMSException {
                String json = "{\"word\":\"" + input.getString(0) + "\", \"count\":" + String.valueOf(input.getInteger(1)) + "}";
                return session.createTextMessage(json);
            }
        });


        builder.setSpout("twitter-spout", new TwitterSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keyWords));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        builder.setBolt("twitter-jms-counter-bolt", new JmsBolt())
                .fieldsGrouping("twitter-hashtag-counter-bolt", new Fields("hashtag"));

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("TwitterHashtagStorm", config, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }

    static String[] getKeyWords(){
        String[] keyWords = {"asdf"};
        //TODO: Fetch keywords stored in database, via data layer, routed by Camel
        return keyWords;
    }
}