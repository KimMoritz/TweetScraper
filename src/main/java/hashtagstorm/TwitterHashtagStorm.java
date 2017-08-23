package hashtagstorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import hashtagstorm.jms.BoltJmsProvider;

import java.util.ResourceBundle;

public class TwitterHashtagStorm {

    public static void main(String[] args) throws Exception{
        ResourceBundle resourceBundle = ResourceBundle.getBundle("application");

        String consumerKey = resourceBundle.getString("consumerKey");
        String consumerSecret = resourceBundle.getString("consumerSecret");
        String accessToken = resourceBundle.getString("accessToken");
        String accessTokenSecret = resourceBundle.getString("accessTokenSecret");
        String[] keyWords = {resourceBundle.getString("keyWords")};
        String brokerUrl = resourceBundle.getString("spring.activemq.broker-url");
        String queue = resourceBundle.getString("queue");

        Config config = new Config();
        config.setDebug(true);

        JmsBolt jmsBolt = new JmsBolt();
        JmsProvider jmsProvider = new BoltJmsProvider(brokerUrl,queue);

        jmsBolt.setJmsProvider(jmsProvider);
        jmsBolt.setJmsMessageProducer((session, input) -> {
            final String json = "{\"word\":\"" + input.getValue(0).toString() + "\", \"count\":"
                    + String.valueOf(input.getValue(0)) + "}";
            return session.createTextMessage(json);});

        //Topology configuration
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-spout", new TwitterSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keyWords));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        builder.setBolt("twitter-jms-bolt", jmsBolt)
        .shuffleGrouping("twitter-hashtag-counter-bolt");

        //LocalCluster cluster = new LocalCluster();
        LocalCluster cluster = new LocalCluster("localhost", 2181L);

        cluster.submitTopology("TwitterHashtagStorm", config, builder.createTopology());
        /*Thread.sleep(30*1000);
        cluster.shutdown();*/
    }

    static String[] getKeyWords(){
        String[] keyWords = {"trump"};
        //TODO: Fetch keywords from config file
        return keyWords;
    }
}