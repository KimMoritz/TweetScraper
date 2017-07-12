package storm;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class CamelBolt extends BaseRichBolt {

    private boolean autoAck = true;
    private OutputCollector collector;
    CamelContext camelContext;
    ProducerTemplate producerTemplate;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
         camelContext = new DefaultCamelContext();
         camelContext.addComponent("activemq", ActiveMQComponent.activeMQComponent("vm://localhost?broker.persistent=false"));
        try {
            camelContext.addRoutes(new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    from("activemq:TwitterQueue")
                            .to("hashTagStormQueue2");
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        producerTemplate = camelContext.createProducerTemplate();
        try {
            camelContext.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {

        try {
            Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) input.getValue(0);
            String message =
                    "{\"key\":\"" + entry.getKey() + "\"," +
                            "\"value\":" + entry.getValue() + "}";

            producerTemplate.sendBody("activemq:TwitterQueue", message);

            if(this.autoAck){
                this.collector.ack(input);
            }
        } catch (Exception e) {
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

    @Override
    public void cleanup(){
        try {
            camelContext.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
