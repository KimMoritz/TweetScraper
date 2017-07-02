package storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class HashtagCounterBolt implements IRichBolt {
    private Map<String, Integer> counterMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getString(0);

        if(!counterMap.containsKey(key)){
            counterMap.put(key, 1);
        }else{
            Integer c = counterMap.get(key) + 1;
            counterMap.put(key, c);
        }
        collector.ack(tuple);

        //Post to data service layer.
        for (Map.Entry<String, Integer> entry:counterMap.entrySet()){
            collector.emit(new Values(entry));
        }

    }

    @Override
    public void cleanup() {
        /*for(Map.Entry<String, Integer> entry:counterMap.entrySet()){
            System.out.println("Result: " + entry.getKey()+" : " + entry.getValue());
        }*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}