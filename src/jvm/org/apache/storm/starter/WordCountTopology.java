/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
    private static final Logger logger = LoggerFactory.getLogger(WordCountTopology.class);


    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            System.out.println("================declareOutputFields");

            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        @Override
        public void execute(Tuple input) {
            System.out.println("execute: " + input.getSourceComponent());
        }
    }

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        PrintWriter pw;


        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
          //  Integer count = counts.get(word);
        //    if (count == null)
        //        count = 0;
        //    count++;
          //  logger.info("COUNT:" + count);

        //    counts.put(word, count);
            //collector.emit(new Values(word, count));

            pw.println(String.format("%s", word));
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            super.prepare(stormConf, context);

            try {
                pw = new PrintWriter(new FileOutputStream("/Users/sergeykim/tmp/res.txt"), true);

            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void cleanup() {
            super.cleanup();

            pw.close();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout_spout", new RandomSentenceSpout(), 5);

        // builder.setBolt("split_bolt", new SplitSentence(), 8).shuffleGrouping("spout_spout");
        // builder.setBolt("count_bolt", new WordCount(), 12).fieldsGrouping("split_bolt", new Fields("word"));

        builder.setBolt("count_bolt", new WordCount(), 12).fieldsGrouping("spout_spout", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism();

        logger.info("create topology: starterwc");

        // if (args != null && args.length > 0) {
        conf.setNumWorkers(3);

        StormSubmitter.submitTopologyWithProgressBar("starterwc", conf, builder.createTopology());
//    }
//    else {
//      conf.setMaxTaskParallelism(3);
//
//      LocalCluster cluster = new LocalCluster();
//      cluster.submitTopology("word-count", conf, builder.createTopology());
//
//      Thread.sleep(10000);
//
//      cluster.shutdown();
//    }
    }
}
