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
package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

public class RandomSentenceSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomSentenceSpout.class);

    SpoutOutputCollector _collector;
    Random _rand;


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        LOG.info("open");
    }

    @Override
    public void nextTuple() {
        LOG.info("nextTuple");
        Utils.sleep(100);
        for (int i = 0; i < 1000; i++) {
            _collector.emit(new Values("[" + Integer.toString(i) + "]"));
        }
        String[] sentences = new String[]{
                sentence("the cow jumped over the moon"),
                sentence("an apple a day keeps the doctor away"),
                sentence("four score and seven years ago"),
                sentence("snow white and the seven dwarfs"),
                sentence("i am at two with nature")};
        final String sentence = sentences[_rand.nextInt(sentences.length)];

        //LOG.info("Emitting tuple: {}", sentence);

        _collector.emit(new Values(sentence));
    }

    protected String sentence(String input) {
        return input;
    }

    @Override
    public void ack(Object id) {
        LOG.info("acknowledge");
    }

    @Override
    public void fail(Object id) {
        LOG.info("fail");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        LOG.info("RandomSentenceSpout.declareOutputFields");
        declarer.declare(new Fields("word"));
    }

    // Add unique identifier to each tuple, which is helpful for debugging
    public static class TimeStamped extends RandomSentenceSpout {
        private final String prefix;

        public TimeStamped() {
            this("");
        }

        public TimeStamped(String prefix) {
            this.prefix = prefix;
        }

        protected String sentence(String input) {
            return prefix + currentDate() + " " + input;
        }

        private String currentDate() {
            return new SimpleDateFormat("yyyy.MM.dd_HH:mm:ss.SSSSSSSSS").format(new Date());
        }
    }
}
