/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.io.kafka.distributed.source;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class implements a Kafka source to receives events from a kafka cluster.
 */
@Extension(
        name = "kafkaDistributed",
        namespace = "source",
        description = "A Kafka source receives events to be processed by WSO2 SP from a topic with a partition " +
                "for a Kafka cluster. The events received must be in the `JSON` format.\n" +
                "If the topic is not already created in the Kafka cluster, the Kafka sink creates the default " +
                "partition for the given topic."
                + "Here an Attribute in Double must be passed as the first attribute if 'order.enabled' is true",
        parameters = {
                @Parameter(name = "bootstrap.servers",
                           description = "This specifies the list of Kafka servers to which the Kafka source " +
                                   "must listen. This list should be provided as a set of comma-separated values.\n" +
                                   "e.g., `localhost:9092,localhost:9093`",
                           type = {DataType.STRING}),
                @Parameter(name = "topic.list",
                           description = "This specifies the list of topics to which the source must listen. This " +
                                   "list should be provided as a set of comma-separated values.\n" +
                                   "e.g., `topic_one,topic_two`",
                           type = {DataType.STRING}),
                @Parameter(name = "group.id",
                           description = "This is an ID to identify the Kafka source group. The group ID ensures " +
                                   "that sources with the same topic and partition that are in the same group do not" +
                                   " receive the same event.",
                           type = {DataType.STRING}),
                @Parameter(name = "partition.no.list",
                           description = "The partition number list for the given topic. This is provided as a list" +
                                   " of comma-separated values. e.g., `0,1,2,`.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "0"),
                @Parameter(name = "seq.enabled",
                           description = "If this parameter is set to `true`, the sequence of the events received via" +
                                   " the source is taken into account. Therefore, each event should contain a " +
                                   "sequence number as an attribute value to indicate the sequence.",
                           type = {DataType.BOOL},
                           optional = true,
                           defaultValue = "false"),
                @Parameter(name = "order.enabled",
                           description = "If this parameter is set to `true`, the sequence of the events received via" +
                                   " the source is taken into account. Therefore, each event will be taken and "
                                   + "compared with other partitioned event and merged in order and sent as one "
                                   + "stream to siddhi",
                           type = {DataType.BOOL},
                           optional = true,
                           defaultValue = "false"),
                @Parameter(name = "optional.configuration",
                           description = "This parameter contains all the other possible configurations that the " +
                                   "consumer is created with. \n" +
                                   "e.g., `ssl.keystore.type:JKS,batch.size:200`.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "null")
        },
        examples = {
                @Example(
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream BarStream (serialNo double, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@source(\n" +
                                "type='kafka', \n" +
                                "topic.list='kafka_topic,kafka_topic2', \n" +
                                "group.id='test', \n" +
                                "order.enabled='true', \n" +
                                "bootstrap.servers='localhost:9092', \n" +
                                "partition.no.list='0', \n" +
                                "@map(type='json'))\n" +
                                "Define stream FooStream (serialNo double, price float, volume long);\n" +
                                "from FooStream select serialNo, price, volume insert into BarStream;\n",
                        description = "This kafka source configuration listens to the `kafka_topic` and " +
                                "`kafka_topic2` topics with `0` partition. A thread is created for all the topics "
                                + "given. The events are received in the JSON format, "
                                + "and ordered and then mapped" +
                                " to a Siddhi event, and sent to a stream named `FooStream`. "),
                @Example(
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream BarStream (serialNo double, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@source(\n" +
                                "type='kafka', \n" +
                                "topic.list='kafka_topic',\n" +
                                "group.id='test', \n" +
                                "order.enabled='true',\n" +
                                "partition.no.list='0,1,2', \n" +
                                "bootstrap.servers='localhost:9092',\n" +
                                "@map(type='json'))\n" +
                                "Define stream FooStream (serialNo double, price float, volume long);\n" +
                                "from FooStream select serialNo, price, volume insert into BarStream;\n",
                        description = "This Kafka source configuration listens to the `kafka_topic` topic for the " +
                                " partitions 0,1,2 because no `partition.no.list` is defined. One thread is " +
                                "created for the topic. The events are received in the JSON format, and "
                                + "compared using the serialNo for the order and then ordered and mapped" +
                                " to a Siddhi event, and sent to a stream named `FooStream`.")
        }
)
public class KafkaSource extends Source {

    public static final String SINGLE_THREADED = "single.thread";
    public static final String TOPIC_WISE = "topic.wise";
    public static final String PARTITION_WISE = "partition.wise";
    public static final String ADAPTOR_SUBSCRIBER_TOPIC = "topic.list";
    public static final String ADAPTOR_SUBSCRIBER_GROUP_ID = "group.id";
    public static final String ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS = "bootstrap.servers";
    public static final String ADAPTOR_SUBSCRIBER_PARTITION_NO_LIST = "partition.no.list";
    public static final String ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES = "optional.configuration";
    public static final String TOPIC_OFFSET_MAP = "topic.offset.map";
    public static final String THREADING_OPTION = "threading.option";
    public static final String SEQ_ENABLED = "seq.enabled";
    public static final String SERIAL_NO = "serial.no";
    public static final String ORDER_ENABLED = "order.enabled";
    public static final String HEADER_SEPARATOR = ",";
    public static final String ENTRY_SEPARATOR = ":";
    public static final String LAST_RECEIVED_SEQ_NO_KEY = "lastReceivedSeqNo";
    private static final Logger LOG = Logger.getLogger(KafkaSource.class);
    private SourceEventListener sourceEventListener;
    private ScheduledExecutorService executorService;
    private OptionHolder optionHolder;
    private ConsumerKafkaGroup consumerKafkaGroup;
    private Map<String, Map<Integer, Long>> topicOffsetMap = new HashMap<>();
    private String bootstrapServers;
    private String groupID;
    private String threadingOption;
    private String partitions[];
    private String topics[];
    private String optionalConfigs;
    private Boolean seqEnabled = false;
    private Boolean orderEnabled = false;
    private Map<String, Map<SequenceKey, Integer>> consumerLastReceivedSeqNoMap = null;

    private static Properties createConsumerConfig(String zkServerList, String groupId, String optionalConfigs) {
        Properties props = new Properties();
        props.put(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS, zkServerList);
        props.put(ADAPTOR_SUBSCRIBER_GROUP_ID, groupId);

        //If it stops heart-beating for a period of time longer than session.timeout.ms then it will be considered dead
        // and its partitions will be assigned to another process
        props.put("session.timeout.ms", "30000");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        if (optionalConfigs != null && optionalConfigs.isEmpty()) {
            String[] optionalProperties = optionalConfigs.split(HEADER_SEPARATOR);
            if (optionalProperties.length > 0) {
                for (String header : optionalProperties) {
                    try {
                        String[] configPropertyWithValue = header.split(ENTRY_SEPARATOR, 2);
                        props.put(configPropertyWithValue[0], configPropertyWithValue[1]);
                    } catch (Exception e) {
                        LOG.warn("Optional property '" + header + "' is not defined in the correct format.", e);
                    }
                }
            }
        }
        return props;
    }

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, String[] strings,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.optionHolder = optionHolder;
        this.executorService = siddhiAppContext.getScheduledExecutorService();
        bootstrapServers = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS);
        groupID = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_GROUP_ID);
        threadingOption = optionHolder.validateAndGetStaticValue(THREADING_OPTION);
        String partitionList = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_PARTITION_NO_LIST, null);
        partitions = (partitionList != null) ? partitionList.split(HEADER_SEPARATOR) : null;
        String topicList = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_TOPIC);
        topics = topicList.split(HEADER_SEPARATOR);
        seqEnabled = optionHolder.validateAndGetStaticValue(SEQ_ENABLED, "false").equalsIgnoreCase("true");
        orderEnabled = optionHolder.validateAndGetStaticValue(ORDER_ENABLED, "false").equalsIgnoreCase("true");
        optionalConfigs = optionHolder.validateAndGetStaticValue(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES, null);
        if (PARTITION_WISE.equals(threadingOption) && null == partitions) {
            throw new SiddhiAppValidationException("Threading option is selected as 'partition.wise' but there are no"
                                                           + " partitions given");
        }
        if ((PARTITION_WISE.equals(threadingOption) || TOPIC_WISE.equals(threadingOption)) && orderEnabled) {
            throw new SiddhiAppValidationException("Threading option is selected as " + threadingOption + "and 'order"
                                                           + ".enabled' also set true. Conflict occurs");
        }
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        checkTopicsAvailableInCluster();
        checkPartitionsAvailableForTheTopicsInCluster();

        if (seqEnabled && consumerLastReceivedSeqNoMap == null) {
            consumerLastReceivedSeqNoMap = new HashMap<>();
        }

        consumerKafkaGroup = new ConsumerKafkaGroup(topics, partitions,
                                                    KafkaSource.createConsumerConfig(bootstrapServers, groupID,
                                                                                     optionalConfigs), topicOffsetMap,
                                                    consumerLastReceivedSeqNoMap, threadingOption, executorService,
                                                    orderEnabled);
        consumerKafkaGroup.run(sourceEventListener);
    }

    @Override
    public void disconnect() {
        if (consumerKafkaGroup != null) {
            consumerKafkaGroup.shutdown();
            LOG.info("Kafka Adapter disconnected for topic/s" +
                             optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_TOPIC));
        }
    }

    @Override
    public void destroy() {
        consumerKafkaGroup = null;
    }

    @Override
    public void pause() {
        if (consumerKafkaGroup != null) {
            consumerKafkaGroup.pause();
            LOG.info("Kafka Adapter paused for topic/s" + optionHolder.validateAndGetStaticValue
                    (ADAPTOR_SUBSCRIBER_TOPIC));
        }
    }

    @Override
    public void resume() {
        if (consumerKafkaGroup != null) {
            consumerKafkaGroup.resume();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka Adapter resumed for topic/s" + optionHolder.validateAndGetStaticValue
                        (ADAPTOR_SUBSCRIBER_TOPIC));
            }
        }
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        currentState.put(TOPIC_OFFSET_MAP, this.topicOffsetMap);
        if (seqEnabled) {
            currentState.put(LAST_RECEIVED_SEQ_NO_KEY, consumerKafkaGroup.getPerConsumerLastReceivedSeqNo());
        }
        return currentState;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        this.topicOffsetMap = (Map<String, Map<Integer, Long>>) state.get(TOPIC_OFFSET_MAP);
        if (seqEnabled) {
            this.consumerLastReceivedSeqNoMap =
                    (Map<String, Map<SequenceKey, Integer>>) state.get(LAST_RECEIVED_SEQ_NO_KEY);
        }
        consumerKafkaGroup.restore(topicOffsetMap);
    }

    private void checkTopicsAvailableInCluster() throws ConnectionUnavailableException {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "test-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        try {
            Map<String, List<PartitionInfo>> testTopicList = consumer.listTopics();
            boolean topicsAvailable = true;
            StringBuilder invalidTopics = new StringBuilder("");
            for (String topic : topics) {
                boolean topicAvailable = false;
                for (Map.Entry<String, List<PartitionInfo>> entry : testTopicList.entrySet()) {
                    if (entry.getKey().equals(topic)) {
                        topicAvailable = true;
                    }
                }
                if (!topicAvailable) {
                    topicsAvailable = false;
                    if ("".equals(invalidTopics.toString())) {
                        invalidTopics.append(topic);
                    } else {
                        invalidTopics.append(',').append(topic);
                    }
                    LOG.warn("Topic, " + topic + " is not available.");
                }
            }
            if (null != partitions && !(partitions.length == 1 && partitions[0].equals("0")) && !topicsAvailable) {
                String errorMessage = "Topic/s " + invalidTopics + " aren't available. Topics wont created since there "
                        + "are partition numbers defined in the query.";
                LOG.error(errorMessage);
                throw new ConnectionUnavailableException("Topic/s " + invalidTopics + " aren't available. "
                                                                 + "Topics wont created since there "
                                                                 + "are partition numbers defined in the query.");
            } else if (!topicsAvailable) {
                LOG.warn("Topic/s " + invalidTopics + " aren't available. "
                                 + "These Topics will be created with the default partition.");
            }
        } catch (NullPointerException ex) {
            //calling super class logs the exception and retry
            throw new ConnectionUnavailableException("Exception when connecting to kafka servers: "
                                                             + sourceEventListener.getStreamDefinition().getId(), ex);
        }
    }

    private void checkPartitionsAvailableForTheTopicsInCluster() throws ConnectionUnavailableException {
        //checking whether the defined partitions are available in the defined topic
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                             "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                             "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
        boolean partitionsAvailable = true;
        StringBuilder invalidPartitions = new StringBuilder("");
        for (String topic : topics) {
            List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
            if (null != partitions && !(partitions.length == 1 && partitions[0].equals("0"))) {
                for (String partition : partitions) {
                    boolean partitonAvailable = false;
                    for (PartitionInfo partitionInfo : partitionInfos) {
                        if (Integer.parseInt(partition) == partitionInfo.partition()) {
                            partitonAvailable = true;
                        }
                    }
                    if (!partitonAvailable) {
                        partitionsAvailable = false;
                        if ("".equals(invalidPartitions.toString())) {
                            invalidPartitions.append(partition);
                        } else {
                            invalidPartitions.append(',').append(partition);
                        }
                        LOG.error("Partition number, " + partition
                                          + " in 'partition.id' is not available in topic partitions");
                    }
                }
                if (!partitionsAvailable) {
                    throw new ConnectionUnavailableException(
                            "Partition number/s " + invalidPartitions + " aren't available for "
                                    + "the topic: " + topic);
                }
            }
        }
    }
}
