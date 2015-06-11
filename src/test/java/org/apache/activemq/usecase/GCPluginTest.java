/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.plugin.GCInactiveDestinationsPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.management.ObjectName;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;

public class GCPluginTest {

    BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("amq");
        GCInactiveDestinationsPlugin gc = new GCInactiveDestinationsPlugin();
        gc.setCheckPeriod(1000);
        broker.setPlugins(new BrokerPlugin[]{gc});
        PolicyEntry entry = new PolicyEntry();
        entry.setGcInactiveDestinations(true);
        entry.setGcWithNetworkConsumers(true);
        entry.setInactiveTimeoutBeforeGC(3000);
        PolicyMap map = new PolicyMap();
        map.setDefaultEntry(entry);
        broker.setDestinationPolicy(map);
        broker.setSchedulePeriodForDestinationPurge(-1);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void shutdown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test
    public void testNetworkSubscriber() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://amq");
        Connection connection = factory.createConnection("admin", "admin");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer hashConsumer = session.createConsumer(session.createTopic(">?consumer.networkSubscription=true"));

        MessageConsumer regularConsumer = session.createConsumer(session.createTopic("TEST"));
        Thread.sleep(2000);
        regularConsumer.close();

        Thread.sleep(10000);

        assertFalse(Arrays.asList(broker.getAdminView().getTopics()).contains(new ObjectName("org.apache.activemq:type=Broker,brokerName=amq,destinationType=Topic,destinationName=TEST")));
    }
}
