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
package org.apache.activemq.plugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.util.BrokerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GCInactiveDestinationsBroker extends BrokerFilter {
    public static final Logger LOG = LoggerFactory.getLogger(GCInactiveDestinationsBroker.class);
    private Runnable gcTask;
    private long checkPeriod;

    private Map<Destination, Long> lastActiveTime = new HashMap<Destination, Long>();
    private final ReentrantReadWriteLock inactiveDestinationsPurgeLock = new ReentrantReadWriteLock();

    public GCInactiveDestinationsBroker(Broker next) {
        super(next);
    }

    @Override
    public void start() throws Exception {
        super.start();
        try {
            BrokerContext brokerContext = next.getBrokerService().getBrokerContext();
            gcTask = new Runnable() {
                @Override
                public void run() {
                    inactiveDestinationsPurgeLock.writeLock().lock();
                    try {
                        List<Destination> list = new ArrayList<Destination>();
                        Map<ActiveMQDestination, Destination> map = next.getDestinationMap();
                        long timeStamp = System.currentTimeMillis();
                        for (Destination d : map.values()) {
                            markForGC((BaseDestination) d, timeStamp);
                            if (canGC((BaseDestination) d)) {
                                list.add(d);
                            }
                        }
                        if (!list.isEmpty()) {
                            ConnectionContext context = BrokerSupport.getConnectionContext(next.getBrokerService().getRegionBroker());
                            context.setBroker(next.getBrokerService().getRegionBroker());

                            for (Destination dest : list) {
                                LOG.info("{} Inactive for longer than {} ms - removing ...", dest.getName(), dest.getInactiveTimoutBeforeGC());
                                try {
                                    getRoot().removeDestination(context, dest.getActiveMQDestination(), 1);
                                    lastActiveTime.remove(dest);
                                } catch (Exception e) {
                                    LOG.error("Failed to remove inactive destination {}", dest, e);
                                }
                            }
                        }

                    } finally {
                        inactiveDestinationsPurgeLock.writeLock().unlock();
                    }
                }
            };

            this.getBrokerService().getScheduler().executePeriodically(gcTask, checkPeriod);

        } catch (Exception error) {
            LOG.error("failed to start", error);
        }
    }

    public void markForGC(BaseDestination destination, long timeStamp) {
        Long lastActive = lastActiveTime.get(destination);
        if (destination.isGcIfInactive() && lastActive != null && lastActive == 0 && isActive(destination) == false
                && destination.getDestinationStatistics().getMessages().getCount() == 0 && destination.getInactiveTimoutBeforeGC() > 0l) {
            lastActiveTime.put(destination, timeStamp);
        }
    }

    public boolean canGC(BaseDestination destination) {
        boolean result = false;
        Long lastActive = lastActiveTime.get(destination);
        if (destination.isGcIfInactive()&& lastActive != null && lastActive != 0l) {
            if ((System.currentTimeMillis() - lastActiveTime.get(destination)) >= destination.getInactiveTimoutBeforeGC()) {
                result = true;
            }
        }
        return result;
    }

    protected boolean isActive(BaseDestination destination) {
        int consumers = 0;
        // find non-hash consumers
        for (Subscription sub : destination.getConsumers()) {
            if (!sub.getConsumerInfo().getDestination().getPhysicalName().equals(">")) {
                consumers++;
            }
        }

        // not active if we have non-hash consumers and producers
        boolean isActive = consumers != 0 || destination.getDestinationStatistics().getProducers().getCount() != 0;
        // ADD MORE LOGIC HERE IF NEEDED
        return isActive;
    }

    @Override
    public void stop() throws Exception {
        if (gcTask != null) {
            try {
                this.getBrokerService().getScheduler().cancel(gcTask);
            } catch (Exception letsNotStopStop) {
                LOG.warn("Failed to cancel gc task", letsNotStopStop);
            }
        }
        super.stop();
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        Subscription sub = super.addConsumer(context, info);
        updateLastActiveTime(next.getDestinations(info.getDestination()));
        return sub;
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        super.addProducer(context, info);
        updateLastActiveTime(next.getDestinations(info.getDestination()));
    }

    protected void updateLastActiveTime(Collection<Destination> destinations) {
        for (Iterator<Destination> it = destinations.iterator(); it.hasNext();) {
            Destination dest = it.next();
            lastActiveTime.put(dest, 0L);
        }
    }

    public long getCheckPeriod() {
        return checkPeriod;
    }

    public void setCheckPeriod(long checkPeriod) {
        this.checkPeriod = checkPeriod;
    }
}
