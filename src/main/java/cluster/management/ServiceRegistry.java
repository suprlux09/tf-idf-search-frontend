/*
 *  MIT License
 *
 *  Copyright (c) 2019 Michael Pogrebinsky - Distributed Systems & Cloud Computing with Java
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ServiceRegistry implements Watcher {
    private final ZooKeeper zooKeeper;
    private List<String> allServiceAddresses = null;
    public static final String COORDINATORS_REGISTRY_ZNODE = "/coordinators_service_registry";
    private final Random random;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        this.random = new Random();
    }

    public synchronized String getRandomServiceAddress() throws KeeperException, InterruptedException {
        if (allServiceAddresses == null) {
            updateAddresses();
        }
        if (!allServiceAddresses.isEmpty()) {
            int randomIndex = random.nextInt(allServiceAddresses.size());
            return allServiceAddresses.get(randomIndex);
        } else {
            return null;
        }
    }

    private synchronized void updateAddresses() throws KeeperException, InterruptedException {
        List<String> clusters = zooKeeper.getChildren("/", this);
        List<String> addresses = new ArrayList<>(clusters.size()-1);

        for (String cluster : clusters) {
            if (!"zookeeper".equals(cluster)) {
                List<String> coordinators = zooKeeper.getChildren(cluster + COORDINATORS_REGISTRY_ZNODE, this);

                for (String coordinator : coordinators) {
                    String serviceFullpath = cluster + COORDINATORS_REGISTRY_ZNODE + "/" + coordinator;
                    Stat stat = zooKeeper.exists(serviceFullpath, false);
                    if (stat == null) {
                        continue;
                    }

                    byte[] addressBytes = zooKeeper.getData(serviceFullpath, false, stat);
                    String address = new String(addressBytes);
                    addresses.add(address);

                    System.out.println("Cluster " + cluster + " has a coordinator " + address);
                }
            }
        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The coordinators addresses are: " + this.allServiceAddresses);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddresses();
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
    }
}
