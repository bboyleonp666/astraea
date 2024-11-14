/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.partitioner;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class YourPartitioner implements Partitioner {

  private Map<Integer, AtomicInteger> brokerCounts;
  private Map<Integer, Set<Integer>> brokerPartitionMap;
  private Map<Integer, AtomicInteger> partitionCounts;

  @Override
  public void configure(Map<String, ?> configs) {
    brokerCounts = new ConcurrentHashMap<>();
    brokerPartitionMap = new ConcurrentHashMap<>();
    partitionCounts = new ConcurrentHashMap<>();
  }

  private int getKeyWithMinCount(Map<Integer, AtomicInteger> map) {
    return map.entrySet()
            .stream()
            .min(Comparator.comparingInt(e -> e.getValue().get()))
            .map(Map.Entry::getKey)
            .orElse(-1);
  }

  @Override
  public int partition(
          String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

//    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    if (partitions.isEmpty()) {
      return -1;
    }

    // Initialize maps if they are empty
    if (brokerCounts.isEmpty() && brokerPartitionMap.isEmpty() && partitionCounts.isEmpty()) {
      for (PartitionInfo partition : partitions) {
        int brokerId = partition.leader().id();
        int partitionId = partition.partition();

        brokerCounts.putIfAbsent(brokerId, new AtomicInteger(0));
        partitionCounts.putIfAbsent(partitionId, new AtomicInteger(0));

        brokerPartitionMap.computeIfAbsent(brokerId, k -> new HashSet<>()).add(partitionId);
      }
    }

    int minCountBrokerId = getKeyWithMinCount(brokerCounts);
    Set<Integer> candidatePartitions = brokerPartitionMap.get(minCountBrokerId);

    if (candidatePartitions == null || candidatePartitions.isEmpty()) {
      return -1;
    }

    int partitionId = candidatePartitions.stream()
            .min(Comparator.comparingInt(p -> partitionCounts.get(p).get()))
            .orElse(-1);

    if (partitionId != -1) {
      brokerCounts.get(minCountBrokerId).incrementAndGet();
      partitionCounts.get(partitionId).incrementAndGet();
    }

    return partitionId;
  }

  @Override
  public void close() {}
}
