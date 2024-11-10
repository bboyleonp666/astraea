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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class YourPartitioner implements Partitioner {

  // not use atomic integer here to reduce implementation complexity
  private Map<Integer, Integer> brokerCounts;
  private Map<Integer, HashSet<Integer>> brokerPartitionMap;
  private Map<Integer, Integer> partitionCounts;

  // get your magic configs
  @Override
  public void configure(Map<String, ?> configs) {
    brokerCounts = new HashMap<>();
    brokerPartitionMap = new HashMap<>();
    partitionCounts = new HashMap<>();
  }

  private int getKeyWithMinCount(Map<Integer, Integer> map) {
    if (map.isEmpty()) { return -1; }
    Integer minKey = null;
    Integer minValue = Integer.MAX_VALUE;
    for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
      if (entry.getValue() < minValue) {
        minValue = entry.getValue();
        minKey = entry.getKey();
      }
    }
    return minKey != null ? minKey : -1; // Return -1 if no key was found
  }

  // write your magic code
  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    var partitions = cluster.availablePartitionsForTopic(topic);
    // no available partition so we return -1
    if (partitions.isEmpty()) return -1;

    // init maps
    for (var partition : partitions) {
      var brokerId = partition.leader().id();
      var partitionId = partition.partition();
      brokerCounts.putIfAbsent(brokerId, 0);
      partitionCounts.putIfAbsent(partitionId, 0);

      HashSet<Integer> instances = brokerPartitionMap.getOrDefault(brokerId, new HashSet<>());
      instances.add(partition.partition());
      brokerPartitionMap.put(brokerId, instances);
    }

    var minCountBrokerId = getKeyWithMinCount(brokerCounts);
    var candidatePartitions = brokerPartitionMap.get(minCountBrokerId);

    Integer partitionId = -1;
    Integer minCount = Integer.MAX_VALUE;
    for (var candidate : candidatePartitions) {
      var partitionCount = partitionCounts.get(candidate);
      if (partitionCount < minCount) {
        partitionId = candidate;
        minCount = partitionCount;
      }
    }
    brokerCounts.put(minCountBrokerId, brokerCounts.get(minCountBrokerId) + 1);
    partitionCounts.put(partitionId, partitionCounts.get(partitionId) + 1);
    return partitionId;
  }

  @Override
  public void close() {}
}
