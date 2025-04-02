// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.database.idassigner.IDPoolExhaustedException;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A partition strategy that assigns vertices to the partition where most of their neighbors reside over time,
 * with a balancing mechanism to prevent excessive imbalance across partitions.
 * 
 * Tracks vertex counts per partition and ensures no partition exceeds 20% more vertices
 * than the least populated partition.
 *
 * @author Spitalas Alexandros
 */
@PreInitializeConfigOptions
public class TemporalPlacementStrategy implements IDPlacementStrategy {

    private static final Logger log =
            LoggerFactory.getLogger(TemporalPlacementStrategy.class);

    public static final ConfigOption<Integer> CONCURRENT_PARTITIONS = new ConfigOption<>(
            GraphDatabaseConfiguration.IDS_NS, "num-partitions",
        "Number of partition blocks to allocate for placement of vertices", ConfigOption.Type.MASKABLE, 10);

    private static final int PARTITION_FINDING_ATTEMPTS = 1000;

    private final Random random = new Random();
    private final int[] currentPartitions;
    private List<PartitionIDRange> localPartitionIdRanges;
    private final Set<Integer> exhaustedPartitions;

    // Static map to track vertex-to-partition assignments globally
    private static final Map<InternalVertex, Integer> vertexPartitionMap = new ConcurrentHashMap<>();

    // Static counter to track the number of vertices in each partition globally
    private static final Map<Integer, Integer> partitionVertexCounts = new ConcurrentHashMap<>();

    public TemporalPlacementStrategy(int concurrentPartitions) {
        Preconditions.checkArgument(concurrentPartitions > 0);
        currentPartitions = new int[concurrentPartitions];
        exhaustedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());
        initializePartitionCounts(concurrentPartitions);
    }

    public TemporalPlacementStrategy(Configuration config) {
        this(config.get(CONCURRENT_PARTITIONS));
    }

    private void initializePartitionCounts(int concurrentPartitions) {
        for (int i = 0; i < concurrentPartitions; i++) {
            partitionVertexCounts.put(i, 0);
        }
    }

    private void updateElement(int index) {
        Preconditions.checkArgument(localPartitionIdRanges != null && !localPartitionIdRanges.isEmpty(),
            "Local partition id ranges have not been initialized");
        int newPartition;
        int attempts = 0;
        do {
            attempts++;
            newPartition = localPartitionIdRanges.get(random.nextInt(localPartitionIdRanges.size())).getRandomID();
            if (attempts > PARTITION_FINDING_ATTEMPTS) throw new IDPoolExhaustedException("Could not find non-exhausted partition");
        } while (exhaustedPartitions.contains(newPartition));
        currentPartitions[index] = newPartition;
        log.debug("Setting partition at index [{}] to: {}", index, newPartition);
    }

    @Override
    public void injectIDManager(IDManager idManager) {} // Not needed here

    @Override
    public int getPartition(InternalElement element) {
        if (element instanceof InternalVertex) {
            // Check for imbalance before applying neighbor-based placement
            if (isImbalanced()) {
                return getLeastLoadedPartition();
            }

            InternalVertex vertex = (InternalVertex) element;

            // Count neighbor partitions using the global map
            Map<Integer, Long> neighborPartitionCounts = new HashMap<>();
            vertex.query().edges().forEach(edge -> {
                InternalVertex neighbor = (InternalVertex) edge.otherVertex(vertex);
                Integer neighborPartition = vertexPartitionMap.get(neighbor);
                if (neighborPartition != null) {
                    Date startTime = (Date) edge.property("startTime");
                    Date endTime = (Date) edge.property("endTime");
                    // A year in MS for edges that have no lifetime.
                    long durationInMs = 31540000000L; 
                    if (startTime != null && endTime != null) {
                        // Calculate duration in days
                        durationInMs = (endTime.getTime() - startTime.getTime());
                    }
                    neighborPartitionCounts.put(neighborPartition,
                            neighborPartitionCounts.getOrDefault(neighborPartition, 0L) + durationInMs);
                }
            });

            // Find the partition with the highest count
            int partitionID = neighborPartitionCounts.entrySet()
                .stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElseGet(this::nextRandomPartitionID); // Fallback to random if no neighbors exist
            incrementVertexCount(partitionID);
            return partitionID;
        }
        int partitionID = nextRandomPartitionID(); // Default fallback
        incrementVertexCount(partitionID);
        return partitionID;
    }

    private int nextRandomPartitionID() {
        return currentPartitions[random.nextInt(currentPartitions.length)];
    }

    @Override
    public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
        for (Map.Entry<InternalVertex, PartitionAssignment> entry : vertices.entrySet()) {
            InternalVertex vertex = entry.getKey();
            int partitionID = getPartition(vertex);

            // Store the vertex-to-partition assignment in the global map
            vertexPartitionMap.put(vertex, partitionID);

            // Update the vertex count for the chosen partition globally
            incrementVertexCount(partitionID);

            entry.setValue(new SimplePartitionAssignment(partitionID));
        }
    }

    @Override
    public boolean supportsBulkPlacement() {
        return true;
    }

    @Override
    public void setLocalPartitionBounds(List<PartitionIDRange> localPartitionIdRanges) {
        Preconditions.checkArgument(localPartitionIdRanges != null && !localPartitionIdRanges.isEmpty());
        this.localPartitionIdRanges = new ArrayList<>(localPartitionIdRanges); // Copy
        for (int i = 0; i < currentPartitions.length; i++) {
            updateElement(i);
        }
    }

    public boolean isExhaustedPartition(int partitionID) {
        return exhaustedPartitions.contains(partitionID);
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        exhaustedPartitions.add(partitionID);
        for (int i = 0; i < currentPartitions.length; i++) {
            if (currentPartitions[i] == partitionID) {
                updateElement(i);
                // Update partition counts
                //partitionVertexCounts.put(partitionID, 0); // Reset count to zero
            }
        }
    }

    /**
     * Checks if there is an imbalance in the number of vertices across partitions.
     * An imbalance occurs if the difference between the maximum and minimum counts exceeds 20%.
     */
    private boolean isImbalanced() {
        int maxCount = Collections.max(partitionVertexCounts.values());
        int minCount = Collections.min(partitionVertexCounts.values());
        return maxCount > minCount * 1.2; // More than 20% difference
    }

    /**
     * Finds the least loaded partition based on vertex counts.
     */
    private int getLeastLoadedPartition() {
        return partitionVertexCounts.entrySet()
                .stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(nextRandomPartitionID()); // Fallback to random if no partitions are found
    }

    /**
     * Increments the vertex count for a given partition globally.
     */
    private void incrementVertexCount(int partitionID) {
        partitionVertexCounts.put(partitionID, partitionVertexCounts.getOrDefault(partitionID, 0) + 1);
    }
}
