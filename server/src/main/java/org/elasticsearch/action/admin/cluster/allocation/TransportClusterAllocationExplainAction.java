/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation.DebugMode;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;

/**
 * The {@code TransportClusterAllocationExplainAction} is responsible for actually executing the explanation of a shard's allocation on the
 * master node in the cluster.
 */
public class TransportClusterAllocationExplainAction extends TransportMasterNodeAction<
    ClusterAllocationExplainRequest,
    ClusterAllocationExplainResponse> {

    public static final ActionType<ClusterAllocationExplainResponse> TYPE = new ActionType<>("cluster:monitor/allocation/explain");
    private static final Logger logger = LogManager.getLogger(TransportClusterAllocationExplainAction.class);

    private final ClusterInfoService clusterInfoService;
    private final SnapshotsInfoService snapshotsInfoService;
    private final AllocationDeciders allocationDeciders;
    private final AllocationService allocationService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportClusterAllocationExplainAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        AllocationDeciders allocationDeciders,
        AllocationService allocationService,
        ProjectResolver projectResolver
    ) {
        super(
            TYPE.name(),
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterAllocationExplainRequest::new,
            ClusterAllocationExplainResponse::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.clusterInfoService = clusterInfoService;
        this.snapshotsInfoService = snapshotsInfoService;
        this.allocationDeciders = allocationDeciders;
        this.allocationService = allocationService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterAllocationExplainRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final ClusterAllocationExplainRequest request,
        final ClusterState state,
        final ActionListener<ClusterAllocationExplainResponse> listener
    ) {
        final ClusterInfo clusterInfo = clusterInfoService.getClusterInfo();
        final Collection<ProjectId> projectIds = projectResolver.getProjectIds(state);
        final RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            state,
            clusterInfo,
            snapshotsInfoService.snapshotShardSizes(),
            System.nanoTime()
        );

        ShardRouting shardRouting = findShardToExplain(request, allocation, projectIds);
        logger.debug("explaining the allocation for [{}], found shard [{}]", request, shardRouting);

        ClusterAllocationExplanation cae = explainShard(
            shardRouting,
            allocation,
            request.includeDiskInfo() ? clusterInfo : null,
            request.includeYesDecisions(),
            request.useAnyUnassignedShard() == false,
            allocationService
        );
        listener.onResponse(new ClusterAllocationExplainResponse(cae));
    }

    // public for testing
    public static ClusterAllocationExplanation explainShard(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        ClusterInfo clusterInfo,
        boolean includeYesDecisions,
        boolean isSpecificShard,
        AllocationService allocationService
    ) {

        allocation.setDebugMode(includeYesDecisions ? DebugMode.ON : DebugMode.EXCLUDE_YES_DECISIONS);

        ShardAllocationDecision shardDecision;
        if (shardRouting.initializing() || shardRouting.relocating()) {
            shardDecision = ShardAllocationDecision.NOT_TAKEN;
        } else {
            shardDecision = allocationService.explainShardAllocation(shardRouting, allocation);
        }

        return new ClusterAllocationExplanation(
            isSpecificShard,
            shardRouting,
            shardRouting.currentNodeId() != null ? allocation.nodes().get(shardRouting.currentNodeId()) : null,
            shardRouting.relocatingNodeId() != null ? allocation.nodes().get(shardRouting.relocatingNodeId()) : null,
            clusterInfo,
            shardDecision
        );
    }

    // public for testing
    public static ShardRouting findShardToExplain(
        ClusterAllocationExplainRequest request,
        RoutingAllocation allocation,
        Collection<ProjectId> projectIds
    ) {
        ShardRouting foundShard = null;
        if (request.useAnyUnassignedShard()) {
            // If we can use any shard, return the first unassigned primary (if there is one) or the first unassigned replica (if not)
            for (ShardRouting unassigned : allocation.routingNodes().unassigned()) {
                final ProjectId projectId = allocation.metadata().lookupProject(unassigned.index()).map(ProjectMetadata::id).orElse(null);
                if (projectIds.contains(projectId)) {
                    if (foundShard == null || unassigned.primary()) {
                        foundShard = unassigned;
                    }
                    if (foundShard.primary()) {
                        break;
                    }
                }
            }
            if (foundShard == null) {
                throw new IllegalArgumentException(Strings.format("""
                    There are no unassigned shards in this cluster. Specify an assigned shard in the request body to explain its \
                    allocation. See %s for more information.""", ReferenceDocs.ALLOCATION_EXPLAIN_API));
            }
        } else {
            if (projectIds.size() != 1) {
                throw new IllegalArgumentException("an explain action for a named index must target exactly one project");
            }
            final ProjectId projectId = projectIds.iterator().next();
            String index = request.getIndex();
            int shard = request.getShard();
            final IndexShardRoutingTable indexShardRoutingTable = allocation.routingTable(projectId).shardRoutingTable(index, shard);
            if (request.isPrimary()) {
                // If we're looking for the primary shard, there's only one copy, so pick it directly
                foundShard = indexShardRoutingTable.primaryShard();
                if (request.getCurrentNode() != null) {
                    DiscoveryNode primaryNode = allocation.nodes().resolveNode(request.getCurrentNode());
                    // the primary is assigned to a node other than the node specified in the request
                    if (primaryNode.getId().equals(foundShard.currentNodeId()) == false) {
                        throw new IllegalArgumentException(
                            "unable to find primary shard assigned to node [" + request.getCurrentNode() + "]"
                        );
                    }
                }
            } else {
                // If looking for a replica, go through all the replica shards
                List<ShardRouting> replicaShardRoutings = indexShardRoutingTable.replicaShards();
                if (request.getCurrentNode() != null) {
                    // the request is to explain a replica shard already assigned on a particular node,
                    // so find that shard copy
                    DiscoveryNode replicaNode = allocation.nodes().resolveNode(request.getCurrentNode());
                    for (ShardRouting replica : replicaShardRoutings) {
                        if (replicaNode.getId().equals(replica.currentNodeId())) {
                            foundShard = replica;
                            break;
                        }
                    }
                    if (foundShard == null) {
                        throw new IllegalArgumentException(
                            "unable to find a replica shard assigned to node [" + request.getCurrentNode() + "]"
                        );
                    }
                } else {
                    if (replicaShardRoutings.size() > 0) {
                        // Pick the first replica at the very least
                        foundShard = replicaShardRoutings.get(0);
                        for (ShardRouting replica : replicaShardRoutings) {
                            // In case there are multiple replicas where some are assigned and some aren't,
                            // try to find one that is unassigned at least
                            if (replica.unassigned()) {
                                foundShard = replica;
                                break;
                            } else if (replica.started() && (foundShard.initializing() || foundShard.relocating())) {
                                // prefer started shards to initializing or relocating shards because started shards
                                // can be explained
                                foundShard = replica;
                            }
                        }
                    }
                }
            }
        }

        if (foundShard == null) {
            throw new IllegalArgumentException("unable to find any shards to explain [" + request + "] in the routing table");
        }
        return foundShard;
    }
}
