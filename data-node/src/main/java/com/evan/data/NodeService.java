package com.evan.data;

import java.util.List;

import com.evan.proto.AppendEntriesRequest;
import com.evan.proto.AppendEntriesResponse;
import com.evan.proto.DeleteRequest;
import com.evan.proto.GetVersionRequest;
import com.evan.proto.GetVersionResponse;
import com.evan.proto.HistoryRequest;
import com.evan.proto.HistoryResponse;
import com.evan.proto.NodeGrpc;
import com.evan.proto.PutRequest;
import com.evan.proto.RecordRequest;
import com.evan.proto.RecordResponse;
import com.evan.proto.RequestStatus;
import com.evan.proto.VersionRecord;
import com.evan.proto.VoteRequest;
import com.evan.proto.VoteResponse;
import com.evan.proto.WriteResponse;

import io.grpc.stub.StreamObserver;

public class NodeService extends NodeGrpc.NodeImplBase {
    private final NodeRuntime runtime;

    public NodeService(NodeRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public void getRecord(RecordRequest request, StreamObserver<RecordResponse> responseObserver) {
        int partitionId = runtime.getPartitionForKey(request.getKey());
        PartitionReplica replica = runtime.getReplicas().get(partitionId);

        if (replica == null) {
            responseObserver.onNext(RecordResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setKey(request.getKey())
                    .setLeaderHint("")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (!runtime.canServeReads(partitionId)) {
            responseObserver.onNext(RecordResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setKey(request.getKey())
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .build());
            responseObserver.onCompleted();
            return;
        }

        runtime.applyCommittedEntriesForPartition(partitionId);

        String value = runtime.getValue(request.getKey());
        if (value == null) {
            responseObserver.onNext(RecordResponse.newBuilder()
                    .setStatus(RequestStatus.NOT_FOUND)
                    .setKey(request.getKey())
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .build());
        } else {
            responseObserver.onNext(RecordResponse.newBuilder()
                    .setStatus(RequestStatus.OK)
                    .setKey(request.getKey())
                    .setValue(value)
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getHistory(HistoryRequest request, StreamObserver<HistoryResponse> responseObserver) {
        int partitionId = runtime.getPartitionForKey(request.getKey());
        PartitionReplica replica = runtime.getReplicas().get(partitionId);

        if (replica == null) {
            responseObserver.onNext(HistoryResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setKey(request.getKey())
                    .setLeaderHint("")
                    .setMessage("This node does not host the partition for the key")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (!runtime.canServeReads(partitionId)) {
            responseObserver.onNext(HistoryResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setKey(request.getKey())
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .setMessage("Partition does not have enough active replicas for quorum")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        runtime.applyCommittedEntriesForPartition(partitionId);

        List<DataConnection.MvccVersion> history = runtime.getHistory(request.getKey());
        if (history.isEmpty()) {
            responseObserver.onNext(HistoryResponse.newBuilder()
                    .setStatus(RequestStatus.NOT_FOUND)
                    .setKey(request.getKey())
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .setMessage("No history found for key")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        HistoryResponse.Builder builder = HistoryResponse.newBuilder()
                .setStatus(RequestStatus.OK)
                .setKey(request.getKey())
                .setLeaderHint(runtime.getLeaderHint(partitionId));

        for (DataConnection.MvccVersion version : history) {
            builder.addVersions(VersionRecord.newBuilder()
                    .setVersion(version.getVersion())
                    .setTerm(version.getTerm())
                    .setOp(version.getOp())
                    .setKey(version.getKey() == null ? "" : version.getKey())
                    .setValue(version.getValue() == null ? "" : version.getValue())
                    .setIsDeleted(version.isDeleted())
                    .setRequestId(version.getRequestId() == null ? "" : version.getRequestId())
                    .build());
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getVersion(GetVersionRequest request, StreamObserver<GetVersionResponse> responseObserver) {
        int partitionId = runtime.getPartitionForKey(request.getKey());
        PartitionReplica replica = runtime.getReplicas().get(partitionId);

        if (replica == null) {
            responseObserver.onNext(GetVersionResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setKey(request.getKey())
                    .setRequestedVersion(request.getVersion())
                    .setLeaderHint("")
                    .setMessage("This node does not host the partition for the key")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (!runtime.canServeReads(partitionId)) {
            responseObserver.onNext(GetVersionResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setKey(request.getKey())
                    .setRequestedVersion(request.getVersion())
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .setMessage("Partition does not have enough active replicas for quorum")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        runtime.applyCommittedEntriesForPartition(partitionId);

        DataConnection.MvccVersion version = runtime.getVersion(request.getKey(), request.getVersion());
        if (version == null) {
            responseObserver.onNext(GetVersionResponse.newBuilder()
                    .setStatus(RequestStatus.NOT_FOUND)
                    .setKey(request.getKey())
                    .setRequestedVersion(request.getVersion())
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .setMessage("Requested version not found for key")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        GetVersionResponse.Builder builder = GetVersionResponse.newBuilder()
                .setStatus(RequestStatus.OK)
                .setKey(request.getKey())
                .setRequestedVersion(request.getVersion())
                .setLeaderHint(runtime.getLeaderHint(partitionId))
                .setVersionRecord(VersionRecord.newBuilder()
                        .setVersion(version.getVersion())
                        .setTerm(version.getTerm())
                        .setOp(version.getOp())
                        .setKey(version.getKey() == null ? "" : version.getKey())
                        .setValue(version.getValue() == null ? "" : version.getValue())
                        .setIsDeleted(version.isDeleted())
                        .setRequestId(version.getRequestId() == null ? "" : version.getRequestId())
                        .build());

        if (version.isDeleted()) {
            builder.setMessage("Requested version is a tombstone");
        } else {
            builder.setMessage("Requested version found");
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void putRecord(PutRequest request, StreamObserver<WriteResponse> responseObserver) {
        int partitionId = runtime.getPartitionForKey(request.getKey());
        PartitionReplica replica = runtime.getReplicas().get(partitionId);

        if (replica == null) {
            responseObserver.onNext(WriteResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setMessage("This node does not host the partition for the key")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (!runtime.canServeWrites(partitionId)) {
            responseObserver.onNext(WriteResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .setMessage("Partition does not have enough active replicas for quorum")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (replica.getRole() != PartitionReplica.Role.LEADER) {
            responseObserver.onNext(WriteResponse.newBuilder()
                    .setStatus(RequestStatus.NOT_LEADER)
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .setMessage("Forward request to current leader")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        boolean committed = runtime.appendClientWrite(
                partitionId,
                LogEntry.OperationType.PUT,
                request.getKey(),
                request.getValue());

        responseObserver.onNext(WriteResponse.newBuilder()
                .setStatus(committed ? RequestStatus.OK : RequestStatus.UNAVAILABLE)
                .setLeaderHint(runtime.getNodeId())
                .setMessage(committed ? "PUT committed" : "Failed to commit PUT to quorum")
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteRecord(DeleteRequest request, StreamObserver<WriteResponse> responseObserver) {
        int partitionId = runtime.getPartitionForKey(request.getKey());
        PartitionReplica replica = runtime.getReplicas().get(partitionId);

        if (replica == null) {
            responseObserver.onNext(WriteResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setMessage("This node does not host the partition for the key")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (!runtime.canServeWrites(partitionId)) {
            responseObserver.onNext(WriteResponse.newBuilder()
                    .setStatus(RequestStatus.UNAVAILABLE)
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .setMessage("Partition does not have enough active replicas for quorum")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (replica.getRole() != PartitionReplica.Role.LEADER) {
            responseObserver.onNext(WriteResponse.newBuilder()
                    .setStatus(RequestStatus.NOT_LEADER)
                    .setLeaderHint(runtime.getLeaderHint(partitionId))
                    .setMessage("Forward request to current leader")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        boolean committed = runtime.appendClientWrite(
                partitionId,
                LogEntry.OperationType.DELETE,
                request.getKey(),
                "");

        responseObserver.onNext(WriteResponse.newBuilder()
                .setStatus(committed ? RequestStatus.OK : RequestStatus.UNAVAILABLE)
                .setLeaderHint(runtime.getNodeId())
                .setMessage(committed ? "DELETE committed" : "Failed to commit DELETE to quorum")
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
        PartitionReplica replica = runtime.getReplicas().get(request.getPartitionId());

        if (replica == null) {
            responseObserver.onNext(VoteResponse.newBuilder()
                    .setTerm(0)
                    .setVoteGranted(false)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        boolean grantVote = false;

        synchronized (replica) {
            if (request.getTerm() < replica.getCurrentTerm()) {
                responseObserver.onNext(VoteResponse.newBuilder()
                        .setTerm(replica.getCurrentTerm())
                        .setVoteGranted(false)
                        .build());
                responseObserver.onCompleted();
                return;
            }

            if (request.getTerm() > replica.getCurrentTerm()) {
                replica.setCurrentTerm(request.getTerm());
                replica.setRole(PartitionReplica.Role.FOLLOWER);
                replica.setVotedFor(null);
                replica.setKnownLeaderId(null);
            }

            if (replica.isPassiveRejoin()) {
                runtime.getRaftStore().saveMeta(
                        replica.getPartitionId(),
                        replica.getCurrentTerm(),
                        replica.getVotedFor(),
                        replica.getCommitIndex(),
                        replica.getLastApplied());

                responseObserver.onNext(VoteResponse.newBuilder()
                        .setTerm(replica.getCurrentTerm())
                        .setVoteGranted(false)
                        .build());
                responseObserver.onCompleted();
                return;
            }

            boolean canVote = replica.getVotedFor() == null || replica.getVotedFor().equals(request.getCandidateId());
            boolean candidateUpToDate = replica.candidateLogAtLeastUpToDate(
                    request.getLastLogIndex(),
                    request.getLastLogTerm());

            if (canVote && candidateUpToDate) {
                grantVote = true;
                replica.setVotedFor(request.getCandidateId());
                runtime.resetElectionDeadline(replica);
            }

            runtime.getRaftStore().saveMeta(
                    replica.getPartitionId(),
                    replica.getCurrentTerm(),
                    replica.getVotedFor(),
                    replica.getCommitIndex(),
                    replica.getLastApplied());

            responseObserver.onNext(VoteResponse.newBuilder()
                    .setTerm(replica.getCurrentTerm())
                    .setVoteGranted(grantVote)
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        PartitionReplica replica = runtime.getReplicas().get(request.getPartitionId());

        if (replica == null) {
            responseObserver.onNext(AppendEntriesResponse.newBuilder()
                    .setTerm(0)
                    .setSuccess(false)
                    .setLastLogIndex(0)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        boolean wasRejoining;
        synchronized (replica) {
            wasRejoining = replica.isPassiveRejoin();

            if (request.getTerm() < replica.getCurrentTerm()) {
                responseObserver.onNext(AppendEntriesResponse.newBuilder()
                        .setTerm(replica.getCurrentTerm())
                        .setSuccess(false)
                        .setLastLogIndex(replica.lastLogIndex())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            if (request.getTerm() > replica.getCurrentTerm()) {
                replica.setCurrentTerm(request.getTerm());
                replica.setVotedFor(null);
            }

            replica.setRole(PartitionReplica.Role.FOLLOWER);
            replica.setKnownLeaderId(request.getLeaderId());
            replica.setLastHeartbeatMs(System.currentTimeMillis());
            replica.markHeardFromLeaderSinceRejoin();
            runtime.resetElectionDeadline(replica);

            if (request.getPrevLogIndex() > replica.lastLogIndex()) {
                runtime.getRaftStore().saveMeta(
                        replica.getPartitionId(),
                        replica.getCurrentTerm(),
                        replica.getVotedFor(),
                        replica.getCommitIndex(),
                        replica.getLastApplied());

                responseObserver.onNext(AppendEntriesResponse.newBuilder()
                        .setTerm(replica.getCurrentTerm())
                        .setSuccess(false)
                        .setLastLogIndex(replica.lastLogIndex())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            if (request.getPrevLogIndex() > 0) {
                long localPrevTerm = replica.termAt(request.getPrevLogIndex());
                if (localPrevTerm != request.getPrevLogTerm()) {
                    runtime.getRaftStore().saveMeta(
                            replica.getPartitionId(),
                            replica.getCurrentTerm(),
                            replica.getVotedFor(),
                            replica.getCommitIndex(),
                            replica.getLastApplied());

                    responseObserver.onNext(AppendEntriesResponse.newBuilder()
                            .setTerm(replica.getCurrentTerm())
                            .setSuccess(false)
                            .setLastLogIndex(replica.lastLogIndex())
                            .build());
                    responseObserver.onCompleted();
                    return;
                }
            }

            for (com.evan.proto.LogEntryMessage msg : request.getEntriesList()) {
                int incomingIndex = msg.getIndex();

                if (incomingIndex <= replica.lastLogIndex()) {
                    long existingTerm = replica.termAt(incomingIndex);
                    if (existingTerm != msg.getTerm()) {
                        runtime.getRaftStore().deleteLogFrom(replica.getPartitionId(), incomingIndex);
                        replica.deleteFrom(incomingIndex);
                    }
                }

                if (incomingIndex > replica.lastLogIndex()) {
                    LogEntry.OperationType op = LogEntry.OperationType.valueOf(msg.getOp());
                    LogEntry entry = new LogEntry(
                            msg.getIndex(),
                            msg.getTerm(),
                            op,
                            msg.getKey(),
                            msg.getValue(),
                            msg.getRequestId());

                    runtime.getRaftStore().appendLogEntry(replica.getPartitionId(), entry);
                    replica.appendEntry(entry);
                }
            }

            int newCommitIndex = Math.min(request.getLeaderCommit(), replica.lastLogIndex());
            if (newCommitIndex > replica.getCommitIndex()) {
                replica.setCommitIndex(newCommitIndex);
            }

            runtime.getRaftStore().saveMeta(
                    replica.getPartitionId(),
                    replica.getCurrentTerm(),
                    replica.getVotedFor(),
                    replica.getCommitIndex(),
                    replica.getLastApplied());

            responseObserver.onNext(AppendEntriesResponse.newBuilder()
                    .setTerm(replica.getCurrentTerm())
                    .setSuccess(true)
                    .setLastLogIndex(replica.lastLogIndex())
                    .build());
            responseObserver.onCompleted();
        }

        runtime.applyCommittedEntriesForPartition(replica.getPartitionId());

        for (com.evan.proto.LogEntryMessage msg : request.getEntriesList()) {
            LogEntry.OperationType op = LogEntry.OperationType.valueOf(msg.getOp());

            if (NodeRuntime.TRACE_ENABLED) {
                if (wasRejoining) {
                    if (op == LogEntry.OperationType.PUT) {
                        System.out.println("catching up put key: " + msg.getKey() + ", value: " + msg.getValue());
                    } else if (op == LogEntry.OperationType.DELETE) {
                        System.out.println("catching up delete key: " + msg.getKey());
                    }
                } else {
                    if (op == LogEntry.OperationType.PUT) {
                        System.out.println("follower put key: " + msg.getKey() + ", value: " + msg.getValue());
                    } else if (op == LogEntry.OperationType.DELETE) {
                        System.out.println("follower delete key: " + msg.getKey());
                    }
                }
            }
        }
    }
}