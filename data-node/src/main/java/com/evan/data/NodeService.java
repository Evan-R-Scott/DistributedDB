package com.evan.data;

import com.evan.proto.NodeGrpc;
import com.evan.proto.RecordRequest;
import com.evan.proto.FullRecord;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;

public class NodeService extends NodeGrpc.NodeImplBase {
    @Override
    public void getRecord(RecordRequest request, StreamObserver<FullRecord> responseObserver) {
    }

    @Override
    public void putRecord(FullRecord request, StreamObserver<Empty> responseObserver) {
    }

    @Override
    public void deleteRecord(RecordRequest request, StreamObserver<Empty> responseObserver) {
    }
}
