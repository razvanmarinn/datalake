// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        v5.29.3
// source: worker.proto

package protobuf

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type GetClientRequestToWorker struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	BatchId       string                 `protobuf:"bytes,1,opt,name=batch_id,json=batchId,proto3" json:"batch_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetClientRequestToWorker) Reset() {
	*x = GetClientRequestToWorker{}
	mi := &file_worker_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetClientRequestToWorker) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetClientRequestToWorker) ProtoMessage() {}

func (x *GetClientRequestToWorker) ProtoReflect() protoreflect.Message {
	mi := &file_worker_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetClientRequestToWorker.ProtoReflect.Descriptor instead.
func (*GetClientRequestToWorker) Descriptor() ([]byte, []int) {
	return file_worker_proto_rawDescGZIP(), []int{0}
}

func (x *GetClientRequestToWorker) GetBatchId() string {
	if x != nil {
		return x.BatchId
	}
	return ""
}

type SendClientRequestToWorker struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	BatchId       string                 `protobuf:"bytes,1,opt,name=batch_id,json=batchId,proto3" json:"batch_id,omitempty"`
	Data          []byte                 `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SendClientRequestToWorker) Reset() {
	*x = SendClientRequestToWorker{}
	mi := &file_worker_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SendClientRequestToWorker) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SendClientRequestToWorker) ProtoMessage() {}

func (x *SendClientRequestToWorker) ProtoReflect() protoreflect.Message {
	mi := &file_worker_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SendClientRequestToWorker.ProtoReflect.Descriptor instead.
func (*SendClientRequestToWorker) Descriptor() ([]byte, []int) {
	return file_worker_proto_rawDescGZIP(), []int{1}
}

func (x *SendClientRequestToWorker) GetBatchId() string {
	if x != nil {
		return x.BatchId
	}
	return ""
}

func (x *SendClientRequestToWorker) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type WorkerBatchResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	BatchData     []byte                 `protobuf:"bytes,1,opt,name=batch_data,json=batchData,proto3" json:"batch_data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *WorkerBatchResponse) Reset() {
	*x = WorkerBatchResponse{}
	mi := &file_worker_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WorkerBatchResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WorkerBatchResponse) ProtoMessage() {}

func (x *WorkerBatchResponse) ProtoReflect() protoreflect.Message {
	mi := &file_worker_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WorkerBatchResponse.ProtoReflect.Descriptor instead.
func (*WorkerBatchResponse) Descriptor() ([]byte, []int) {
	return file_worker_proto_rawDescGZIP(), []int{2}
}

func (x *WorkerBatchResponse) GetBatchData() []byte {
	if x != nil {
		return x.BatchData
	}
	return nil
}

var File_worker_proto protoreflect.FileDescriptor

var file_worker_proto_rawDesc = string([]byte{
	0x0a, 0x0c, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0a,
	0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x5f, 0x64, 0x66, 0x73, 0x1a, 0x0c, 0x63, 0x6f, 0x6d, 0x6d,
	0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x35, 0x0a, 0x18, 0x47, 0x65, 0x74, 0x43,
	0x6c, 0x69, 0x65, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x54, 0x6f, 0x57, 0x6f,
	0x72, 0x6b, 0x65, 0x72, 0x12, 0x19, 0x0a, 0x08, 0x62, 0x61, 0x74, 0x63, 0x68, 0x5f, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x62, 0x61, 0x74, 0x63, 0x68, 0x49, 0x64, 0x22,
	0x4a, 0x0a, 0x19, 0x53, 0x65, 0x6e, 0x64, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x54, 0x6f, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x12, 0x19, 0x0a, 0x08,
	0x62, 0x61, 0x74, 0x63, 0x68, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07,
	0x62, 0x61, 0x74, 0x63, 0x68, 0x49, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x34, 0x0a, 0x13, 0x57,
	0x6f, 0x72, 0x6b, 0x65, 0x72, 0x42, 0x61, 0x74, 0x63, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x62, 0x61, 0x74, 0x63, 0x68, 0x5f, 0x64, 0x61, 0x74, 0x61,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x62, 0x61, 0x74, 0x63, 0x68, 0x44, 0x61, 0x74,
	0x61, 0x32, 0xca, 0x01, 0x0a, 0x14, 0x42, 0x61, 0x74, 0x63, 0x68, 0x52, 0x65, 0x63, 0x65, 0x69,
	0x76, 0x65, 0x72, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x4f, 0x0a, 0x0c, 0x52, 0x65,
	0x63, 0x65, 0x69, 0x76, 0x65, 0x42, 0x61, 0x74, 0x63, 0x68, 0x12, 0x25, 0x2e, 0x77, 0x6f, 0x72,
	0x6b, 0x65, 0x72, 0x5f, 0x64, 0x66, 0x73, 0x2e, 0x53, 0x65, 0x6e, 0x64, 0x43, 0x6c, 0x69, 0x65,
	0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x54, 0x6f, 0x57, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x1a, 0x16, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x57, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x61, 0x0a, 0x16, 0x52,
	0x65, 0x74, 0x72, 0x69, 0x65, 0x76, 0x65, 0x42, 0x61, 0x74, 0x63, 0x68, 0x46, 0x6f, 0x72, 0x43,
	0x6c, 0x69, 0x65, 0x6e, 0x74, 0x12, 0x24, 0x2e, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x5f, 0x64,
	0x66, 0x73, 0x2e, 0x47, 0x65, 0x74, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x54, 0x6f, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x1a, 0x1f, 0x2e, 0x77, 0x6f,
	0x72, 0x6b, 0x65, 0x72, 0x5f, 0x64, 0x66, 0x73, 0x2e, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x42,
	0x61, 0x74, 0x63, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x2b,
	0x5a, 0x29, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x72, 0x61, 0x7a,
	0x76, 0x61, 0x6e, 0x6d, 0x61, 0x72, 0x69, 0x6e, 0x6e, 0x2f, 0x64, 0x61, 0x74, 0x61, 0x6c, 0x61,
	0x6b, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
})

var (
	file_worker_proto_rawDescOnce sync.Once
	file_worker_proto_rawDescData []byte
)

func file_worker_proto_rawDescGZIP() []byte {
	file_worker_proto_rawDescOnce.Do(func() {
		file_worker_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_worker_proto_rawDesc), len(file_worker_proto_rawDesc)))
	})
	return file_worker_proto_rawDescData
}

var file_worker_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_worker_proto_goTypes = []any{
	(*GetClientRequestToWorker)(nil),  // 0: worker_dfs.GetClientRequestToWorker
	(*SendClientRequestToWorker)(nil), // 1: worker_dfs.SendClientRequestToWorker
	(*WorkerBatchResponse)(nil),       // 2: worker_dfs.WorkerBatchResponse
	(*WorkerResponse)(nil),            // 3: common.WorkerResponse
}
var file_worker_proto_depIdxs = []int32{
	1, // 0: worker_dfs.BatchReceiverService.ReceiveBatch:input_type -> worker_dfs.SendClientRequestToWorker
	0, // 1: worker_dfs.BatchReceiverService.RetrieveBatchForClient:input_type -> worker_dfs.GetClientRequestToWorker
	3, // 2: worker_dfs.BatchReceiverService.ReceiveBatch:output_type -> common.WorkerResponse
	2, // 3: worker_dfs.BatchReceiverService.RetrieveBatchForClient:output_type -> worker_dfs.WorkerBatchResponse
	2, // [2:4] is the sub-list for method output_type
	0, // [0:2] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_worker_proto_init() }
func file_worker_proto_init() {
	if File_worker_proto != nil {
		return
	}
	file_common_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_worker_proto_rawDesc), len(file_worker_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_worker_proto_goTypes,
		DependencyIndexes: file_worker_proto_depIdxs,
		MessageInfos:      file_worker_proto_msgTypes,
	}.Build()
	File_worker_proto = out.File
	file_worker_proto_goTypes = nil
	file_worker_proto_depIdxs = nil
}
