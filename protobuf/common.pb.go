// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        v5.29.3
// source: common.proto

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

type Batch struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Uuid          string                 `protobuf:"bytes,1,opt,name=uuid,proto3" json:"uuid,omitempty"`
	Size          int32                  `protobuf:"varint,2,opt,name=size,proto3" json:"size,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Batch) Reset() {
	*x = Batch{}
	mi := &file_common_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Batch) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Batch) ProtoMessage() {}

func (x *Batch) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Batch.ProtoReflect.Descriptor instead.
func (*Batch) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{0}
}

func (x *Batch) GetUuid() string {
	if x != nil {
		return x.Uuid
	}
	return ""
}

func (x *Batch) GetSize() int32 {
	if x != nil {
		return x.Size
	}
	return 0
}

type Batches struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Batches       []*Batch               `protobuf:"bytes,1,rep,name=batches,proto3" json:"batches,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Batches) Reset() {
	*x = Batches{}
	mi := &file_common_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Batches) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Batches) ProtoMessage() {}

func (x *Batches) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Batches.ProtoReflect.Descriptor instead.
func (*Batches) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{1}
}

func (x *Batches) GetBatches() []*Batch {
	if x != nil {
		return x.Batches
	}
	return nil
}

type ClientFileRequestToMaster struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	FileName      string                 `protobuf:"bytes,1,opt,name=file_name,json=fileName,proto3" json:"file_name,omitempty"`
	FileFormat    string                 `protobuf:"bytes,2,opt,name=file_format,json=fileFormat,proto3" json:"file_format,omitempty"`
	Hash          int32                  `protobuf:"varint,3,opt,name=hash,proto3" json:"hash,omitempty"`
	FileSize      int64                  `protobuf:"varint,4,opt,name=file_size,json=fileSize,proto3" json:"file_size,omitempty"`
	BatchInfo     *Batches               `protobuf:"bytes,5,opt,name=batch_info,json=batchInfo,proto3" json:"batch_info,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ClientFileRequestToMaster) Reset() {
	*x = ClientFileRequestToMaster{}
	mi := &file_common_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ClientFileRequestToMaster) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ClientFileRequestToMaster) ProtoMessage() {}

func (x *ClientFileRequestToMaster) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ClientFileRequestToMaster.ProtoReflect.Descriptor instead.
func (*ClientFileRequestToMaster) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{2}
}

func (x *ClientFileRequestToMaster) GetFileName() string {
	if x != nil {
		return x.FileName
	}
	return ""
}

func (x *ClientFileRequestToMaster) GetFileFormat() string {
	if x != nil {
		return x.FileFormat
	}
	return ""
}

func (x *ClientFileRequestToMaster) GetHash() int32 {
	if x != nil {
		return x.Hash
	}
	return 0
}

func (x *ClientFileRequestToMaster) GetFileSize() int64 {
	if x != nil {
		return x.FileSize
	}
	return 0
}

func (x *ClientFileRequestToMaster) GetBatchInfo() *Batches {
	if x != nil {
		return x.BatchInfo
	}
	return nil
}

type ClientBatchRequestToMaster struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	BatchId       string                 `protobuf:"bytes,1,opt,name=batch_id,json=batchId,proto3" json:"batch_id,omitempty"`
	BatchSize     int32                  `protobuf:"varint,2,opt,name=batch_size,json=batchSize,proto3" json:"batch_size,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ClientBatchRequestToMaster) Reset() {
	*x = ClientBatchRequestToMaster{}
	mi := &file_common_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ClientBatchRequestToMaster) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ClientBatchRequestToMaster) ProtoMessage() {}

func (x *ClientBatchRequestToMaster) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ClientBatchRequestToMaster.ProtoReflect.Descriptor instead.
func (*ClientBatchRequestToMaster) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{3}
}

func (x *ClientBatchRequestToMaster) GetBatchId() string {
	if x != nil {
		return x.BatchId
	}
	return ""
}

func (x *ClientBatchRequestToMaster) GetBatchSize() int32 {
	if x != nil {
		return x.BatchSize
	}
	return 0
}

type MasterResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	WorkerIp      string                 `protobuf:"bytes,1,opt,name=worker_ip,json=workerIp,proto3" json:"worker_ip,omitempty"`
	WorkerPort    int32                  `protobuf:"varint,2,opt,name=worker_port,json=workerPort,proto3" json:"worker_port,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *MasterResponse) Reset() {
	*x = MasterResponse{}
	mi := &file_common_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MasterResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MasterResponse) ProtoMessage() {}

func (x *MasterResponse) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MasterResponse.ProtoReflect.Descriptor instead.
func (*MasterResponse) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{4}
}

func (x *MasterResponse) GetWorkerIp() string {
	if x != nil {
		return x.WorkerIp
	}
	return ""
}

func (x *MasterResponse) GetWorkerPort() int32 {
	if x != nil {
		return x.WorkerPort
	}
	return 0
}

type MasterFileResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Success       bool                   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *MasterFileResponse) Reset() {
	*x = MasterFileResponse{}
	mi := &file_common_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MasterFileResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MasterFileResponse) ProtoMessage() {}

func (x *MasterFileResponse) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MasterFileResponse.ProtoReflect.Descriptor instead.
func (*MasterFileResponse) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{5}
}

func (x *MasterFileResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

type WorkerResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Success       bool                   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *WorkerResponse) Reset() {
	*x = WorkerResponse{}
	mi := &file_common_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WorkerResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WorkerResponse) ProtoMessage() {}

func (x *WorkerResponse) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WorkerResponse.ProtoReflect.Descriptor instead.
func (*WorkerResponse) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{6}
}

func (x *WorkerResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

type BatchLocation struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	WorkerIds     []string               `protobuf:"bytes,1,rep,name=worker_ids,json=workerIds,proto3" json:"worker_ids,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *BatchLocation) Reset() {
	*x = BatchLocation{}
	mi := &file_common_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BatchLocation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchLocation) ProtoMessage() {}

func (x *BatchLocation) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchLocation.ProtoReflect.Descriptor instead.
func (*BatchLocation) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{7}
}

func (x *BatchLocation) GetWorkerIds() []string {
	if x != nil {
		return x.WorkerIds
	}
	return nil
}

type MasterMetadataResponse struct {
	state          protoimpl.MessageState    `protogen:"open.v1"`
	BatchIds       []string                  `protobuf:"bytes,1,rep,name=batch_ids,json=batchIds,proto3" json:"batch_ids,omitempty"`
	BatchLocations map[string]*BatchLocation `protobuf:"bytes,2,rep,name=batch_locations,json=batchLocations,proto3" json:"batch_locations,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *MasterMetadataResponse) Reset() {
	*x = MasterMetadataResponse{}
	mi := &file_common_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MasterMetadataResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MasterMetadataResponse) ProtoMessage() {}

func (x *MasterMetadataResponse) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MasterMetadataResponse.ProtoReflect.Descriptor instead.
func (*MasterMetadataResponse) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{8}
}

func (x *MasterMetadataResponse) GetBatchIds() []string {
	if x != nil {
		return x.BatchIds
	}
	return nil
}

func (x *MasterMetadataResponse) GetBatchLocations() map[string]*BatchLocation {
	if x != nil {
		return x.BatchLocations
	}
	return nil
}

type Location struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	FileName      string                 `protobuf:"bytes,1,opt,name=file_name,json=fileName,proto3" json:"file_name,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Location) Reset() {
	*x = Location{}
	mi := &file_common_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Location) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Location) ProtoMessage() {}

func (x *Location) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Location.ProtoReflect.Descriptor instead.
func (*Location) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{9}
}

func (x *Location) GetFileName() string {
	if x != nil {
		return x.FileName
	}
	return ""
}

var File_common_proto protoreflect.FileDescriptor

var file_common_proto_rawDesc = string([]byte{
	0x0a, 0x0c, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06,
	0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x22, 0x2f, 0x0a, 0x05, 0x42, 0x61, 0x74, 0x63, 0x68, 0x12,
	0x12, 0x0a, 0x04, 0x75, 0x75, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75,
	0x75, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x22, 0x32, 0x0a, 0x07, 0x42, 0x61, 0x74, 0x63, 0x68,
	0x65, 0x73, 0x12, 0x27, 0x0a, 0x07, 0x62, 0x61, 0x74, 0x63, 0x68, 0x65, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x42, 0x61, 0x74,
	0x63, 0x68, 0x52, 0x07, 0x62, 0x61, 0x74, 0x63, 0x68, 0x65, 0x73, 0x22, 0xba, 0x01, 0x0a, 0x19,
	0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x46, 0x69, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x54, 0x6f, 0x4d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x12, 0x1b, 0x0a, 0x09, 0x66, 0x69, 0x6c,
	0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x66, 0x69,
	0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x66, 0x69, 0x6c, 0x65, 0x5f, 0x66,
	0x6f, 0x72, 0x6d, 0x61, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x66, 0x69, 0x6c,
	0x65, 0x46, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61, 0x73, 0x68, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x12, 0x1b, 0x0a, 0x09, 0x66,
	0x69, 0x6c, 0x65, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08,
	0x66, 0x69, 0x6c, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x12, 0x2e, 0x0a, 0x0a, 0x62, 0x61, 0x74, 0x63,
	0x68, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x63,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x65, 0x73, 0x52, 0x09, 0x62,
	0x61, 0x74, 0x63, 0x68, 0x49, 0x6e, 0x66, 0x6f, 0x22, 0x56, 0x0a, 0x1a, 0x43, 0x6c, 0x69, 0x65,
	0x6e, 0x74, 0x42, 0x61, 0x74, 0x63, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x54, 0x6f,
	0x4d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x12, 0x19, 0x0a, 0x08, 0x62, 0x61, 0x74, 0x63, 0x68, 0x5f,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x62, 0x61, 0x74, 0x63, 0x68, 0x49,
	0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x62, 0x61, 0x74, 0x63, 0x68, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x09, 0x62, 0x61, 0x74, 0x63, 0x68, 0x53, 0x69, 0x7a, 0x65,
	0x22, 0x4e, 0x0a, 0x0e, 0x4d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x5f, 0x69, 0x70, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x49, 0x70, 0x12,
	0x1f, 0x0a, 0x0b, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x5f, 0x70, 0x6f, 0x72, 0x74, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x05, 0x52, 0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x50, 0x6f, 0x72, 0x74,
	0x22, 0x2e, 0x0a, 0x12, 0x4d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x46, 0x69, 0x6c, 0x65, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73,
	0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73,
	0x22, 0x2a, 0x0a, 0x0e, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x22, 0x2e, 0x0a, 0x0d,
	0x42, 0x61, 0x74, 0x63, 0x68, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1d, 0x0a,
	0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x09, 0x52, 0x09, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x49, 0x64, 0x73, 0x22, 0xec, 0x01, 0x0a,
	0x16, 0x4d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x62, 0x61, 0x74, 0x63, 0x68,
	0x5f, 0x69, 0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x08, 0x62, 0x61, 0x74, 0x63,
	0x68, 0x49, 0x64, 0x73, 0x12, 0x5b, 0x0a, 0x0f, 0x62, 0x61, 0x74, 0x63, 0x68, 0x5f, 0x6c, 0x6f,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x32, 0x2e,
	0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x4d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x4d, 0x65, 0x74,
	0x61, 0x64, 0x61, 0x74, 0x61, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x42, 0x61,
	0x74, 0x63, 0x68, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x45, 0x6e, 0x74, 0x72,
	0x79, 0x52, 0x0e, 0x62, 0x61, 0x74, 0x63, 0x68, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x1a, 0x58, 0x0a, 0x13, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x2b, 0x0a, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x63, 0x6f, 0x6d, 0x6d,
	0x6f, 0x6e, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x27, 0x0a, 0x08, 0x4c,
	0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1b, 0x0a, 0x09, 0x66, 0x69, 0x6c, 0x65, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x66, 0x69, 0x6c, 0x65,
	0x4e, 0x61, 0x6d, 0x65, 0x42, 0x2b, 0x5a, 0x29, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x72, 0x61, 0x7a, 0x76, 0x61, 0x6e, 0x6d, 0x61, 0x72, 0x69, 0x6e, 0x6e, 0x2f,
	0x64, 0x61, 0x74, 0x61, 0x6c, 0x61, 0x6b, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_common_proto_rawDescOnce sync.Once
	file_common_proto_rawDescData []byte
)

func file_common_proto_rawDescGZIP() []byte {
	file_common_proto_rawDescOnce.Do(func() {
		file_common_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_common_proto_rawDesc), len(file_common_proto_rawDesc)))
	})
	return file_common_proto_rawDescData
}

var file_common_proto_msgTypes = make([]protoimpl.MessageInfo, 11)
var file_common_proto_goTypes = []any{
	(*Batch)(nil),                      // 0: common.Batch
	(*Batches)(nil),                    // 1: common.Batches
	(*ClientFileRequestToMaster)(nil),  // 2: common.ClientFileRequestToMaster
	(*ClientBatchRequestToMaster)(nil), // 3: common.ClientBatchRequestToMaster
	(*MasterResponse)(nil),             // 4: common.MasterResponse
	(*MasterFileResponse)(nil),         // 5: common.MasterFileResponse
	(*WorkerResponse)(nil),             // 6: common.WorkerResponse
	(*BatchLocation)(nil),              // 7: common.BatchLocation
	(*MasterMetadataResponse)(nil),     // 8: common.MasterMetadataResponse
	(*Location)(nil),                   // 9: common.Location
	nil,                                // 10: common.MasterMetadataResponse.BatchLocationsEntry
}
var file_common_proto_depIdxs = []int32{
	0,  // 0: common.Batches.batches:type_name -> common.Batch
	1,  // 1: common.ClientFileRequestToMaster.batch_info:type_name -> common.Batches
	10, // 2: common.MasterMetadataResponse.batch_locations:type_name -> common.MasterMetadataResponse.BatchLocationsEntry
	7,  // 3: common.MasterMetadataResponse.BatchLocationsEntry.value:type_name -> common.BatchLocation
	4,  // [4:4] is the sub-list for method output_type
	4,  // [4:4] is the sub-list for method input_type
	4,  // [4:4] is the sub-list for extension type_name
	4,  // [4:4] is the sub-list for extension extendee
	0,  // [0:4] is the sub-list for field type_name
}

func init() { file_common_proto_init() }
func file_common_proto_init() {
	if File_common_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_common_proto_rawDesc), len(file_common_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   11,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_common_proto_goTypes,
		DependencyIndexes: file_common_proto_depIdxs,
		MessageInfos:      file_common_proto_msgTypes,
	}.Build()
	File_common_proto = out.File
	file_common_proto_goTypes = nil
	file_common_proto_depIdxs = nil
}
