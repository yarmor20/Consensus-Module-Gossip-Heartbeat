# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: inter-server-rpcs.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x17inter-server-rpcs.proto\"\x1c\n\tHeartbeat\x12\x0f\n\x07message\x18\x01 \x01(\t\"\x1b\n\x0bPullEntries\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\t2\xca\x01\n\x15InterServerRPCHandler\x12(\n\x0cHeartbeatRPC\x12\n.Heartbeat\x1a\n.Heartbeat\"\x00\x12*\n\x0eRequestVoteRPC\x12\n.Heartbeat\x1a\n.Heartbeat\"\x00\x12,\n\x0ePullEntriesRPC\x12\n.Heartbeat\x1a\x0c.PullEntries\"\x00\x12-\n\x11UpdatePositionRPC\x12\n.Heartbeat\x1a\n.Heartbeat\"\x00\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'inter_server_rpcs_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _HEARTBEAT._serialized_start=27
  _HEARTBEAT._serialized_end=55
  _PULLENTRIES._serialized_start=57
  _PULLENTRIES._serialized_end=84
  _INTERSERVERRPCHANDLER._serialized_start=87
  _INTERSERVERRPCHANDLER._serialized_end=289
# @@protoc_insertion_point(module_scope)
