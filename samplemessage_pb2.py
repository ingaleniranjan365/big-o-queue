# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: protobufgenerator/samplemessage.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='protobufgenerator/samplemessage.proto',
  package='protobufgenerator',
  syntax='proto2',
  serialized_options=None,
  serialized_pb=_b('\n%protobufgenerator/samplemessage.proto\x12\x11protobufgenerator\"9\n\rSampleMessage\x12\x11\n\tfile_name\x18\x01 \x02(\t\x12\x15\n\x0c\x66ile_content\x18\xff\x01 \x02(\t')
)




_SAMPLEMESSAGE = _descriptor.Descriptor(
  name='SampleMessage',
  full_name='protobufgenerator.SampleMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='file_name', full_name='protobufgenerator.SampleMessage.file_name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='file_content', full_name='protobufgenerator.SampleMessage.file_content', index=1,
      number=255, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=60,
  serialized_end=117,
)

DESCRIPTOR.message_types_by_name['SampleMessage'] = _SAMPLEMESSAGE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

SampleMessage = _reflection.GeneratedProtocolMessageType('SampleMessage', (_message.Message,), {
  'DESCRIPTOR' : _SAMPLEMESSAGE,
  '__module__' : 'protobufgenerator.samplemessage_pb2'
  # @@protoc_insertion_point(class_scope:protobufgenerator.SampleMessage)
  })
_sym_db.RegisterMessage(SampleMessage)


# @@protoc_insertion_point(module_scope)
