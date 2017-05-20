{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}

module Data.Hadoop.Protobuf.Headers where

import Data.ByteString (ByteString)
import Data.Int (Int32)
import Data.ProtocolBuffers
import Data.ProtocolBuffers.Orphans ()
import Data.Text (Text)
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)

------------------------------------------------------------------------

-- TODO From Hadoop source:
--  Spec for UserInformationProto is specified in ProtoUtil#makeIpcConnectionContext

-- | User information beyond what can be determined as part of the
-- security handshake at connection time (kerberos, tokens, etc)
data UserInformation = UserInformation
    { effectiveUser :: Optional 1 (Value Text)
    , realUser      :: Optional 2 (Value Text)
    } deriving (Generic, Show)

instance Encode UserInformation
instance Decode UserInformation

-- | The connection context is sent as part of the connection
-- establishment. It establishes the context for ALL Rpc calls
-- within the connection.
data IpcConnectionContext = IpcConnectionContext
    { ctxUserInfo :: Optional 2 (Message UserInformation)
    , ctxProtocol :: Optional 3 (Value Text) -- ^ Name of the next RPC layer.
    } deriving (Generic, Show)

instance Encode IpcConnectionContext
instance Decode IpcConnectionContext

------------------------------------------------------------------------

-- | Determines the RPC Engine and the serialization of the RPC request.
data RpcKind = Builtin        -- ^ Used for built-in calls by tests
             | Writable       -- ^ Use WritableRpcEngine
             | ProtocolBuffer -- ^ Use ProtobufRpcEngine
  deriving (Generic, Show, Enum)

data RpcOperation = FinalPacket        -- ^ The final RPC packet
                  | ContinuationPacket -- ^ Not implemented yet
                  | CloseConnection    -- ^ Close the RPC connection
  deriving (Generic, Show, Enum)

data RpcRequestHeader = RpcRequestHeader
    { reqKind       :: Optional 1 (Enumeration RpcKind)
    , reqOp         :: Optional 2 (Enumeration RpcOperation)
    , reqCallId     :: Required 3 (Value (Signed Int32)) -- ^ Sequence number that is sent back in response
    , reqClientId   :: Required 4 (Value ByteString)     -- ^ Globally unique client ID
    , reqRetryCount :: Optional 5 (Value (Signed Int32)) -- ^ Retry count, 1 means this is the first retry
    } deriving (Generic, Show)

instance Encode RpcRequestHeader
instance Decode RpcRequestHeader

-- | This message is the header for the Protobuf Rpc Engine
-- when sending a RPC request from  RPC client to the RPC server.
-- The actual request (serialized as protobuf) follows this request.
--
-- No special header is needed for the Rpc Response for Protobuf Rpc Engine.
-- The normal RPC response header (see RpcHeader.proto) are sufficient.
data RequestHeader = RequestHeader {
  -- | Name of the RPC method
    reqMethodName :: Required 1 (Value Text)

  -- | RPCs for a particular interface (ie protocol) are done using an IPC
  -- connection that is setup using rpcProxy.  The rpcProxy has a declared
  -- protocol name that is sent from client to server at connection time.
  --
  -- Each Rpc call also sends a protocol name (reqProtocolName). This name is
  -- usually the same as the connection protocol name, but not always. For example,
  -- meta protocols, such as ProtocolInfoProto, reuse the connection but need to
  -- indicate that the actual protocol is different (i.e. the protocol is
  -- ProtocolInfoProto) since they reuse the connection; in this case the
  -- protocol name is set to ProtocolInfoProto.
  , reqProtocolName :: Required 2 (Value Text)

  -- | Protocol version of class declaring the called method.
  , reqProtocolVersion :: Required 3 (Value Word64)
} deriving (Generic, Show)

instance Encode RequestHeader
instance Decode RequestHeader

------------------------------------------------------------------------

-- | Success or failure. The reponse header's error detail, exception
-- class name and error message contains further details on the error.
data RpcStatus = Success -- ^ Succeeded
               | Error   -- ^ Non-fatal error, connection left open
               | Fatal   -- ^ Fatal error, connection closed
  deriving (Generic, Show, Eq, Enum)

-- | Note that RPC response header is also used when connection setup fails.
-- (i.e. the response looks like an RPC response with a fake callId)
--
--  In case of Fatal error then the respose contains the Serverside's IPC version.

data RpcResponseHeader = RpcResponseHeader
    { rspCallId             :: Required 1 (Value Word32)          -- ^ Call ID used in request
    , rspStatus             :: Required 2 (Enumeration RpcStatus)
    , rspServerIpcVersion   :: Optional 3 (Value Word32)          -- ^ v7: Sent if fatal v9: Sent if success or fail
    , rspExceptionClassName :: Optional 4 (Value Text)            -- ^ If the request fails
    , rspErrorMsg           :: Optional 5 (Value Text)            -- ^ If the request fails, often contains stack trace
    , rspErrorDetail        :: Optional 6 (Enumeration Error)     -- ^ In case of error
    , rspClientId           :: Optional 7 (Value ByteString)      -- ^ Globally unique client ID
    , rspRetryCount         :: Optional 8 (Value Int32)
    } deriving (Generic, Show)

instance Encode RpcResponseHeader
instance Decode RpcResponseHeader

-- | Describes why an RPC error occurred.
data Error = ErrorApplication         -- ^ RPC failed - RPC app threw exception
           | ErrorNoSuchMethod        -- ^ RPC error - no such method
           | ErrorNoSuchProtocol      -- ^ RPC error - no such protocol
           | ErrorRpcServer           -- ^ RPC error on server side
           | ErrorSerializingResponse -- ^ Error serializing response
           | ErrorRpcVersionMismatch  -- ^ RPC protocol version mismatch
           | ErrorCode Int            -- ^ RPC error that we don't know about

             -- starts at 10
           | FatalUnknown                  -- ^ Unknown fatal error
           | FatalUnsupportedSerialization -- ^ IPC layer serilization type invalid
           | FatalInvalidRpcHeader         -- ^ Fields of RPC header are invalid
           | FatalDeserializingRequest     -- ^ Could not deserialize RPC request
           | FatalVersionMismatch          -- ^ IPC layer version mismatch
           | FatalUnauthorized             -- ^ Auth failed
           | FatalCode Int                 -- ^ Fatal error that we don't know about
  deriving (Generic, Show)

instance Enum Error where
    toEnum n = case n of
      1  -> ErrorApplication
      2  -> ErrorNoSuchMethod
      3  -> ErrorNoSuchProtocol
      4  -> ErrorRpcServer
      5  -> ErrorSerializingResponse
      6  -> ErrorRpcVersionMismatch
      10 -> FatalUnknown
      11 -> FatalUnsupportedSerialization
      12 -> FatalInvalidRpcHeader
      13 -> FatalDeserializingRequest
      14 -> FatalVersionMismatch
      15 -> FatalUnauthorized
      _  -> if n < 10 then ErrorCode n else FatalCode n

    fromEnum e = case e of
      ErrorApplication              -> 1
      ErrorNoSuchMethod             -> 2
      ErrorNoSuchProtocol           -> 3
      ErrorRpcServer                -> 4
      ErrorSerializingResponse      -> 5
      ErrorRpcVersionMismatch       -> 6
      ErrorCode n                   -> n
      FatalUnknown                  -> 10
      FatalUnsupportedSerialization -> 11
      FatalInvalidRpcHeader         -> 12
      FatalDeserializingRequest     -> 13
      FatalVersionMismatch          -> 14
      FatalUnauthorized             -> 15
      FatalCode n                   -> n
