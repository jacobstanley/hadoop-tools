{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}

module Hadoop.Messages.Headers where

import Data.Word (Word32, Word64)

import Data.ByteString (ByteString)
import Data.ProtocolBuffers
import Data.ProtocolBuffers.Orphans ()
import Data.Text (Text)
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
    , reqCallId     :: Required 3 (Value Word32)     -- ^ A sequence number that is sent back in the response

    -- | Fields below don't apply until v9
    --, reqClientId   :: Required 4 (Value ByteString) -- ^ Globally unique client ID
    --, reqRetryCount :: Optional 5 (Value Int32)      -- ^ Retry count, 1 means this is the first retry
    } deriving (Generic, Show)

instance Encode RpcRequestHeader
instance Decode RpcRequestHeader

-- | This message is used for Protobuf RPC Engine.
-- The message is used to marshal a RPC request from RPC client to the
-- RPC server. The response to the RPC call (including errors) are handled
-- as part of the standard RPC response.
data RpcRequest = RpcRequest
    { reqMethodName      :: Required 1 (Value Text)       -- ^ Name of the RPC method
    , reqBytes           :: Optional 2 (Value ByteString) -- ^ Bytes corresponding to the client protobuf request
    , reqProtocolName    :: Required 3 (Value Text)       -- ^ Protocol name of class declaring the called method
    , reqProtocolVersion :: Required 4 (Value Word64)     -- ^ Protocol version of class declaring the called method
    } deriving (Generic, Show)

instance Encode RpcRequest
instance Decode RpcRequest

------------------------------------------------------------------------

-- | Success or failure. The reponse header's error detail, exception
-- class name and error message contains further details on the error.
data RpcStatus = RpcSuccess -- ^ Succeeded
               | RpcError   -- ^ Non-fatal error, connection left open
               | RpcFatal   -- ^ Fatal error, connection closed
  deriving (Generic, Show, Eq, Enum)

-- | Note that RPC response header is also used when connection setup fails.
-- (i.e. the response looks like an RPC response with a fake callId)
--
-- For v7:
--  - If successfull then the Respose follows after this header
--      - length (4 byte int), followed by the response
--  - If error or fatal - the exception info follow
--      - length (4 byte int) Class name of exception - UTF-8 string
--      - length (4 byte int) Stacktrace - UTF-8 string
--      - if the strings are null then the length is -1
--
--  In case of Fatal error then the respose contains the Serverside's IPC version.

data RpcResponseHeader = RpcResponseHeader
    { rspCallId             :: Required 1 (Value Word32)          -- ^ Call ID used in request
    , rspStatus             :: Required 2 (Enumeration RpcStatus)
    , rspServerIpcVersion   :: Optional 3 (Value Word32)          -- ^ v7: Sent if fatal v9: Sent if success or fail

    -- | Fields below don't apply until v9
    --, rspExceptionClassName :: Optional 4 (Value Text)            -- ^ If the request fails
    --, rspErrorMsg           :: Optional 5 (Value Text)            -- ^ If the request fails, often contains stack trace
    --, rspErrorDetail        :: Optional 6 (Enumeration RpcError)  -- ^ In case of error
    --, rspClientId           :: Optional 7 (Value ByteString)      -- ^ Globally unique client ID
    --, rspRetryCount         :: Optional 8 (Value Int32)
    } deriving (Generic, Show)

instance Encode RpcResponseHeader
instance Decode RpcResponseHeader

{-
-- RpcError doesn't apply until v9

-- | Describes why an RPC error occurred.
data RpcError = ErrorApplication         -- ^ RPC failed - RPC app threw exception
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

instance Enum RpcError where
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
-}
