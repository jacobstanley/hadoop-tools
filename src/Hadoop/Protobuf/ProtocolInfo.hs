{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}

module Hadoop.Protobuf.ProtocolInfo where

import Data.Word (Word32, Word64)

import Data.ProtocolBuffers
import Data.Text (Text)
import GHC.Generics (Generic)

------------------------------------------------------------------------

-- | Request to get protocol versions for all supported rpc kinds.
data GetProtocolVersionsRequest = GetProtocolVersionsRequest
    { pvProtocol :: Required 1 (Value Text) -- ^ Protocol name
    } deriving (Generic, Show)

instance Encode GetProtocolVersionsRequest
instance Decode GetProtocolVersionsRequest

-- | Protocol version with corresponding RPC kind.
data ProtocolVersion = ProtocolVersion
    { pvRpcKind  :: Required 1 (Value Text)   -- ^ RPC kind
    , pvVersions :: Repeated 2 (Value Word64) -- ^ Protocol version corresponding to the rpc kind
    } deriving (Generic, Show)

instance Encode ProtocolVersion
instance Decode ProtocolVersion

-- | Get protocol version response.
data GetProtocolVersionsResponse = GetProtocolVersionsResponse
    { pvProtocolVersions :: Repeated 1 (Message ProtocolVersion)
    } deriving (Generic, Show)

instance Encode GetProtocolVersionsResponse
instance Decode GetProtocolVersionsResponse

------------------------------------------------------------------------

-- | Get protocol signature request
data GetProtocolSignatureRequest = GetProtocolSignatureRequest
    { psProtocol :: Required 1 (Value Text) -- ^ Protocol name
    , psRpcKind  :: Required 2 (Value Text) -- ^ RPC kind
    } deriving (Generic, Show)

instance Encode GetProtocolSignatureRequest
instance Decode GetProtocolSignatureRequest

-- | Get protocol signature response
data GetProtocolSignatureResponse = GetProtocolSignatureResponse
    { psSignatures :: Repeated 1 (Message ProtocolSignature)
    } deriving (Generic, Show)

instance Encode GetProtocolSignatureResponse
instance Decode GetProtocolSignatureResponse

data ProtocolSignature = ProtocolSignature
    { psVersion :: Required 1 (Value Word64)
    , psMethods :: Repeated 2 (Value Word32)
    } deriving (Generic, Show)

instance Encode ProtocolSignature
instance Decode ProtocolSignature

------------------------------------------------------------------------

{-
Example Request
===============

    req = RpcRequest
        { reqMethodName      = putField "getProtocolSignature"
        , reqBytes           = putField $ Just $ toBytes GetProtocolSignatureRequest
            { psProtocol = putField "org.apache.hadoop.hdfs.protocol.ClientProtocol"
            , psRpcKind  = putField "RPC_PROTOCOL_BUFFER"
            }
        , reqProtocolName    = putField "org.apache.hadoop.ipc.ProtocolMetaInfoPB"
        , reqProtocolVersion = putField 1
        }

-}
