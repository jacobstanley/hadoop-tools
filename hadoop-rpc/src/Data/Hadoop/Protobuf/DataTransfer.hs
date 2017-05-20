{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}

-- | This module contains protocol buffers that are used to transfer data
-- to and from the datanode, as well as between datanodes.
module Data.Hadoop.Protobuf.DataTransfer where

import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
import Data.ProtocolBuffers
import Data.ProtocolBuffers.Orphans ()
import Data.Text (Text)
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)

import Data.Hadoop.Protobuf.Hdfs
import Data.Hadoop.Protobuf.Security

------------------------------------------------------------------------

data DataTransferEncryptorStatus =
    DTES_SUCCESS | DTES_ERROR_UNKNOWN_KEY | DTES_ERROR
    deriving (Generic, Show, Eq)

instance Enum DataTransferEncryptorStatus where
    toEnum n = case n of
      0 -> DTES_SUCCESS
      1 -> DTES_ERROR_UNKNOWN_KEY
      2 -> DTES_ERROR
      _ -> error $ "DataTransferEncryptorStatus.toEnum: invalid value <" ++ show n ++ ">"

    fromEnum e = case e of
      DTES_SUCCESS           -> 0
      DTES_ERROR_UNKNOWN_KEY -> 1
      DTES_ERROR             -> 2

instance Encode DataTransferEncryptorStatus
instance Decode DataTransferEncryptorStatus

data DataTransferEncryptorMessage = DataTransferEncryptorMessage
    { dtesStatus :: Required 1 (Enumeration DataTransferEncryptorStatus)
    , dtesPayload :: Optional 2 (Value ByteString)
    , dtesData    :: Optional 3 (Value Text)
    } deriving (Generic, Show)

instance Encode DataTransferEncryptorMessage
instance Decode DataTransferEncryptorMessage

data BaseHeader = BaseHeader
    { bhBlock :: Required 1 (Message ExtendedBlock)
    , bhToken :: Optional 2 (Message Token)
    } deriving (Generic, Show)

instance Encode BaseHeader
instance Decode BaseHeader

data ClientOperationHeader = ClientOperationHeader
    { cohBaseHeader :: Required 1 (Message BaseHeader)
    , cohClientName :: Required 2 (Value Text)
    } deriving (Generic, Show)

instance Encode ClientOperationHeader
instance Decode ClientOperationHeader

data CachingStrategy = CachingStrategy
    { csDropBehind :: Optional 1 (Value Bool)
    , csReadahead  :: Optional 2 (Value Int64)
    } deriving (Generic, Show)

instance Encode CachingStrategy
instance Decode CachingStrategy

data OpReadBlock = OpReadBlock
    { orbHeader          :: Required 1 (Message ClientOperationHeader)
    , orbOffset          :: Required 2 (Value Word64)
    , orbLen             :: Required 3 (Value Word64)
    , orbSendChecksums   :: Optional 4 (Value Bool)
    , orbCachingStrategy :: Optional 5 (Message CachingStrategy)
    } deriving (Generic, Show)

instance Encode OpReadBlock
instance Decode OpReadBlock

data Checksum = Checksum
    { csType             :: Required 1 (Message ChecksumType)
    , csBytesPerChecksum :: Required 2 (Value Word32)
    } deriving (Generic, Show)

instance Encode Checksum
instance Decode Checksum


data BlockConstructionStage =
      BCS_PIPELINE_SETUP_APPEND
    -- | pipeline set up for failed PIPELINE_SETUP_APPEND recovery
    | BCS_PIPELINE_SETUP_APPEND_RECOVERY
    -- | data streaming
    | BCS_DATA_STREAMING
    -- | pipeline setup for failed data streaming recovery
    | BCS_PIPELINE_SETUP_STREAMING_RECOVERY
    -- | close the block and pipeline
    | BCS_PIPELINE_CLOSE
    -- | Recover a failed PIPELINE_CLOSE
    | BCS_PIPELINE_CLOSE_RECOVERY
    -- | pipeline set up for block creation
    | BCS_PIPELINE_SETUP_CREATE
    -- | transfer RBW for adding datanodes
    | BCS_TRANSFER_RBW
    -- | transfer Finalized for adding datanodes
    | BCS_TRANSFER_FINALIZED
    deriving (Generic, Show, Eq)

instance Enum BlockConstructionStage where
    toEnum n = case n of
      0 -> BCS_PIPELINE_SETUP_APPEND
      1 -> BCS_PIPELINE_SETUP_APPEND_RECOVERY
      2 -> BCS_DATA_STREAMING
      3 -> BCS_PIPELINE_SETUP_STREAMING_RECOVERY
      4 -> BCS_PIPELINE_CLOSE
      5 -> BCS_PIPELINE_CLOSE_RECOVERY
      6 -> BCS_PIPELINE_SETUP_CREATE
      7 -> BCS_TRANSFER_RBW
      8 -> BCS_TRANSFER_FINALIZED
      _ -> error $ "BlockConstructionStage.toEnum: invalid enum value <" ++ show n ++ ">"

    fromEnum e = case e of
      BCS_PIPELINE_SETUP_APPEND             -> 0
      BCS_PIPELINE_SETUP_APPEND_RECOVERY    -> 1
      BCS_DATA_STREAMING                    -> 2
      BCS_PIPELINE_SETUP_STREAMING_RECOVERY -> 3
      BCS_PIPELINE_CLOSE                    -> 4
      BCS_PIPELINE_CLOSE_RECOVERY           -> 5
      BCS_PIPELINE_SETUP_CREATE             -> 6
      BCS_TRANSFER_RBW                      -> 7
      BCS_TRANSFER_FINALIZED                -> 8

instance Encode BlockConstructionStage
instance Decode BlockConstructionStage

data OpWriteBlock = OpWriteBlock
    { owbHeader    :: Required 1 (Message ClientOperationHeader)
    , owbTargets   :: Repeated 2 (Message DataNodeInfo)
    , owbSource    :: Optional 3 (Message DataNodeInfo)
    , owbStage     :: Required 4 (Enumeration BlockConstructionStage)
    , owbPipelineSize :: Required 5 (Value Word32)
    , owbMinBytesRcvd :: Required 6 (Value Word64)
    , owbMaxBytesRcvd :: Required 7 (Value Word64)
    , owbLatestGenerationStamp :: Required 8 (Value Word64)

    -- The requested checksum mechanism for this block write.
    , owbRequestedChecksum :: Required 9 (Message Checksum)
    , owbCachingStrategy   :: Optional 10 (Message CachingStrategy)
    } deriving (Generic, Show)

instance Encode OpWriteBlock
instance Decode OpWriteBlock

data OpTransferBlock = OpTransferBlock
    { otbHeader  :: Required 1 (Message ClientOperationHeader)
    , otbTargets :: Repeated 2 (Message DataNodeInfo)
    } deriving (Generic, Show)

instance Encode OpTransferBlock
instance Decode OpTransferBlock

data OpReplaceBlock = OpReplaceBlock
    { orpHeader  :: Required 1 (Message BaseHeader)
    , orpDelHint :: Required 2 (Value Text)
    , orpSource  :: Required 3 (Message DataNodeInfo)
    } deriving (Generic, Show)

instance Encode OpReplaceBlock
instance Decode OpReplaceBlock

data OpCopyBlock = OpCopyBlock
    { ocbHeader :: Required 1 (Message BaseHeader)
    } deriving (Generic, Show)

instance Encode OpCopyBlock
instance Decode OpCopyBlock

data OpBlockChecksum = OpBlockChecksum
    { obcHeader :: Required 1 (Message BaseHeader)
    } deriving (Generic, Show)

instance Encode OpBlockChecksum
instance Decode OpBlockChecksum

data OpRequestShortCircuitAccess = OpRequestShortCircuitAccess
    { orscHeader :: Required 1 (Message BaseHeader)

      -- | In order to get short-circuit access to block data, clients must
      -- set this to the highest version of the block data that they can
      -- understand. Currently 1 is the only version, but more versions
      -- may exist in the future if the on-disk format changes.
    , orscMaxVersion :: Required 2 (Value Word32)
    } deriving (Generic, Show)

instance Encode OpRequestShortCircuitAccess
instance Decode OpRequestShortCircuitAccess

-- All fields must be fixed-length!
data PacketHeader = PacketHeader
    { phOffsetInBlock     :: Required 1 (Value (Fixed Int64))
    , phSeqno             :: Required 2 (Value (Fixed Int64))
    , phLastPacketInBlock :: Required 3 (Value Bool)
    , phDataLen           :: Required 4 (Value (Fixed Int32))
    , phSyncBlock         :: Optional 5 (Value Bool) -- ^ default = false
    } deriving (Generic, Show)

instance Encode PacketHeader
instance Decode PacketHeader

data DataTransferStatus =
      DT_SUCCESS
    | DT_ERROR
    | DT_ERROR_CHECKSUM
    | DT_ERROR_INVALID
    | DT_ERROR_EXISTS
    | DT_ERROR_ACCESS_TOKEN
    | DT_CHECKSUM_OK
    | DT_ERROR_UNSUPPORTED
    deriving (Generic, Show, Eq)

instance Enum DataTransferStatus where
    toEnum n = case n of
      0 -> DT_SUCCESS
      1 -> DT_ERROR
      2 -> DT_ERROR_CHECKSUM
      3 -> DT_ERROR_INVALID
      4 -> DT_ERROR_EXISTS
      5 -> DT_ERROR_ACCESS_TOKEN
      6 -> DT_CHECKSUM_OK
      7 -> DT_ERROR_UNSUPPORTED
      _ -> error $ "DataTransferStatus.toEnum: invalid enum value <" ++ show n ++ ">"

    fromEnum e = case e of
      DT_SUCCESS            -> 0
      DT_ERROR              -> 1
      DT_ERROR_CHECKSUM     -> 2
      DT_ERROR_INVALID      -> 3
      DT_ERROR_EXISTS       -> 4
      DT_ERROR_ACCESS_TOKEN -> 5
      DT_CHECKSUM_OK        -> 6
      DT_ERROR_UNSUPPORTED  -> 7

data PipelineAck = PipelineAck
    { psSeqno  :: Required 1 (Value (Signed Int64))
    -- TODO paStatus should be a repeated field but there is a bug in protobuf
    -- TODO with repeated enumeration's
    , paStatus :: Optional 2 (Enumeration DataTransferStatus)
    , paDownstreamAckTimeNanos :: Optional 3 (Value Word64) -- ^ default = 0
    } deriving (Generic, Show)

instance Encode PipelineAck
instance Decode PipelineAck

-- | Sent as part of the BlockOpResponse
-- for READ_BLOCK and COPY_BLOCK operations.
data ReadOpChecksumInfo = ReadOpChecksumInfo
    { rociChecksum :: Required 1 (Message Checksum)

    -- | The offset into the block at which the first packet
    -- will start. This is necessary since reads will align
    -- backwards to a checksum chunk boundary.
    , rociChunkOffset :: Required 2 (Value Word64)
    } deriving (Generic, Show)

instance Encode ReadOpChecksumInfo
instance Decode ReadOpChecksumInfo

data BlockOpResponse = BlockOpResponse
    { borStatus           :: Required 1 (Enumeration DataTransferStatus)
    , borFirstBadLink     :: Optional 2 (Value String)
    , borChecksumResponse :: Optional 3 (Message OpBlockChecksumResponse)
    , borReadOpChecksumInfo :: Optional 4 (Message ReadOpChecksumInfo)

    -- | Explanatory text which may be useful to log on the client side
    , borData :: Optional 5 (Value Text)

    -- | If the server chooses to agree to the request of a client for
    -- short-circuit access, it will send a response data with the relevant
    -- file descriptors attached.
    --
    -- In the body of the data, this version number will be set to the
    -- specific version number of the block data that the client is about to
    -- read.
    , borShortCircuitAccessVersion :: Optional 6 (Value Word32)
    } deriving (Generic, Show)

instance Encode BlockOpResponse
instance Decode BlockOpResponse

-- | Message sent from the client to the DN after reading the entire
-- read request.
data ClientReadStatus = ClientReadStatus
    { crsStatus :: Required 1 (Enumeration DataTransferStatus)
    } deriving (Generic, Show)

instance Encode ClientReadStatus
instance Decode ClientReadStatus

data DNTransferAck = DNTransferAck
    { dntaStatus :: Required 1 (Enumeration DataTransferStatus)
    } deriving (Generic, Show)

instance Encode DNTransferAck
instance Decode DNTransferAck

data OpBlockChecksumResponse = OpBlockChecksumResponse
    { obcrBytesPerCrc :: Required 1 (Value Word32)
    , obcrCrcPerBlock :: Required 2 (Value Word64)
    , obcrMd5         :: Required 3 (Value ByteString)
    , obcrCrcType     :: Optional 4 (Message ChecksumType)
    } deriving (Generic, Show)

instance Encode OpBlockChecksumResponse
instance Decode OpBlockChecksumResponse
