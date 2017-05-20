{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}

module Data.Hadoop.Protobuf.Hdfs where

import Data.ByteString (ByteString)
import Data.ProtocolBuffers
import Data.ProtocolBuffers.Orphans ()
import Data.Text (Text)
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)

import Data.Hadoop.Protobuf.Security

------------------------------------------------------------------------

-- | Summary of a file or directory
data ContentSummary = ContentSummary
    { csLength         :: Required 1 (Value Word64)
    , csFileCount      :: Required 2 (Value Word64)
    , csDirectoryCount :: Required 3 (Value Word64)
    , csQuota          :: Required 4 (Value Word64)
    , csSpaceConsumed  :: Required 5 (Value Word64)
    , csSpaceQuota     :: Required 6 (Value Word64)
    } deriving (Generic, Show)

instance Encode ContentSummary
instance Decode ContentSummary

------------------------------------------------------------------------

-- | Directory listing.
data DirectoryListing = DirectoryListing
    { dlPartialListing :: Repeated 1 (Message FileStatus)
    , dlRemaingEntries :: Required 2 (Value Word32)
    } deriving (Generic, Show)

instance Encode DirectoryListing
instance Decode DirectoryListing

------------------------------------------------------------------------

-- | Status of a file, directory or symbolic link. Optionally includes a
-- files block locations if requested by client on the RPC call.
data FileStatus = FileStatus
    { fsFileType         :: Required  1 (Enumeration FileType)
    , fsPath             :: Required  2 (Value ByteString) -- ^ local name of inode (encoded java utf8)
    , fsLength           :: Required  3 (Value Word64)
    , fsPermission       :: Required  4 (Message FilePermission)
    , fsOwner            :: Required  5 (Value Text)
    , fsGroup            :: Required  6 (Value Text)
    , fsModificationTime :: Required  7 (Value Word64)
    , fsAccessTime       :: Required  8 (Value Word64)

    -- Optional fields for symlink
    , fsSymLink          :: Optional  9 (Value ByteString) -- ^ if symlink, target (encoded java utf8)

    -- Optional fields for file
    , fsBlockReplication :: Optional 10 (Value Word32) -- ^ default = 0, only 16bits used
    , fsBlockSize        :: Optional 11 (Value Word64) -- ^ default = 0
    , fsLocations        :: Optional 12 (Message LocatedBlocks) -- ^ supplied only if asked by client
    } deriving (Generic, Show)

instance Encode FileStatus
instance Decode FileStatus

------------------------------------------------------------------------

-- | The type of a file (either directory, file or symbolic link)
data FileType = IS_DIR | IS_FILE | IS_SYMLINK
    deriving (Generic, Show, Eq)

instance Enum FileType where
    toEnum n = case n of
      1 -> IS_DIR
      2 -> IS_FILE
      3 -> IS_SYMLINK
      _ -> error $ "FileType.toEnum: invalid enum value <" ++ show n ++ ">"

    fromEnum e = case e of
      IS_DIR     -> 1
      IS_FILE    -> 2
      IS_SYMLINK -> 3

------------------------------------------------------------------------

data ChecksumType = CHECKSUM_NULL | CHECKSUM_CRC32 | CHECKSUM_CRC32C
    deriving (Generic, Show, Eq)

instance Enum ChecksumType where
    toEnum n = case n of
      0 -> CHECKSUM_NULL
      1 -> CHECKSUM_CRC32
      2 -> CHECKSUM_CRC32C
      _ -> error $ "ChecksumType.toEnum: invalid enum value <" ++ show n ++ ">"

    fromEnum e = case e of
      CHECKSUM_NULL   -> 0
      CHECKSUM_CRC32  -> 1
      CHECKSUM_CRC32C -> 2

instance Encode ChecksumType
instance Decode ChecksumType

------------------------------------------------------------------------

-- | File or directory permission, same spec as POSIX.
data FilePermission = FilePermission
    { fpPerm :: Required 1 (Value Word32) -- ^ actually a short, only 16 bits used
    } deriving (Generic, Show)

instance Encode FilePermission
instance Decode FilePermission

------------------------------------------------------------------------

-- | A set of file blocks and their locations.
data LocatedBlocks = LocatedBlocks
    { lbFileLength        :: Required 1 (Value Word64)
    , lbBlocks            :: Repeated 2 (Message LocatedBlock)
    , lbUnderConstruction :: Required 3 (Value Bool)
    , lbLastBlock         :: Optional 4 (Message LocatedBlock)
    , lbLastBlockComplete :: Required 5 (Value Bool)
    } deriving (Generic, Show)

instance Encode LocatedBlocks
instance Decode LocatedBlocks

------------------------------------------------------------------------

-- | Information about a block and its location.
data LocatedBlock = LocatedBlock
    { lbExtended  :: Required 1 (Message ExtendedBlock)
    , lbOffset    :: Required 2 (Value Word64)         -- ^ offset of first byte of block in the file
    , lbLocations :: Repeated 3 (Message DataNodeInfo) -- ^ locations ordered by proximity to client IP

    -- | `True` if all replicas of a block are corrupt. If the block has a few corrupt replicas,
    -- they are filtered and their locations are not part of this object.
    , lbCorrupt   :: Required 4 (Value Bool)
    , lbToken     :: Required 5 (Message Token)
    } deriving (Generic, Show)

instance Encode LocatedBlock
instance Decode LocatedBlock

------------------------------------------------------------------------

-- | Identifies a block.
data ExtendedBlock = ExtendedBlock
    { ebPoolId          :: Required 1 (Value Text)   -- ^ block pool id - globally unique across clusters
    , ebBlockId         :: Required 2 (Value Word64) -- ^ the local id within a pool
    , ebGenerationStamp :: Required 3 (Value Word64)
    , ebNumBytes        :: Optional 4 (Value Word64) -- ^ does not belong, here for historical reasons
    } deriving (Generic, Show)

instance Encode ExtendedBlock
instance Decode ExtendedBlock

------------------------------------------------------------------------

-- | Status of a data node.
data DataNodeInfo = DataNodeInfo
    { dnId            :: Required 1 (Message DataNodeId)
    , dnCapacity      :: Optional 2 (Value Word64) -- ^ default = 0
    , dnDfsUsed       :: Optional 3 (Value Word64) -- ^ default = 0
    , dnRemaining     :: Optional 4 (Value Word64) -- ^ default = 0
    , dnBlockPoolUsed :: Optional 5 (Value Word64) -- ^ default = 0
    , dnLastUpdate    :: Optional 6 (Value Word64) -- ^ default = 0
    , dnXceiverCount  :: Optional 7 (Value Word32) -- ^ default = 0
    , dnLocation      :: Optional 8 (Value Text)
    , dnAdminState    :: Optional 9 (Enumeration AdminState) -- ^ default = NORMAL
    } deriving (Generic, Show)

instance Encode DataNodeInfo
instance Decode DataNodeInfo

------------------------------------------------------------------------

data AdminState = NORMAL | DECOMMISSION_INPROGRESS | DECOMMISSIONED
    deriving (Generic, Show, Eq)

instance Enum AdminState where
    toEnum n = case n of
      0 -> NORMAL
      1 -> DECOMMISSION_INPROGRESS
      2 -> DECOMMISSIONED
      _ -> error $ "AdminState.toEnum: invalid enum value <" ++ show n ++ ">"

    fromEnum e = case e of
      NORMAL                  -> 0
      DECOMMISSION_INPROGRESS -> 1
      DECOMMISSIONED          -> 2

------------------------------------------------------------------------

-- | Identifies a data node
data DataNodeId = DataNodeId
    { dnIpAddr    :: Required 1 (Value Text)   -- ^ IP address
    , dnHostName  :: Required 2 (Value Text)   -- ^ Host name
    , dnStorageId :: Required 3 (Value Text)   -- ^ Storage ID
    , dnXferPort  :: Required 4 (Value Word32) -- ^ Data streaming port
    , dnInfoPort  :: Required 5 (Value Word32) -- ^ Info server port
    , dnIpcPort   :: Required 6 (Value Word32) -- ^ IPC server port
    } deriving (Generic, Show)

instance Encode DataNodeId
instance Decode DataNodeId

------------------------------------------------------------------------

-- BlockTokenId was moved to Security.Token in protocol version 9
