{-# LANGUAGE DeriveDataTypeable #-}

module Data.Hadoop.Types where

import           Control.Exception (Exception)
import           Data.ByteString (ByteString)
import           Data.Data (Data)
import           Data.Text (Text)
import           Data.Typeable (Typeable)
import qualified Data.Vector as V
import           Data.Word (Word16, Word64)

------------------------------------------------------------------------

type NameNode   = Endpoint
type SocksProxy = Endpoint

data Endpoint = Endpoint
    { epHost :: !HostName
    , epPort :: !Port
    } deriving (Eq, Ord, Show)

type HostName = Text
type Port     = Int

------------------------------------------------------------------------

data HadoopConfig = HadoopConfig
    { hcUser      :: !User
    , hcNameNodes :: ![NameNode]
    , hcProxy     :: !(Maybe SocksProxy)
    } deriving (Eq, Ord, Show)

------------------------------------------------------------------------

data ConfigError = ConfigError !Text
    deriving (Show, Eq, Data, Typeable)

instance Exception ConfigError

------------------------------------------------------------------------

data RemoteError = RemoteError !Text !Text
    deriving (Show, Eq, Data, Typeable)

instance Exception RemoteError

------------------------------------------------------------------------

data DecodeError = DecodeError !Text
    deriving (Show, Eq, Data, Typeable)

instance Exception DecodeError

------------------------------------------------------------------------

data ConnectionClosed = ConnectionClosed
    deriving (Show, Eq, Data, Typeable)

instance Exception ConnectionClosed

------------------------------------------------------------------------

type User       = Text
type Group      = Text
type Permission = Word16

-- | Path to a file/directory on HDFS, stored as UTF8.
type HdfsPath = ByteString

-- | Microseconds since 1 January 1970.
type HdfsTime = Word64

-- | The type of a file (either directory, file or symbolic link)
data FileType = Dir | File | SymLink
    deriving (Eq, Ord, Show)

-- | Summary of a file or directory
data ContentSummary = ContentSummary
    { csLength         :: !Word64
    , csFileCount      :: !Word64
    , csDirectoryCount :: !Word64
    , csQuota          :: !Word64
    , csSpaceConsumed  :: !Word64
    , csSpaceQuota     :: !Word64
    } deriving (Eq, Ord, Show)

-- | Partial directory listing.
data PartialListing = PartialListing
    { lsRemaining :: !Int -- ^ number of files left to fetch
    , lsFiles     :: !(V.Vector FileStatus)
    } deriving (Eq, Ord, Show)

-- | Status of a file, directory or symbolic link.
data FileStatus = FileStatus
    { fsFileType         :: !FileType
    , fsPath             :: !HdfsPath
    , fsLength           :: !Word64
    , fsPermission       :: !Permission
    , fsOwner            :: !User
    , fsGroup            :: !Group
    , fsModificationTime :: !HdfsTime
    , fsAccessTime       :: !HdfsTime

    -- Optional fields for symlink
    , fsSymLink          :: !(Maybe HdfsPath) -- ^ if symlink, target

    -- Optional fields for file
    , fsBlockReplication :: !Word16
    , fsBlockSize        :: !Word64
    } deriving (Eq, Ord, Show)
