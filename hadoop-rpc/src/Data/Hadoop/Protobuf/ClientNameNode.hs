{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}

module Data.Hadoop.Protobuf.ClientNameNode where

import Data.ByteString (ByteString)
import Data.ProtocolBuffers
import Data.Text (Text)
import Data.Int (Int64)
import GHC.Generics (Generic)

import Data.Hadoop.Protobuf.Hdfs

------------------------------------------------------------------------

data GetListingRequest = GetListingRequest
    { lsSrc          :: Required 1 (Value Text)       -- ^ the directory to list
    , lsStartAfter   :: Required 2 (Value ByteString) -- ^ begin the listing after this file (encoded java utf8)
    , lsNeedLocation :: Required 3 (Value Bool)       -- ^ return the location data in the cluster
    } deriving (Generic, Show)

instance Encode GetListingRequest
instance Decode GetListingRequest

data GetListingResponse = GetListingResponse
    { lsDirList :: Optional 1 (Message DirectoryListing)
    } deriving (Generic, Show)

instance Encode GetListingResponse
instance Decode GetListingResponse

------------------------------------------------------------------------

data GetFileInfoRequest = GetFileInfoRequest
    { fiSrc :: Required 1 (Value Text)
    } deriving (Generic, Show)

instance Encode GetFileInfoRequest
instance Decode GetFileInfoRequest

data GetFileInfoResponse = GetFileInfoResponse
    { fiFileStatus :: Optional 1 (Message FileStatus)
    } deriving (Generic, Show)

instance Encode GetFileInfoResponse
instance Decode GetFileInfoResponse

------------------------------------------------------------------------

data GetContentSummaryRequest = GetContentSummaryRequest
    { csPath :: Required 1 (Value Text)
    } deriving (Generic, Show)

instance Encode GetContentSummaryRequest
instance Decode GetContentSummaryRequest

data GetContentSummaryResponse = GetContentSummaryResponse
    { csSummary :: Required 1 (Message ContentSummary)
    } deriving (Generic, Show)

instance Encode GetContentSummaryResponse
instance Decode GetContentSummaryResponse

------------------------------------------------------------------------

data MkdirsRequest = MkdirsRequest
    { mdSrc          :: Required 1 (Value Text)
    , mdMasked       :: Required 2 (Message FilePermission)
    , mdCreateParent :: Required 3 (Value Bool)
    } deriving (Generic, Show)

instance Encode MkdirsRequest
instance Decode MkdirsRequest

data MkdirsResponse = MkdirsResponse
    { mdResult :: Required 1 (Value Bool)
    } deriving (Generic, Show)

instance Encode MkdirsResponse
instance Decode MkdirsResponse

------------------------------------------------------------------------

data DeleteRequest = DeleteRequest
    { dlSrc       :: Required 1 (Value Text)
    , dlRecursive :: Required 2 (Value Bool)
    } deriving (Generic, Show)

instance Encode DeleteRequest
instance Decode DeleteRequest

data DeleteResponse = DeleteResponse
    { dlResult :: Required 1 (Value Bool)
    } deriving (Generic, Show)

instance Encode DeleteResponse
instance Decode DeleteResponse

------------------------------------------------------------------------

data Rename2Request = Rename2Request
    { mvSrc       :: Required 1 (Value Text)
    , mvDst       :: Required 2 (Value Text)
    , mvOverwrite :: Required 3 (Value Bool)
    } deriving (Generic, Show)

instance Encode Rename2Request
instance Decode Rename2Request

data Rename2Response = Rename2Response
    {
    } deriving (Generic, Show)

instance Encode Rename2Response
instance Decode Rename2Response

------------------------------------------------------------------------

data SetPermissionRequest = SetPermissionRequest
    { chmodPath       :: Required 1 (Value Text)
    , chmodMode       :: Required 2 (Message FilePermission)
    } deriving (Generic, Show)

instance Encode SetPermissionRequest
instance Decode SetPermissionRequest

data SetPermissionResponse = SetPermissionResponse
    {
    } deriving (Generic, Show)

instance Encode SetPermissionResponse
instance Decode SetPermissionResponse

------------------------------------------------------------------------

data SetOwnerRequest = SetOwnerRequest
    { chownPath  :: Required 1 (Value Text)
    , chownUser  :: Optional 2 (Value Text)
    , chownGroup :: Optional 3 (Value Text)
    } deriving (Generic, Show)

instance Encode SetOwnerRequest
instance Decode SetOwnerRequest

data SetOwnerResponse = SetOwnerResponse
    {
    } deriving (Generic, Show)

instance Encode SetOwnerResponse
instance Decode SetOwnerResponse

------------------------------------------------------------------------

data GetBlockLocationsRequest = GetBlockLocationsRequest
    { catSrc    :: Required 1 (Value Text)
    , catOffset :: Required 2 (Value Int64)
    , catLength :: Required 3 (Value Int64)
    } deriving (Generic, Show)

instance Encode GetBlockLocationsRequest
instance Decode GetBlockLocationsRequest

data GetBlockLocationsResponse = GetBlockLocationsResponse
    { locsLocations :: Optional 1 (Message LocatedBlocks)
    } deriving (Generic, Show)

instance Encode GetBlockLocationsResponse
instance Decode GetBlockLocationsResponse
