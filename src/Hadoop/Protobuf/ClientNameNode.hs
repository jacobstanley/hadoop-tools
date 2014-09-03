{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}

module Hadoop.Protobuf.ClientNameNode where

import Data.ByteString (ByteString)
import Data.ProtocolBuffers
import Data.Text (Text)
import GHC.Generics (Generic)

import Hadoop.Protobuf.Hdfs

------------------------------------------------------------------------

data GetListingRequest = GetListingRequest
    { glSrc          :: Required 1 (Value Text)       -- ^ the directory to list
    , glStartAfter   :: Required 2 (Value ByteString) -- ^ begin the listing after this file (encoded java utf8)
    , glNeedLocation :: Required 3 (Value Bool)       -- ^ return the location data in the cluster
    } deriving (Generic, Show)

instance Encode GetListingRequest
instance Decode GetListingRequest

data GetListingResponse = GetListingResponse
    { glDirList :: Optional 1 (Message DirectoryListing)
    } deriving (Generic, Show)

instance Encode GetListingResponse
instance Decode GetListingResponse

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
