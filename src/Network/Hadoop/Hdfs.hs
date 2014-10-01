{-# LANGUAGE OverloadedStrings #-}

module Network.Hadoop.Hdfs
    ( CreateParent
    , Recursive

    , getListing
    , getFileInfo
    , getContentSummary
    , mkdirs
    , delete
    , rename
    ) where

import           Control.Applicative ((<$>))
import qualified Data.Text as T

import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()

import           Data.Hadoop.Protobuf.ClientNameNode
import           Data.Hadoop.Protobuf.Hdfs
import           Network.Hadoop.Rpc

------------------------------------------------------------------------

type CreateParent = Bool
type Recursive = Bool
type Overwrite = Bool

------------------------------------------------------------------------

getListing :: FilePath -> Connection -> IO (Maybe DirectoryListing)
getListing path c = get lsDirList <$> invoke c "getListing" GetListingRequest
    { lsSrc          = putField (T.pack path)
    , lsStartAfter   = putField ""
    , lsNeedLocation = putField False
    }

getFileInfo :: FilePath -> Connection -> IO (Maybe FileStatus)
getFileInfo path c = get fiFileStatus <$> invoke c "getFileInfo" GetFileInfoRequest
    { fiSrc = putField (T.pack path)
    }

getContentSummary :: FilePath -> Connection -> IO ContentSummary
getContentSummary path c = get csSummary <$> invoke c "getContentSummary" GetContentSummaryRequest
    { csPath = putField (T.pack path)
    }

mkdirs :: FilePath -> CreateParent -> Connection -> IO Bool
mkdirs path createParent c = get mdResult <$> invoke c "mkdirs" MkdirsRequest
    { mdSrc          = putField (T.pack path)
    , mdMasked       = putField (FilePermission (putField 0o755))
    , mdCreateParent = putField createParent
    }

delete :: FilePath -> Recursive -> Connection -> IO Bool
delete path recursive c = get dlResult <$> invoke c "delete" DeleteRequest
    { dlSrc       = putField (T.pack path)
    , dlRecursive = putField recursive
    }

rename :: FilePath -> FilePath -> Overwrite -> Connection -> IO ()
rename src dst overwrite c = ignore <$> invoke c "rename2" Rename2Request
    { mvSrc       = putField (T.pack src)
    , mvDst       = putField (T.pack dst)
    , mvOverwrite = putField overwrite
    }
  where
    ignore :: Rename2Response -> ()
    ignore = const ()

------------------------------------------------------------------------

get :: HasField a => (t -> a) -> t -> FieldType a
get f x = getField (f x)
