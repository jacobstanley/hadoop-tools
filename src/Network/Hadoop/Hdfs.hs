{-# LANGUAGE OverloadedStrings #-}

module Network.Hadoop.Hdfs
    ( Hdfs
    , hdfs
    , runHdfs
    , runHdfs'

    , CreateParent
    , Recursive

    , getListing
    , getFileInfo
    , getContentSummary
    , mkdirs
    , delete
    , rename
    ) where

import           Control.Applicative ((<$>))
import           Control.Exception (throwIO)
import qualified Data.Text as T

import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()

import           Data.Hadoop.Configuration
import           Data.Hadoop.Protobuf.ClientNameNode
import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Types
import           Network.Hadoop.Rpc
import qualified Network.Hadoop.Socket as S

------------------------------------------------------------------------

type Hdfs a = Connection -> IO a

type CreateParent = Bool
type Recursive    = Bool
type Overwrite    = Bool

------------------------------------------------------------------------

hdfs :: Protocol
hdfs = Protocol "org.apache.hadoop.hdfs.protocol.ClientProtocol" 1

runHdfs :: Hdfs a -> IO a
runHdfs remote = do
    -- TODO This throws if it can't find the config xml files, maybe this is good?
    nns <- getNameNodes
    case nns of
        []     -> throwIO (RemoteError "ConfigError" "Could not find name node configuration")
        (nn:_) -> runHdfs' nn remote

runHdfs' :: Endpoint -> Hdfs a -> IO a
runHdfs' nameNode remote = do
    user <- getUser

    let app (sock, _) = do
          conn <- initConnectionV7 user hdfs sock
          remote conn

    S.runTcp nameNode app

------------------------------------------------------------------------

getListing :: FilePath -> Hdfs (Maybe DirectoryListing)
getListing path c = get lsDirList <$> invoke c "getListing" GetListingRequest
    { lsSrc          = putField (T.pack path)
    , lsStartAfter   = putField ""
    , lsNeedLocation = putField False
    }

getFileInfo :: FilePath -> Hdfs (Maybe FileStatus)
getFileInfo path c = get fiFileStatus <$> invoke c "getFileInfo" GetFileInfoRequest
    { fiSrc = putField (T.pack path)
    }

getContentSummary :: FilePath -> Hdfs ContentSummary
getContentSummary path c = get csSummary <$> invoke c "getContentSummary" GetContentSummaryRequest
    { csPath = putField (T.pack path)
    }

mkdirs :: FilePath -> CreateParent -> Hdfs Bool
mkdirs path createParent c = get mdResult <$> invoke c "mkdirs" MkdirsRequest
    { mdSrc          = putField (T.pack path)
    , mdMasked       = putField (FilePermission (putField 0o755))
    , mdCreateParent = putField createParent
    }

delete :: FilePath -> Recursive -> Hdfs Bool
delete path recursive c = get dlResult <$> invoke c "delete" DeleteRequest
    { dlSrc       = putField (T.pack path)
    , dlRecursive = putField recursive
    }

rename :: FilePath -> FilePath -> Overwrite -> Hdfs ()
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
