{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Hadoop.Hdfs
    ( Hdfs(..)
    , hdfsProtocol
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

import           Control.Applicative (Applicative(..), (<$>))
import           Control.Exception (throwIO)
import           Control.Monad (ap)
import           Control.Monad.IO.Class (MonadIO(..))
import           Data.ByteString (ByteString)
import           Data.Maybe (fromMaybe)
import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V

import           Data.Hadoop.Configuration
import           Data.Hadoop.Protobuf.ClientNameNode
import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Types
import           Network.Hadoop.Rpc
import qualified Network.Hadoop.Socket as S

------------------------------------------------------------------------

newtype Hdfs a = Hdfs { unHdfs :: Connection -> IO a }

instance Functor Hdfs where
    fmap f m = Hdfs $ \c -> fmap f (unHdfs m c)

instance Applicative Hdfs where
    pure  = return
    (<*>) = ap

instance Monad Hdfs where
    return x = Hdfs $ \_ -> return x
    m >>= k  = Hdfs $ \c -> unHdfs m c >>= \x -> unHdfs (k x) c

instance MonadIO Hdfs where
    liftIO io = Hdfs $ \_ -> io

------------------------------------------------------------------------

type CreateParent = Bool
type Recursive    = Bool
type Overwrite    = Bool

------------------------------------------------------------------------

hdfsProtocol :: Protocol
hdfsProtocol = Protocol "org.apache.hadoop.hdfs.protocol.ClientProtocol" 1

runHdfs :: Hdfs a -> IO a
runHdfs hdfs = do
    -- TODO This throws if it can't find the config xml files, maybe this is good?
    nns <- getNameNodes
    case nns of
        []     -> throwIO (RemoteError "ConfigError" "Could not find name node configuration")
        (nn:_) -> runHdfs' nn hdfs

runHdfs' :: Endpoint -> Hdfs a -> IO a
runHdfs' nameNode hdfs = do
    user <- getUser

    let app (sock, _) = do
          conn <- initConnectionV7 user hdfsProtocol sock
          (unHdfs hdfs) conn

    S.runTcp nameNode app

hdfsInvoke :: (Decode b, Encode a) => Text -> a -> Hdfs b
hdfsInvoke method arg = Hdfs $ \c -> invoke c method arg

------------------------------------------------------------------------

getListing :: FilePath -> Hdfs (Maybe (V.Vector FileStatus))
getListing path = do
    mDirList <- getPartialListing path ""
    case mDirList of
      Nothing -> return Nothing
      Just dirList -> do
        let p = partialListing dirList
        if hasRemainingEntries dirList
           then Just <$> loop [p] (lastFileName p)
           else return (Just p)
  where
    partialListing :: DirectoryListing -> V.Vector FileStatus
    partialListing = V.fromList . getField . dlPartialListing

    hasRemainingEntries :: DirectoryListing -> Bool
    hasRemainingEntries = (/= 0) . getField . dlRemaingEntries

    lastFileName :: V.Vector FileStatus -> ByteString
    lastFileName v | V.null v  = ""
                   | otherwise = getField . fsPath . V.last $ v

    loop :: [V.Vector FileStatus] -> ByteString -> Hdfs (V.Vector FileStatus)
    loop ps startAfter = do
        dirList <- fromMaybe emptyListing <$> getPartialListing path startAfter

        let p   = V.fromList . getField . dlPartialListing $ dirList
            ps' = ps ++ [p]
            sa  = getField . fsPath $ V.last p

        if hasRemainingEntries dirList
           then return (V.concat ps')
           else loop ps' sa

    emptyListing = DirectoryListing (putField []) (putField 0)

------------------------------------------------------------------------

getPartialListing :: FilePath -> ByteString -> Hdfs (Maybe DirectoryListing)
getPartialListing path startAfter = get lsDirList <$> hdfsInvoke "getListing" GetListingRequest
    { lsSrc          = putField (T.pack path)
    , lsStartAfter   = putField startAfter
    , lsNeedLocation = putField True
    }

getFileInfo :: FilePath -> Hdfs (Maybe FileStatus)
getFileInfo path = get fiFileStatus <$> hdfsInvoke "getFileInfo" GetFileInfoRequest
    { fiSrc = putField (T.pack path)
    }

getContentSummary :: FilePath -> Hdfs ContentSummary
getContentSummary path = get csSummary <$> hdfsInvoke "getContentSummary" GetContentSummaryRequest
    { csPath = putField (T.pack path)
    }

mkdirs :: FilePath -> CreateParent -> Hdfs Bool
mkdirs path createParent = get mdResult <$> hdfsInvoke "mkdirs" MkdirsRequest
    { mdSrc          = putField (T.pack path)
    , mdMasked       = putField (FilePermission (putField 0o755))
    , mdCreateParent = putField createParent
    }

delete :: FilePath -> Recursive -> Hdfs Bool
delete path recursive = get dlResult <$> hdfsInvoke "delete" DeleteRequest
    { dlSrc       = putField (T.pack path)
    , dlRecursive = putField recursive
    }

rename :: FilePath -> FilePath -> Overwrite -> Hdfs ()
rename src dst overwrite = ignore <$> hdfsInvoke "rename2" Rename2Request
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
