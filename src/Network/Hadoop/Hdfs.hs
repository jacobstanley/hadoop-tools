{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Hadoop.Hdfs
    ( Hdfs(..)
    , hdfsProtocol
    , getConnection
    , runHdfs
    , runHdfs'

    , CreateParent
    , Recursive
    , Overwrite
    , NeedLocation

    , getListing
    , getListing'
    , getPartialListing
    , getFileInfo
    , getContentSummary
    , mkdirs
    , delete
    , rename
    ) where

import           Control.Applicative (Applicative(..), (<$>))
import           Control.Exception (throw)
import           Control.Monad (ap)
import           Control.Monad.Catch (MonadThrow(..), MonadCatch(..))
import           Control.Monad.IO.Class (MonadIO(..))
import           Data.ByteString (ByteString)
import           Data.Maybe (fromMaybe)
import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import           Data.Text (Text)
import qualified Data.Text.Encoding as T
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

instance MonadThrow Hdfs where
    throwM = liftIO . throwM

instance MonadCatch Hdfs where
    catch m k = Hdfs $ \c -> unHdfs m c `catch` \e -> unHdfs (k e) c

------------------------------------------------------------------------

type CreateParent = Bool
type Recursive    = Bool
type Overwrite    = Bool
type NeedLocation = Bool

------------------------------------------------------------------------

getConnection :: Hdfs Connection
getConnection = Hdfs $ return . id

hdfsProtocol :: Protocol
hdfsProtocol = Protocol "org.apache.hadoop.hdfs.protocol.ClientProtocol" 1

runHdfs :: Hdfs a -> IO a
runHdfs hdfs = do
    config <- getHadoopConfig
    runHdfs' config hdfs

runHdfs' :: HadoopConfig -> Hdfs a -> IO a
runHdfs' config@HadoopConfig{..} hdfs = S.runTcp hcProxy nameNode session
  where
    session socket = do
        conn <- initConnectionV7 config hdfsProtocol socket
        (unHdfs hdfs) conn

    nameNode = case hcNameNodes of
        []    -> throw (ConfigError "Could not find name nodes in Hadoop configuration")
        (x:_) -> x

hdfsInvoke :: (Decode b, Encode a) => Text -> a -> Hdfs b
hdfsInvoke method arg = Hdfs $ \c -> invoke c method arg

------------------------------------------------------------------------

getListing :: NeedLocation -> HdfsPath -> Hdfs (Maybe (V.Vector FileStatus))
getListing needLocation path = do
    mDirList <- getPartialListing needLocation path ""
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
        dirList <- fromMaybe emptyListing <$> getPartialListing needLocation path startAfter

        let p   = V.fromList . getField . dlPartialListing $ dirList
            ps' = ps ++ [p]
            sa  = getField . fsPath $ V.last p

        if hasRemainingEntries dirList
           then loop ps' sa
           else return (V.concat ps')

    emptyListing = DirectoryListing (putField []) (putField 0)

getListing' :: HdfsPath -> Hdfs (V.Vector FileStatus)
getListing' path = fromMaybe V.empty <$> getListing False path

------------------------------------------------------------------------

getPartialListing :: NeedLocation -> HdfsPath -> ByteString -> Hdfs (Maybe DirectoryListing)
getPartialListing needLocation path startAfter = get lsDirList <$> hdfsInvoke "getListing" GetListingRequest
    { lsSrc          = putField (T.decodeUtf8 path)
    , lsStartAfter   = putField startAfter
    , lsNeedLocation = putField needLocation
    }

getFileInfo :: HdfsPath -> Hdfs (Maybe FileStatus)
getFileInfo path = get fiFileStatus <$> hdfsInvoke "getFileInfo" GetFileInfoRequest
    { fiSrc = putField (T.decodeUtf8 path)
    }

getContentSummary :: HdfsPath -> Hdfs ContentSummary
getContentSummary path = get csSummary <$> hdfsInvoke "getContentSummary" GetContentSummaryRequest
    { csPath = putField (T.decodeUtf8 path)
    }

mkdirs :: CreateParent -> HdfsPath -> Hdfs Bool
mkdirs createParent path = get mdResult <$> hdfsInvoke "mkdirs" MkdirsRequest
    { mdSrc          = putField (T.decodeUtf8 path)
    , mdMasked       = putField (FilePermission (putField 0o755))
    , mdCreateParent = putField createParent
    }

delete :: Recursive -> HdfsPath -> Hdfs Bool
delete recursive path = get dlResult <$> hdfsInvoke "delete" DeleteRequest
    { dlSrc       = putField (T.decodeUtf8 path)
    , dlRecursive = putField recursive
    }

rename :: Overwrite -> HdfsPath -> HdfsPath -> Hdfs ()
rename overwrite src dst = ignore <$> hdfsInvoke "rename2" Rename2Request
    { mvSrc       = putField (T.decodeUtf8 src)
    , mvDst       = putField (T.decodeUtf8 dst)
    , mvOverwrite = putField overwrite
    }
  where
    ignore :: Rename2Response -> ()
    ignore = const ()

------------------------------------------------------------------------

get :: HasField a => (t -> a) -> t -> FieldType a
get f x = getField (f x)
