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
import           Data.Text (Text)
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V

import qualified Data.Hadoop.Protobuf.ClientNameNode as P
import qualified Data.Hadoop.Protobuf.Hdfs as P
import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()

import           Data.Hadoop.Configuration
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

getListing :: NeedLocation -> HdfsPath -> Hdfs (Maybe (V.Vector P.FileStatus))
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
    partialListing :: P.DirectoryListing -> V.Vector P.FileStatus
    partialListing = V.fromList . getField . P.dlPartialListing

    hasRemainingEntries :: P.DirectoryListing -> Bool
    hasRemainingEntries = (/= 0) . getField . P.dlRemaingEntries

    lastFileName :: V.Vector P.FileStatus -> ByteString
    lastFileName v | V.null v  = ""
                   | otherwise = getField . P.fsPath . V.last $ v

    loop :: [V.Vector P.FileStatus] -> ByteString -> Hdfs (V.Vector P.FileStatus)
    loop ps startAfter = do
        dirList <- fromMaybe emptyListing <$> getPartialListing needLocation path startAfter

        let p   = V.fromList . getField . P.dlPartialListing $ dirList
            ps' = ps ++ [p]
            sa  = getField . P.fsPath $ V.last p

        if hasRemainingEntries dirList
           then loop ps' sa
           else return (V.concat ps')

    emptyListing = P.DirectoryListing (putField []) (putField 0)

getListing' :: HdfsPath -> Hdfs (V.Vector P.FileStatus)
getListing' path = fromMaybe V.empty <$> getListing False path

------------------------------------------------------------------------

getPartialListing :: NeedLocation -> HdfsPath -> ByteString -> Hdfs (Maybe P.DirectoryListing)
getPartialListing needLocation path startAfter = get P.lsDirList <$> hdfsInvoke "getListing" P.GetListingRequest
    { P.lsSrc          = putField (T.decodeUtf8 path)
    , P.lsStartAfter   = putField startAfter
    , P.lsNeedLocation = putField needLocation
    }

getFileInfo :: HdfsPath -> Hdfs (Maybe P.FileStatus)
getFileInfo path = get P.fiFileStatus <$> hdfsInvoke "getFileInfo" P.GetFileInfoRequest
    { P.fiSrc = putField (T.decodeUtf8 path)
    }

getContentSummary :: HdfsPath -> Hdfs P.ContentSummary
getContentSummary path = get P.csSummary <$> hdfsInvoke "getContentSummary" P.GetContentSummaryRequest
    { P.csPath = putField (T.decodeUtf8 path)
    }

mkdirs :: CreateParent -> HdfsPath -> Hdfs Bool
mkdirs createParent path = get P.mdResult <$> hdfsInvoke "mkdirs" P.MkdirsRequest
    { P.mdSrc          = putField (T.decodeUtf8 path)
    , P.mdMasked       = putField (P.FilePermission (putField 0o755))
    , P.mdCreateParent = putField createParent
    }

delete :: Recursive -> HdfsPath -> Hdfs Bool
delete recursive path = get P.dlResult <$> hdfsInvoke "delete" P.DeleteRequest
    { P.dlSrc       = putField (T.decodeUtf8 path)
    , P.dlRecursive = putField recursive
    }

rename :: Overwrite -> HdfsPath -> HdfsPath -> Hdfs ()
rename overwrite src dst = ignore <$> hdfsInvoke "rename2" P.Rename2Request
    { P.mvSrc       = putField (T.decodeUtf8 src)
    , P.mvDst       = putField (T.decodeUtf8 dst)
    , P.mvOverwrite = putField overwrite
    }
  where
    ignore :: P.Rename2Response -> ()
    ignore = const ()

------------------------------------------------------------------------

get :: HasField a => (t -> a) -> t -> FieldType a
get f x = getField (f x)
