{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Hadoop.Hdfs
    ( Hdfs(..)
    , hdfsProtocol
    , getConnection
    , runHdfs
    , runHdfs'
    , hdfsInvoke

    , CreateParent
    , Recursive
    , Overwrite

    , getListing
    , getListing'
    , getFileInfo
    , getContentSummary
    , mkdirs
    , delete
    , rename
    , setPermissions
    ) where

import           Control.Applicative (Applicative(..), (<$>))
import           Control.Exception (throw)
import           Control.Monad (ap)
import           Control.Monad.Catch (MonadMask(..), MonadThrow(..), MonadCatch(..))
import           Control.Monad.IO.Class (MonadIO(..))
import           Data.ByteString (ByteString)
import           Data.Maybe (fromMaybe)
import           Data.Text (Text)
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import           Data.Word (Word32)

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
    liftIO io = Hdfs $ const io

instance MonadThrow Hdfs where
    throwM = liftIO . throwM

instance MonadCatch Hdfs where
    catch m k = Hdfs $ \c -> unHdfs m c `catch` \e -> unHdfs (k e) c

instance MonadMask Hdfs where
    mask a = Hdfs $ \e -> mask $ \u -> unHdfs (a $ q u) e
      where q u (Hdfs b) = Hdfs (u . b)
    uninterruptibleMask a =
      Hdfs $ \e -> uninterruptibleMask $ \u -> unHdfs (a $ q u) e
        where q u (Hdfs b) = Hdfs (u . b)

------------------------------------------------------------------------

type CreateParent = Bool
type Recursive    = Bool
type Overwrite    = Bool

------------------------------------------------------------------------

getConnection :: Hdfs Connection
getConnection = Hdfs return

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
        unHdfs hdfs conn

    nameNode = case hcNameNodes of
        []    -> throw (ConfigError "Could not find name nodes in Hadoop configuration")
        (x:_) -> x

hdfsInvoke :: (Encode a, Decode b) => Text -> a -> Hdfs b
hdfsInvoke method arg = Hdfs $ \c -> invoke c method arg

------------------------------------------------------------------------

getListing :: HdfsPath -> Hdfs (Maybe (V.Vector FileStatus))
getListing path = do
    mDirList <- getPartialListing path ""
    case mDirList of
      Nothing                    -> return Nothing
      Just (PartialListing 0 fs) -> return (Just fs)
      Just (PartialListing _ fs) -> Just <$> loop [fs] (lastFileName fs)
  where
    lastFileName :: V.Vector FileStatus -> ByteString
    lastFileName v | V.null v  = ""
                   | otherwise = fsPath (V.last v)

    loop :: [V.Vector FileStatus] -> ByteString -> Hdfs (V.Vector FileStatus)
    loop ps startAfter = do
        PartialListing{..} <- fromMaybe (PartialListing 0 V.empty)
                          <$> getPartialListing path startAfter

        let ps' = ps ++ [lsFiles]

        if lsRemaining == 0
           then return (V.concat ps')
           else loop ps' (lastFileName lsFiles)

getListing' :: HdfsPath -> Hdfs (V.Vector FileStatus)
getListing' path = fromMaybe V.empty <$> getListing path

------------------------------------------------------------------------

getPartialListing :: HdfsPath -> HdfsPath -> Hdfs (Maybe PartialListing)
getPartialListing path startAfter = fmap fromProtoDirectoryListing . getField . P.lsDirList <$>
    hdfsInvoke "getListing" P.GetListingRequest
    { P.lsSrc          = putField (T.decodeUtf8 path)
    , P.lsStartAfter   = putField startAfter
    , P.lsNeedLocation = putField False
    }

getFileInfo :: HdfsPath -> Hdfs (Maybe FileStatus)
getFileInfo path = fmap fromProtoFileStatus . getField . P.fiFileStatus <$>
    hdfsInvoke "getFileInfo" P.GetFileInfoRequest
    { P.fiSrc = putField (T.decodeUtf8 path)
    }

getContentSummary :: HdfsPath -> Hdfs ContentSummary
getContentSummary path = fromProtoContentSummary . getField . P.csSummary <$>
    hdfsInvoke "getContentSummary" P.GetContentSummaryRequest
    { P.csPath = putField (T.decodeUtf8 path)
    }

mkdirs :: CreateParent -> HdfsPath -> Hdfs Bool
mkdirs createParent path = getField . P.mdResult <$>
    hdfsInvoke "mkdirs" P.MkdirsRequest
    { P.mdSrc          = putField (T.decodeUtf8 path)
    , P.mdMasked       = putField (P.FilePermission (putField 0o755))
    , P.mdCreateParent = putField createParent
    }

delete :: Recursive -> HdfsPath -> Hdfs Bool
delete recursive path = getField . P.dlResult <$>
    hdfsInvoke "delete" P.DeleteRequest
    { P.dlSrc       = putField (T.decodeUtf8 path)
    , P.dlRecursive = putField recursive
    }

rename :: Overwrite -> HdfsPath -> HdfsPath -> Hdfs ()
rename overwrite src dst = ignore <$>
    hdfsInvoke "rename2" P.Rename2Request
    { P.mvSrc       = putField (T.decodeUtf8 src)
    , P.mvDst       = putField (T.decodeUtf8 dst)
    , P.mvOverwrite = putField overwrite
    }
  where
    ignore :: P.Rename2Response -> ()
    ignore = const ()

setPermissions :: Word32 -> HdfsPath -> Hdfs ()
setPermissions mode path = ignore <$>
    hdfsInvoke "setPermission" P.SetPermissionRequest
    { P.chmodPath = putField (T.decodeUtf8 path)
    , P.chmodMode = putField $ P.FilePermission { fpPerm = putField mode }
    }
  where
    ignore :: P.SetPermissionResponse -> ()
    ignore = const ()

------------------------------------------------------------------------
-- Awesome Protobuf Mapping (╯°□°)╯︵ ┻━┻

fromProtoContentSummary :: P.ContentSummary -> ContentSummary
fromProtoContentSummary p = ContentSummary
    { csLength         = getField $ P.csLength p
    , csFileCount      = getField $ P.csFileCount p
    , csDirectoryCount = getField $ P.csDirectoryCount p
    , csQuota          = getField $ P.csQuota p
    , csSpaceConsumed  = getField $ P.csSpaceConsumed p
    , csSpaceQuota     = getField $ P.csSpaceQuota p
    }

fromProtoDirectoryListing :: P.DirectoryListing -> PartialListing
fromProtoDirectoryListing p = PartialListing
    { lsRemaining = fromIntegral . getField $ P.dlRemaingEntries p
    , lsFiles     = V.map fromProtoFileStatus . V.fromList . getField $ P.dlPartialListing p
    }

fromProtoFileStatus :: P.FileStatus -> FileStatus
fromProtoFileStatus p = FileStatus
    { fsFileType         = fromProtoFileType . getField $ P.fsFileType p
    , fsPath             = getField $ P.fsPath p
    , fsLength           = getField $ P.fsLength p
    , fsPermission       = fromIntegral . getField . P.fpPerm . getField $ P.fsPermission p
    , fsOwner            = getField $ P.fsOwner p
    , fsGroup            = getField $ P.fsGroup p
    , fsModificationTime = getField $ P.fsModificationTime p
    , fsAccessTime       = getField $ P.fsAccessTime p
    , fsSymLink          = getField $ P.fsSymLink p
    , fsBlockReplication = fromIntegral . fromMaybe 0 . getField $ P.fsBlockReplication p
    , fsBlockSize        = fromMaybe 0 . getField $ P.fsBlockSize p
    }

fromProtoFileType :: P.FileType -> FileType
fromProtoFileType p = case p of
    P.IS_DIR     -> Dir
    P.IS_FILE    -> File
    P.IS_SYMLINK -> SymLink
