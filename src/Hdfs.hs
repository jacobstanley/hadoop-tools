{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Hdfs where

import           Control.Applicative ((<$>), (<*>), pure)
import           Control.Exception (SomeException)
import           Control.Monad
import           Control.Monad.Catch (handle, throwM)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.Configurator as C
import           Data.Configurator.Types (Worth(..))
import           Data.Monoid ((<>))
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import           System.Environment (getEnv)
import           System.FilePath.Posix
import           System.IO.Unsafe (unsafePerformIO)

import           Data.Hadoop.Configuration (getHadoopConfig)
import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs hiding (runHdfs)

------------------------------------------------------------------------

runHdfs :: Hdfs a -> IO a
runHdfs hdfs = do
    config <- getConfig
    runHdfs' config hdfs

getConfig :: IO HadoopConfig
getConfig = do
    hdfsUser   <- getHdfsUser
    nameNode   <- getNameNode
    socksProxy <- getSocksProxy

    liftM ( set hdfsUser   (\c x -> c { hcUser      = x })
          . set nameNode   (\c x -> c { hcNameNodes = [x] })
          . set socksProxy (\c x -> c { hcProxy     = Just x })
          ) getHadoopConfig
  where
    set :: Maybe a -> (b -> a -> b) -> b -> b
    set m f c = maybe c (f c) m

------------------------------------------------------------------------

configPath :: FilePath
configPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home </> ".hh")
{-# NOINLINE configPath #-}

getHdfsUser :: IO (Maybe User)
getHdfsUser = C.load [Optional configPath] >>= flip C.lookup "hdfs.user"

getNameNode :: IO (Maybe NameNode)
getNameNode = do
    cfg  <- C.load [Optional configPath]
    host <- C.lookup cfg "namenode.host"
    port <- C.lookupDefault 8020 cfg "namenode.port"
    return (Endpoint <$> host <*> pure port)

getSocksProxy :: IO (Maybe SocksProxy)
getSocksProxy = do
    cfg   <- C.load [Optional configPath]
    mhost <- C.lookup cfg "proxy.host"
    case mhost of
        Nothing   -> return Nothing
        Just host -> Just . Endpoint host <$> C.lookupDefault 1080 cfg "proxy.port"

------------------------------------------------------------------------

workingDirConfigPath :: FilePath
workingDirConfigPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home </> ".hhwd")
{-# NOINLINE workingDirConfigPath #-}

getDefaultWorkingDir :: MonadIO m => m HdfsPath
getDefaultWorkingDir = liftIO $ (("/user" //) . T.encodeUtf8 . hcUser) <$> getConfig

getWorkingDir :: MonadIO m => m HdfsPath
getWorkingDir = liftIO $ handle onError
                       $ B.takeWhile (/= '\n')
                     <$> B.readFile workingDirConfigPath
  where
    onError :: SomeException -> IO HdfsPath
    onError = const getDefaultWorkingDir

setWorkingDir :: MonadIO m => HdfsPath -> m ()
setWorkingDir path = liftIO $ B.writeFile workingDirConfigPath
                            $ path <> "\n"

getAbsolute :: MonadIO m => HdfsPath -> m HdfsPath
getAbsolute path = liftIO (normalizePath <$> getPath)
  where
    getPath = if "/" `B.isPrefixOf` path
              then return path
              else getWorkingDir >>= \pwd -> return (pwd // path)

normalizePath :: HdfsPath -> HdfsPath
normalizePath = B.intercalate "/" . dropAbsParentDir . B.split '/'

dropAbsParentDir :: [HdfsPath] -> [HdfsPath]
dropAbsParentDir []       = error "dropAbsParentDir: not an absolute path"
dropAbsParentDir (p : ps) = p : reverse (fst $ go [] ps)
  where
    go []       (".." : ys) = go [] ys
    go (_ : xs) (".." : ys) = go xs ys
    go xs       (y    : ys) = go (y : xs) ys
    go xs       []          = (xs, [])

------------------------------------------------------------------------

getListingOrFail :: HdfsPath -> Hdfs (V.Vector FileStatus)
getListingOrFail path = do
    mls <- getListing False path
    case mls of
      Nothing -> throwM $ RemoteError ("File/directory does not exist: " <> T.decodeUtf8 path) T.empty
      Just ls -> return ls

------------------------------------------------------------------------

infixr 5 //

(//) :: HdfsPath -> HdfsPath -> HdfsPath
(//) xs ys | B.isPrefixOf "/" ys = ys
           | otherwise           = trimEnd '/' xs <> "/" <> ys

trimEnd :: Char -> ByteString -> ByteString
trimEnd b bs | B.null bs      = B.empty
             | B.last bs == b = trimEnd b (B.init bs)
             | otherwise      = bs
