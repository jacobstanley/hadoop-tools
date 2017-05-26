{-# LANGUAGE OverloadedStrings #-}
module Hadoop.Tools.Path
    ( getHomeDir
    , getWorkingDir
    , setWorkingDir
    , getAbsolute
    , dropFileName
    , takeFileName
    , takeParent
    , displayPath
    ) where

import           Control.Exception (SomeException)
import           Control.Monad.Catch (handle)
import           Control.Monad.IO.Class (MonadIO, liftIO)

import qualified Data.ByteString.Char8 as B
import           Data.Hadoop.HdfsPath
import           Data.Hadoop.Types
import           Data.Monoid
import qualified Data.Text.Encoding as T

import           System.Environment (getEnv)
import qualified System.FilePath as FilePath
import qualified System.FilePath.Posix as Posix
import           System.IO.Unsafe (unsafePerformIO)

import           Hadoop.Tools.Configuration

-- | The path in which our working dir state is stored.
workingDirConfigPath :: FilePath
workingDirConfigPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home `FilePath.combine` ".hhwd")
{-# NOINLINE workingDirConfigPath #-}

-- | Retrieve the current users home directory on HDFS.
getHomeDir :: MonadIO m => m HdfsPath
getHomeDir = liftIO $ (("/user" </>) . T.encodeUtf8 . (udUser . hcUser)) <$> getConfig

-- | Get the working directory for your current session.
getWorkingDir :: MonadIO m => m HdfsPath
getWorkingDir = liftIO $ handle onError
                       $ B.takeWhile (/= '\n')
                     <$> B.readFile workingDirConfigPath
  where
    onError :: SomeException -> IO HdfsPath
    onError = const getHomeDir

-- | Set the working directory for your current session.
setWorkingDir :: MonadIO m => HdfsPath -> m ()
setWorkingDir path = liftIO $ B.writeFile workingDirConfigPath
                            $ path <> "\n"

-- | Convert a path on HDFS to an absolute path.
getAbsolute :: MonadIO m => HdfsPath -> m HdfsPath
getAbsolute path = liftIO (normalizePath <$> getPath)
  where
    getPath = if "/" `B.isPrefixOf` path
              then return path
              else getWorkingDir >>= \pwd -> return (pwd </> path)

normalizePath :: HdfsPath -> HdfsPath
normalizePath = B.intercalate "/" . dropAbsParentDir . B.split '/'

dropAbsParentDir :: [HdfsPath] -> [HdfsPath]
dropAbsParentDir []       = error "dropAbsParentDir: not an absolute path"
dropAbsParentDir (p : ps) = p : reverse (fst $ go [] ps)
  where
    go []       (".." : ys) = go [] ys
    go (_ : xs) (".." : ys) = go xs ys
    go xs       ("."  : ys) = go xs ys
    go xs       (y    : ys) = go (y : xs) ys
    go xs       []          = (xs, [])


dropFileName :: HdfsPath -> HdfsPath
dropFileName = B.pack . Posix.dropFileName . B.unpack

takeFileName :: HdfsPath -> HdfsPath
takeFileName = B.pack . Posix.takeFileName . B.unpack

takeParent :: HdfsPath -> HdfsPath
takeParent bs = case B.elemIndexEnd '/' bs of
    Nothing -> B.empty
    Just 0  -> "/"
    Just ix -> B.take ix bs

displayPath :: HdfsPath -> FileStatus -> HdfsPath
displayPath parent file = parent </> fsPath file <> suffix
  where
    suffix = case fsFileType file of
        Dir -> "/"
        _   -> ""
