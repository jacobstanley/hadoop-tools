{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Hadoop.Tools.Run where

import           Control.Exception (SomeException)
import           Control.Monad.Catch (handle, throwM)
import           Control.Monad.IO.Class (MonadIO, liftIO)

import qualified Data.ByteString.Char8 as B
import           Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T

import           System.Environment (getEnv)
import           System.Exit (exitFailure)
import qualified System.FilePath as FilePath
import qualified System.FilePath.Posix as Posix
import           System.IO
import           System.IO.Unsafe (unsafePerformIO)

import           Data.Hadoop.HdfsPath
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs hiding (runHdfs)

import Hadoop.Tools.Configuration

-- | Run @Hdfs@ actions, returning a result in IO. Differes from
-- @Network.Hadoop.Hdfs@ by detecting namenode failover and attempting to
-- access a different namenode.
runHdfs :: forall a. Hdfs a -> IO a
runHdfs hdfs = do
    config <- getConfig
    run config
  where
    run :: HadoopConfig -> IO a
    run cfg = handle (runAgain cfg) (runHdfs' cfg hdfs)

    runAgain :: HadoopConfig -> RemoteError -> IO a
    runAgain cfg e | isStandbyError e = maybe (throwM e) run (dropNameNode cfg)
                   | otherwise        = throwM e

    dropNameNode :: HadoopConfig -> Maybe HadoopConfig
    dropNameNode cfg | null ns   = Nothing
                     | otherwise = Just (cfg { hcNameNodes = ns })
      where
        ns = drop 1 (hcNameNodes cfg)

-- | Exception handler which prints remote errors and terminates the process.
exitError :: RemoteError -> IO a
exitError err = printError err >> exitFailure

------------------------------------------------------------------------

workingDirConfigPath :: FilePath
workingDirConfigPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home `FilePath.combine` ".hhwd")
{-# NOINLINE workingDirConfigPath #-}

-- | Retrieve the current users home directory on HDFS
getHomeDir :: MonadIO m => m HdfsPath
getHomeDir = liftIO $ (("/user" </>) . T.encodeUtf8 . (udUser . hcUser)) <$> getConfig

getWorkingDir :: MonadIO m => m HdfsPath
getWorkingDir = liftIO $ handle onError
                       $ B.takeWhile (/= '\n')
                     <$> B.readFile workingDirConfigPath
  where
    onError :: SomeException -> IO HdfsPath
    onError = const getHomeDir

setWorkingDir :: MonadIO m => HdfsPath -> m ()
setWorkingDir path = liftIO $ B.writeFile workingDirConfigPath
                            $ path <> "\n"
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

printError :: RemoteError -> IO ()
printError (RemoteError subject body)
    | oneLiner    = T.hPutStrLn stderr firstLine
    | T.null body = T.hPutStrLn stderr subject
    | otherwise   = T.hPutStrLn stderr subject >> T.putStrLn body
  where
    oneLiner  = subject `elem` [ "org.apache.hadoop.security.AccessControlException"
                               , "org.apache.hadoop.fs.FileAlreadyExistsException"
                               , "java.io.FileNotFoundException" ]
    firstLine = T.takeWhile (/= '\n') body

isAccessDenied :: RemoteError -> Bool
isAccessDenied (RemoteError s _) = s == "org.apache.hadoop.security.AccessControlException"

isStandbyError :: RemoteError -> Bool
isStandbyError (RemoteError s _) = s == "org.apache.hadoop.ipc.StandbyException"

------------------------------------------------------------------------
