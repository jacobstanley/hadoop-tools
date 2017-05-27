{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Hadoop.Tools.Run
    ( runHdfs
    , exitError
    , printError
    , isAccessDenied
    , isStandbyError
    )
    where

import           Control.Monad.Catch (handle, throwM)

import qualified Data.Text as T
import qualified Data.Text.IO as T

import           System.Exit (exitFailure)
import           System.IO

import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs hiding (runHdfs)

import           Hadoop.Tools.Configuration

-- | Run @Hdfs@ actions, returning a result in IO. Differes from
-- @Network.Hadoop.Hdfs@ by using the `.hh` configuration and by detecting
-- namenode failover and attempting to access a different namenode.
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

-- | Print remote errors in a slightly more human friendly fashion.
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
