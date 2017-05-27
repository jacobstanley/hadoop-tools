{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Hadoop.Tools.Options
    ( completePath
    , completeDir
    , bstr
    , hdfsPathArg
    , hdfsDirArg
    , HdfsPath
    ) where

import           Control.Monad.Catch (handle)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Monoid
import qualified Data.Vector as V

import           Options.Applicative hiding (Success)

import           Data.Hadoop.HdfsPath
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs hiding (runHdfs)

import           Hadoop.Tools.Run
import           Hadoop.Tools.Path

completePath :: Mod ArgumentFields a
completePath = completer (fileCompletion (const True)) <> metavar "PATH"

completeDir :: Mod ArgumentFields a
completeDir  = completer (fileCompletion (== Dir)) <> metavar "DIRECTORY"

bstr :: ReadM ByteString
bstr = B.pack <$> str

-- | @Parser@ for @HdfsPath@ arguments that complete to paths on HDFS
hdfsPathArg :: Mod ArgumentFields HdfsPath -> Parser HdfsPath
hdfsPathArg opts = argument bstr (completePath <> opts)

-- | @Parser@ for @HdfsPath@ arguments that complete to directories on HDFS
hdfsDirArg :: Mod ArgumentFields HdfsPath -> Parser HdfsPath
hdfsDirArg opts = argument bstr (completeDir <> opts)

fileCompletion :: (FileType -> Bool) -> Completer
fileCompletion p = mkCompleter $ \strPath -> handle ignore $ runHdfs $ do
    let path = B.pack strPath
        dir  = takeParent path

    ls <- getListing' =<< getAbsolute dir

    return $ V.toList
           . V.map B.unpack
           . V.filter (path `B.isPrefixOf`)
           . V.map (displayPath dir)
           . V.filter (p . fsFileType)
           $ ls
  where
    ignore (RemoteError _ _) = return []
