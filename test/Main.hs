{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Data.ProtocolBuffers (getField)
import qualified Data.Vector as V

import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs

------------------------------------------------------------------------

main :: IO ()
main = do
    files <- runHdfs' config (getListing' "/")
    print $ V.map (getField . fsPath) files

config :: HadoopConfig
config = HadoopConfig {
      hcUser      = "hdfs"
    , hcNameNodes = [(Endpoint "127.0.0.1" 8020)]
    , hcProxy     = Just (Endpoint "127.0.0.1" 2080)
    }
