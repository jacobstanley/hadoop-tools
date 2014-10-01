{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Control.Applicative ((<$>))
import           Data.Maybe (fromMaybe)
import           Data.ProtocolBuffers (getField)
import qualified Data.Vector as V

import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs

------------------------------------------------------------------------

main :: IO ()
main = do
    files <- runHdfs' (Endpoint "192.168.56.10" 8020) $ do
        foo <- fromMaybe V.empty <$> getListing "/"
        return foo

    print $ V.map (getField . fsPath) files
