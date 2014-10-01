{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.Hadoop.Protobuf.Hdfs
import Data.Hadoop.Types
import Data.ProtocolBuffers (getField)
import Network.Hadoop.Hdfs

------------------------------------------------------------------------

main :: IO ()
main = do
    (Just ls) <- runHdfs' (Endpoint "192.168.56.10" 8020) (getListing "/")
    let files = map (getField . fsPath) (getField $ dlPartialListing ls)
    print files
