{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.Hadoop.Types
import Network.Hadoop.Hdfs
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (testCase)

------------------------------------------------------------------------

main :: IO ()
main = defaultMain testTree

testTree :: TestTree
testTree = testGroup "Commands" [listing]
  where
    listing = testCase "Listing root" $ do
        files <- runHdfs' config (getListing' "/")
        print files

config :: HadoopConfig
config = HadoopConfig {
      hcUser      = "hdfs"
    , hcNameNodes = [(Endpoint "127.0.0.1" 8020)]
    , hcProxy     = Just (Endpoint "127.0.0.1" 2080)
    }
