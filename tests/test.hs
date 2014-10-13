{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck as QC

import           Data.Attoparsec.ByteString as Atto
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Either
import           Data.Word (Word16, Word32)

import           Chmod

------------------------------------------------------------

main = defaultMain tests

------------------------------------------------------------

tests :: TestTree
tests = testGroup "Tests" [unitTests]

unitTests = testGroup "Unit tests"
  [ testChmod "755" (493)
  , testChmod "0755" (493)
  , testChmodShouldFail "kittens"
  ]

testChmod :: ByteString -> Word16 -> TestTree
testChmod input expected =
    testCase (unwords ["Parse chmod", B.unpack input]) $
        parseOnly parseChmod input @?= Right expected

testChmodShouldFail :: ByteString -> TestTree
testChmodShouldFail input =
    testCase (unwords ["Parse chmod", B.unpack input, "(should fail)"]) $
        assertBool "Failed to catch invalid chmod"
        (isLeft $ parseOnly parseChmod input)
