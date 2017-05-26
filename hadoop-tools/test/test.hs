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

import           Data.Hadoop.Types (FileType(..))

import           Hadoop.Tools.Options.Chmod

------------------------------------------------------------

main = defaultMain tests

------------------------------------------------------------

tests :: TestTree
tests = testGroup "Tests" [unitTests]

unitTests = testGroup "Unit tests"
  [ testChmod "755"  [SetOctal 493]
  , testChmod "0755" [SetOctal 493]
  , testChmod "u=r"  [SetEqual Chmod_u [Chmod_r]]
  , testChmodShouldFail "kittens"
  
  , testApplyChmod File [SetEqual    Chmod_o [Chmod_r]]
        (oct 0 0 0) (oct 0 0 4)
  , testApplyChmod File [SetEqual    Chmod_o [Chmod_r, Chmod_w]]
        (oct 0 0 0) (oct 0 0 6)
  , testApplyChmod File [SetEqual    Chmod_g [Chmod_r]]
        (oct 0 0 0) (oct 0 4 0)
  , testApplyChmod File [SetPlus     Chmod_o [Chmod_w]]
        (oct 0 0 4) (oct 0 0 6)
  , testApplyChmod File [SetPlus     Chmod_o [Chmod_w,Chmod_r]]
        (oct 0 0 1) (oct 0 0 7)
  , testApplyChmod File [SetMinus    Chmod_o [Chmod_w]]
        (oct 0 0 6) (oct 0 0 4)
  , testApplyChmod File [SetMinus    Chmod_o [Chmod_w,Chmod_r]]
        (oct 0 0 7) (oct 0 0 1)
  , testApplyChmod File [SetEqualWho Chmod_g Chmod_o]
        (oct 0 0 1) (oct 0 1 1)
  , testApplyChmod File [SetPlusWho  Chmod_g Chmod_o]
        (oct 0 2 1) (oct 0 3 1)
  , testApplyChmod File [SetMinusWho Chmod_g Chmod_o]
        (oct 0 3 1) (oct 0 2 1)
  , testApplyChmod File [SetMinusWho Chmod_g Chmod_o]
        (oct 0 3 1) (oct 0 2 1)
  , testApplyChmod File [SetEqual    Chmod_a [Chmod_r]]
        (oct 0 0 0) (oct 4 4 4)
  , testApplyChmod File [SetPlus     Chmod_a [Chmod_r]]
        (oct 0 1 0) (oct 4 5 4)
  , testApplyChmod File [SetMinus    Chmod_a [Chmod_x]]
        (oct 7 5 5) (oct 6 4 4)
  , testApplyChmod File [SetEqualWho Chmod_a Chmod_u]
        (oct 6 4 4) (oct 6 6 6)
  , testApplyChmod File [SetPlusWho  Chmod_a Chmod_u]
        (oct 7 5 5) (oct 7 7 7)
  , testApplyChmod File [SetMinusWho Chmod_a Chmod_o]
        (oct 7 5 5) (oct 2 0 0)

  , testApplyChmod File [SetPlus     Chmod_o [Chmod_X]]
        (oct 0 1 0) (oct 0 1 1)
  , testApplyChmod Dir  [SetPlus     Chmod_o [Chmod_X]]
        (oct 0 0 0) (oct 0 0 1)
  , testApplyChmod Dir  [SetPlus     Chmod_o [Chmod_X]]
        (oct 0 1 0) (oct 0 1 1)
  ]

oct u g o = u*64 + g*8 + o

testChmod :: ByteString -> [Chmod] -> TestTree
testChmod input expected =
    testCase (unwords ["Parse chmod", B.unpack input]) $
        parseOnly parseChmod input @?= Right expected

testChmodShouldFail :: ByteString -> TestTree
testChmodShouldFail input =
    testCase (unwords ["Parse chmod", B.unpack input]) $
        assertBool "Failed to catch invalid chmod"
        (isLeft $ parseOnly parseChmod input)

testApplyChmod :: FileType -> [Chmod] -> Word16 -> Word16 -> TestTree
testApplyChmod filetype input old expected =
    testCase (unwords ["Apply chmod", show input, "to", show old]) $
        applyChmod filetype input old @?= expected

