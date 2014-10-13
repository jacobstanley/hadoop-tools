module Chmod (
      parseChmod
    ) where

import qualified Data.Attoparsec.ByteString.Char8 as Atto
import qualified Data.ByteString.Char8 as B
import           Data.Char (ord)
import           Data.Word (Word16, Word32)

parseChmod :: Atto.Parser Word16
parseChmod = octal
  where
    octal :: Atto.Parser Word16
    octal = B.foldl' step 0 `fmap` Atto.takeWhile1 isDig
      where
        isDig w = w >= '0' && w <= '7'
        step a w = a * 8 + fromIntegral (ord w - 48)
