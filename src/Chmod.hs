module Chmod (
      Chmod(..)
    , parseChmod
    , applyChmod
    ) where

import           Control.Applicative ((<$>))
import qualified Data.Attoparsec.ByteString.Char8 as Atto
import qualified Data.ByteString.Char8 as B
import           Data.Char (ord)
import           Data.Word (Word16, Word32)

data Chmod = SetMode Word16
    deriving (Show, Eq)

parseChmod :: Atto.Parser Chmod
parseChmod = SetMode <$> octal
  where
    octal :: Atto.Parser Word16
    octal = B.foldl' step 0 `fmap` Atto.takeWhile1 isDig
      where
        isDig w = w >= '0' && w <= '7'
        step a w = a * 8 + fromIntegral (ord w - 48)

applyChmod :: Chmod -> Word16 -> Word16
applyChmod (SetMode new) _ = new
