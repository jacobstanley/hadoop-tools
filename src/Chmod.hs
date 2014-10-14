module Chmod (
      Chmod(..)
    , parseChmod
    , applyChmod
    ) where

import           Control.Applicative ((<$>))
import           Control.Monad (guard)
import qualified Data.Attoparsec.ByteString.Char8 as Atto
import qualified Data.Attoparsec.Combinator as Atto
import qualified Data.ByteString.Char8 as B
import           Data.Char (ord)
import           Data.List (foldl')
import           Data.Word (Word16, Word32)

data Chmod = SetMode Word16
    deriving (Show, Eq)

parseChmod :: Atto.Parser [Chmod]
parseChmod = do
    cs <- Atto.sepBy chmod1 (Atto.char ',')
    guard (cs /= [])
    return cs
  where
    chmod1 = SetMode <$> octal

    octal :: Atto.Parser Word16
    octal = B.foldl' step 0 `fmap` Atto.takeWhile1 isDig
      where
        isDig w = w >= '0' && w <= '7'
        step a w = a * 8 + fromIntegral (ord w - 48)

applyChmod :: [Chmod] -> Word16 -> Word16
applyChmod cs old = foldl' f old cs
  where
    f :: Word16 -> Chmod -> Word16
    f _ (SetMode new) = new
