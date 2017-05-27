module Hadoop.Tools.Options.Chmod (
      Chmod(..)
    , ChmodWho(..)
    , ChmodWhat(..)
    , parseChmod
    , applyChmod
    ) where

import           Control.Applicative
import           Control.Monad (guard, msum)

import qualified Data.Attoparsec.ByteString.Char8 as Atto
import           Data.Bits
import qualified Data.ByteString.Char8 as B
import           Data.Char (ord, isOctDigit)
import           Data.List (foldl')
import           Data.Maybe (mapMaybe)
import           Data.Word (Word16)

import           Prelude

import           Data.Hadoop.Types (FileType(..))

------------------------------------------------------------------------

data ChmodWho = Chmod_u | Chmod_g | Chmod_o | Chmod_a
    deriving (Show, Eq)
data ChmodWhat = Chmod_r | Chmod_w | Chmod_x | Chmod_X
    | Chmod_s | Chmod_t
    deriving (Show, Eq)

data Chmod = SetOctal    Word16
           | SetEqual    ChmodWho [ChmodWhat]
           | SetPlus     ChmodWho [ChmodWhat]
           | SetMinus    ChmodWho [ChmodWhat]
           | SetEqualWho ChmodWho ChmodWho
           | SetPlusWho  ChmodWho ChmodWho
           | SetMinusWho ChmodWho ChmodWho
    deriving (Show, Eq)

parseChmod :: Atto.Parser [Chmod]
parseChmod = do
    cs <- Atto.sepBy chmod1 (Atto.char ',')
    guard (cs /= [])
    return cs
  where
    chmod1 = msum
        [ SetOctal <$> octal
        , SetEqual <$> ugoa <* Atto.char '=' <*> Atto.many1 rwx
        , SetPlus  <$> ugoa <* Atto.char '+' <*> Atto.many1 rwx
        , SetMinus <$> ugoa <* Atto.char '-' <*> Atto.many1 rwx
        , SetEqualWho <$> ugoa <* Atto.char '=' <*> ugo
        , SetPlusWho  <$> ugoa <* Atto.char '+' <*> ugo
        , SetMinusWho <$> ugoa <* Atto.char '-' <*> ugo
        ]

    ugo :: Atto.Parser ChmodWho
    ugo = msum
        [ pure Chmod_u <* Atto.char 'u'
        , pure Chmod_g <* Atto.char 'g'
        , pure Chmod_o <* Atto.char 'o'
        ]

    ugoa :: Atto.Parser ChmodWho
    ugoa = msum
        [ ugo
        , pure Chmod_a <* Atto.char 'a'
        ]

    rwx :: Atto.Parser ChmodWhat
    rwx = msum
        [ pure Chmod_r <* Atto.char 'r'
        , pure Chmod_w <* Atto.char 'w'
        , pure Chmod_x <* Atto.char 'x'
        , pure Chmod_X <* Atto.char 'X'
        {-
        , pure Chmod_s <* Atto.char 's'
        , pure Chmod_t <* Atto.char 't'
        -}
        ]

    octal :: Atto.Parser Word16
    octal = B.foldl' step 0 `fmap` Atto.takeWhile1 isOctDigit
      where
        step a w = a * 8 + fromIntegral (ord w - 48)

applyChmod :: FileType -> [Chmod] -> Word16 -> Word16
applyChmod filetype = flip (foldl' f)
  where
    f :: Word16 -> Chmod -> Word16
    f _   (SetOctal new)        = new
    f old (SetEqual who ws)     = set who old (foldRWX old ws)
    f old (SetPlus  who ws)     = plus who old (foldRWX old ws)
    f old (SetMinus who ws)     = minus who old (foldRWX old ws)
    f old (SetEqualWho who src) = set who old (extract src old)
    f old (SetPlusWho who src)  = plus who old (extract src old)
    f old (SetMinusWho who src) = minus who old (extract src old)

    set :: ChmodWho -> Word16 -> Word16 -> Word16
    set who old new = (old .&. complement (mask who)) .|. setWho who new

    plus :: ChmodWho -> Word16 -> Word16 -> Word16
    plus who old new = old .|. setWho who new

    minus :: ChmodWho -> Word16 -> Word16 -> Word16
    minus who old new = old `xor` setWho who new

    foldRWX old = foldl' (\old' what -> old' .|. b what) 0 . mapMaybe (hX old)

    o3 u g o = u*64 + g*8 + o

    mask :: ChmodWho -> Word16
    mask Chmod_a = o3 7 7 7
    mask who     = 7 `shiftL` s who

    setWho :: ChmodWho -> Word16 -> Word16
    setWho Chmod_a new = foldl' (.|.) 0 $
        map (`setWho` new) [Chmod_u, Chmod_g, Chmod_o]
    setWho who new = new `shiftL` s who

    extract :: ChmodWho -> Word16 -> Word16
    extract who old = (mask who .&. old) `shiftR` s who

    -- handle X
    hX :: Word16 -> ChmodWhat -> Maybe ChmodWhat
    hX old Chmod_X = if filetype == Dir || old .&. o3 1 1 1 /= 0
                         then Just Chmod_x
                         else Nothing
    hX _   what    = Just what

    -- Bit to set for what
    b :: ChmodWhat -> Word16
    b Chmod_r = 4
    b Chmod_w = 2
    b Chmod_x = 1
    b Chmod_X = error "applyChmod: can't set a bit for X"
    b Chmod_s = error "applyChmod: can't set a bit for s"
    b Chmod_t = error "applyChmod: can't set a bit for t"

    -- Number of bits to shift for who
    s :: ChmodWho -> Int
    s Chmod_u = 6
    s Chmod_g = 3
    s Chmod_o = 0
    s Chmod_a = error "applyChmod: can't shift for a"
