{-# LANGUAGE OverloadedStrings #-}

module Hadoop.Tools.Options.Glob
    ( Glob
    , compile
    , matches
    ) where

import           Control.Applicative

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Monoid ((<>))

import           System.IO.Unsafe (unsafeDupablePerformIO)

import qualified Text.Regex.PCRE.ByteString as Regex

import           Prelude

------------------------------------------------------------------------

newtype Glob = Glob { unGlob :: Regex.Regex }

compile :: ByteString -> IO Glob
compile = (glob <$>) . Regex.compile Regex.compBlank Regex.execBlank . globToRegex
  where
    glob (Left (_, str)) = error str
    glob (Right regex)   = Glob regex

matches :: Glob -> ByteString -> Bool
matches g = fromResult . unsafeDupablePerformIO . Regex.execute (unGlob g)
  where
    fromResult (Right (Just _)) = True
    fromResult _                = False

------------------------------------------------------------------------
-- Stolen from Real World Haskell

globToRegex :: ByteString -> ByteString
globToRegex bs = B.pack ("^" <> globToRegex' (B.unpack bs) <> "$")

globToRegex' :: String -> String
globToRegex' ""             = ""
globToRegex' ('*':cs)       = ".*" ++ globToRegex' cs
globToRegex' ('?':cs)       = '.' : globToRegex' cs
globToRegex' ('[':'!':c:cs) = "[^" ++ c : charClass cs
globToRegex' ('[':c:cs)     = '['  :  c : charClass cs
globToRegex' ('[':_)        = error "unterminated character class"
globToRegex' (c:cs)         = escape c ++ globToRegex' cs

escape :: Char -> String
escape c | c `elem` regexChars = '\\' : [c]
         | otherwise = [c]
    where
      regexChars :: [Char]
      regexChars = "\\+()^$.{}]|"

charClass :: String -> String
charClass (']':cs) = ']' : globToRegex' cs
charClass (c:cs)   = c : charClass cs
charClass []       = error "unterminated character class"
