{-# LANGUAGE OverloadedStrings #-}

module Data.Hadoop.HdfsPath
    ( HdfsPath
    , (</>)
    , combine
    ) where

import qualified Data.ByteString.Char8 as B
import           Data.Monoid ((<>))

import           Data.Hadoop.Types

------------------------------------------------------------------------

infixr 5 </>

(</>) :: HdfsPath -> HdfsPath -> HdfsPath
(</>) = combine

combine :: HdfsPath -> HdfsPath -> HdfsPath
combine xs ys | B.null xs        = ys
              | B.null ys        = xs
              | B.head ys == '/' = ys
              | B.last xs == '/' = xs <> ys
              | otherwise        = xs <> "/" <> ys
