module Data.Hadoop.Types where

import Data.Text (Text)

------------------------------------------------------------------------

type User     = Text
type HostName = Text
type Port     = Int

type NameNode = Endpoint

data Endpoint = Endpoint
    { epHost :: HostName
    , epPort :: Port
    } deriving (Eq, Ord, Show)
