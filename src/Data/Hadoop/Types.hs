{-# LANGUAGE DeriveDataTypeable #-}

module Data.Hadoop.Types where

import Control.Exception (Exception)
import Data.ByteString (ByteString)
import Data.Data (Data)
import Data.Text (Text)
import Data.Typeable (Typeable)

------------------------------------------------------------------------

type User = Text

type NameNode   = Endpoint
type SocksProxy = Endpoint

data Endpoint = Endpoint
    { epHost :: HostName
    , epPort :: Port
    } deriving (Eq, Ord, Show)

type HostName = Text
type Port     = Int

type HdfsPath = ByteString

------------------------------------------------------------------------

data HadoopConfig = HadoopConfig
    { hcUser      :: User
    , hcNameNodes :: [NameNode]
    , hcProxy     :: Maybe SocksProxy
    } deriving (Eq, Ord, Show)

------------------------------------------------------------------------

data ConfigError = ConfigError !Text
    deriving (Show, Eq, Data, Typeable)

instance Exception ConfigError

------------------------------------------------------------------------

data RemoteError = RemoteError !Text !Text
    deriving (Show, Eq, Data, Typeable)

instance Exception RemoteError
