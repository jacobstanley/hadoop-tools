module Network.Hadoop.Types where

import Data.Text
import Data.ByteString
import Control.Exception
import Data.Hadoop.Types

------------------------------------------------------------------------

data Connection = Connection
    { cnVersion  :: !Int
    , cnConfig   :: !HadoopConfig
    , cnProtocol :: !Protocol
    , invokeRaw  :: !(Method -> RawRequest -> (RawResponse -> IO ()) -> IO ())
    }

data Protocol = Protocol
    { prName    :: !Text
    , prVersion :: !Int
    } deriving (Eq, Ord, Show)

type Method      = Text
type RawRequest  = ByteString
type RawResponse = Either SomeException ByteString

