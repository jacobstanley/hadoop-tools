{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}

module Data.Hadoop.Protobuf.Security where

import Data.ByteString (ByteString)
import Data.ProtocolBuffers
import Data.ProtocolBuffers.Orphans ()
import Data.Text (Text)
import Data.Word (Word64)
import GHC.Generics (Generic)

------------------------------------------------------------------------

-- | Security token identifier
data Token = Token
    { tokenIdentifier :: Required 1 (Value ByteString)
    , tokenPassword   :: Required 2 (Value ByteString)
    , tokenKind       :: Required 3 (Value Text)
    , tokenService    :: Required 4 (Value Text)
    } deriving (Generic, Show)

instance Encode Token
instance Decode Token

data GetDelegationTokenRequest = GetDelegationTokenRequest
    { gdtRenewer :: Required 1 (Value Text)
    } deriving (Generic, Show)

instance Encode GetDelegationTokenRequest
instance Decode GetDelegationTokenRequest

data GetDelegationTokenResponse = GetDelegationTokenResponse
    { gdtToken :: Optional 1 (Message Token)
    } deriving (Generic, Show)

instance Encode GetDelegationTokenResponse
instance Decode GetDelegationTokenResponse

data RenewDelegationTokenRequest = RenewDelegationTokenRequest
    { rdtToken :: Required 1 (Message Token)
    } deriving (Generic, Show)

instance Encode RenewDelegationTokenRequest
instance Decode RenewDelegationTokenRequest

data RenewDelegationTokenResponse = RenewDelegationTokenResponse
    { rtdNewExpiryTime :: Required 1 (Value Word64)
    } deriving (Generic, Show)

instance Encode RenewDelegationTokenResponse
instance Decode RenewDelegationTokenResponse

data CancelDelegationTokenRequest = CancelDelegationTokenRequest
    { cdtToken :: Required 1 (Message Token)
    } deriving (Generic, Show)

instance Encode CancelDelegationTokenRequest
instance Decode CancelDelegationTokenRequest

data CancelDelegationTokenResponse = CancelDelegationTokenResponse
    { -- void response
    } deriving (Generic, Show)

instance Encode CancelDelegationTokenResponse
instance Decode CancelDelegationTokenResponse
