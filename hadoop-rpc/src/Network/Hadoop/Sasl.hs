{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Hadoop.Sasl where

import           Network.Protocol.SASL.GNU
import           Data.ProtocolBuffers

import           Control.Exception (Exception)

import           GHC.Generics
import           Data.ByteString
import           Data.Text
import           Data.Typeable
import           Data.Word

------------------------------------------------------------------------

newtype SaslException = SaslException Error
    deriving (Show, Typeable)

instance Exception SaslException

data SaslNegotiationError
    = FailedNegotiation
    | NoSharedMechanism
    | NoToken
    | UnexptededState
    deriving (Show, Typeable)

instance Exception SaslNegotiationError

rpcSaslProto :: Text
rpcSaslProto = "RpcSaslProto"

data RpcSaslProto = RpcSaslProto
    { spVersion :: Optional 1 (Value Word32)
    , spState   :: Required 2 (Enumeration SaslState)
    , spToken   :: Optional 3 (Value ByteString)
    , spAuths   :: Repeated 4 (Message SaslAuth)
    } deriving (Eq, Ord, Show, Generic)

data SaslAuth = SaslAuth
    { saMethod    :: Required 1 (Value ByteString)
    , saMechanism :: Required 2 (Value ByteString)
    , saProtocol  :: Optional 3 (Value ByteString)
    , saServerId  :: Optional 4 (Value ByteString)
    , saChallenge :: Optional 5 (Value ByteString)
    } deriving (Eq, Ord, Show, Generic)

data SaslState
    = Success
    | Negotiate
    | Initiate
    | Challenge
    | Response
    | Wrap
    deriving (Eq, Ord, Show, Enum, Bounded)

instance Encode SaslAuth
instance Decode SaslAuth
instance Encode RpcSaslProto
instance Decode RpcSaslProto

