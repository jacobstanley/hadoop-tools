{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Hadoop.Rpc
    ( Connection(..)
    , Protocol(..)
    , User
    , Method
    , RawRequest
    , RawResponse

    , initConnectionV7
    , invoke
    ) where

import           Control.Applicative ((<$>), (<*>))
import           Control.Exception (throwIO)
import           Control.Monad.IO.Class (liftIO)

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.IORef
import           Data.Monoid (mempty)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import           Data.Serialize.Get
import           Data.Serialize.Put

import           Data.Hadoop.Protobuf.Headers
import           Data.Hadoop.Types
import qualified Network.Hadoop.Stream as S
import           Network.Socket (Socket)

------------------------------------------------------------------------

data Connection = Connection
    { cnVersion  :: !Int
    , cnUser     :: !User
    , cnProtocol :: !Protocol
    , invokeRaw  :: !(Method -> RawRequest -> IO RawResponse)
    }

data Protocol = Protocol
    { prName    :: !Text
    , prVersion :: !Int
    } deriving (Eq, Ord, Show)

type Method      = Text
type RawRequest  = ByteString
type RawResponse = ByteString

------------------------------------------------------------------------

-- hadoop-2.1.0-beta is on version 9
-- see https://issues.apache.org/jira/browse/HADOOP-8990 for differences

initConnectionV7 :: User -> Protocol -> Socket -> IO Connection
initConnectionV7 user protocol sock = do
    stream <- S.mkSocketStream sock
    S.runPut stream $ do
        putByteString "hrpc"
        putWord8 7  -- version
        putWord8 80 -- auth method (80 = simple, 81 = kerberos/gssapi, 82 = token/digest-md5)
        putWord8 0  -- ipc serialization type (0 = protobuf)

        let bs = runPut (encodeMessage context)
        putWord32be (fromIntegral (B.length bs))
        putByteString bs

    ref <- newIORef 0
    return (Connection 7 user protocol (sendAndWait stream ref))
  where
    sendAndWait :: S.Stream -> IORef Int -> Method -> ByteString -> IO ByteString
    sendAndWait stream ref method requestBytes = do
        callId <- atomicModifyIORef' ref (\x -> (succ x, x))

        S.runPut stream $ do
            let bs = runPut $ encodeLengthPrefixedMessage (requestHeader callId)
                           >> encodeLengthPrefixedMessage (request method requestBytes)

            putWord32be (fromIntegral (B.length bs))
            putByteString bs

        responseHdr <- S.maybeGet stream decodeLengthPrefixedMessage
        case getField . rspStatus <$> responseHdr of
            Just Success -> S.runGet stream getResponse
            Just _       -> S.runGet stream getError >>= liftIO . throwIO
            Nothing      -> throwClosed

    context = IpcConnectionContext
        { ctxProtocol = putField (Just (prName protocol))
        , ctxUserInfo = putField (Just UserInformation
            { effectiveUser = putField (Just user)
            , realUser      = mempty
            })
        }

    requestHeader callId = RpcRequestHeader
        { reqKind       = putField (Just ProtocolBuffer)
        , reqOp         = putField (Just FinalPacket)
        , reqCallId     = putField (fromIntegral callId)
        }

    request method bytes = RpcRequest
        { reqMethodName      = putField method
        , reqBytes           = putField (Just bytes)
        , reqProtocolName    = putField (prName protocol)
        , reqProtocolVersion = putField (fromIntegral (prVersion protocol))
        }

    throwClosed = throwIO (RemoteError "ConnectionClosed" "The socket connection was closed")

------------------------------------------------------------------------

getResponse :: Get ByteString
getResponse = do
    n <- fromIntegral <$> getWord32be
    getByteString n

getError :: Get RemoteError
getError = RemoteError <$> getText <*> getText
  where
    getText = do
        n <- fromIntegral <$> getWord32be
        T.decodeUtf8 <$> getByteString n

------------------------------------------------------------------------

invoke :: (Decode b, Encode a) => Connection -> Text -> a -> IO b
invoke Connection{..} method arg = decodeBytes =<< invokeRaw method (encodeBytes arg)
  where
    encodeBytes = runPut . encodeMessage
    decodeBytes bs = case runGetState decodeMessage bs 0 of
        Left err      -> throwIO (RemoteError "DecodeError" (T.pack err))
        Right (x, "") -> return x
        Right (_, _)  -> throwIO (RemoteError "DecodeError" "decoded response but did not consume enough bytes")
