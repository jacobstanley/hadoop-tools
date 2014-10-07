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
    , invokeAsync
    , invoke
    ) where

import           Control.Applicative ((<$>), (<*>))
import           Control.Concurrent (ThreadId, forkIO, newEmptyMVar, putMVar, takeMVar)
import           Control.Concurrent.STM
import           Control.Exception (SomeException(..), throwIO, handle)
import           Control.Monad (forever, when)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as H
import           Data.Hashable (Hashable)
import           Data.Maybe (fromMaybe, isNothing)
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
    { cnVersion    :: !Int
    , cnConfig     :: !HadoopConfig
    , cnProtocol   :: !Protocol
    , invokeRaw    :: !(Method -> RawRequest -> (RawResponse -> IO ()) -> IO ())
    }

data Protocol = Protocol
    { prName    :: !Text
    , prVersion :: !Int
    } deriving (Eq, Ord, Show)

type Method      = Text
type RawRequest  = ByteString
type RawResponse = Either SomeException ByteString

type CallId = Int

------------------------------------------------------------------------

-- hadoop-2.1.0-beta is on version 9
-- see https://issues.apache.org/jira/browse/HADOOP-8990 for differences

data ConnectionState = ConnectionState
    { csStream        :: !S.Stream
    , csCallId        :: !(TVar CallId)
    , csRecvCallbacks :: !(TVar (H.HashMap CallId (RawResponse -> IO ())))
    , csSendQueue     :: !(TQueue (Method, RawRequest, RawResponse -> IO ()))
    , csFatalError    :: !(TVar (Maybe SomeException))
    }

initConnectionV7 :: HadoopConfig -> Protocol -> Socket -> IO Connection
initConnectionV7 config@HadoopConfig{..} protocol sock = do
    csStream <- S.mkSocketStream sock

    S.runPut csStream $ do
        putByteString "hrpc"
        putWord8 7  -- version
        putWord8 80 -- auth method (80 = simple, 81 = kerberos/gssapi, 82 = token/digest-md5)
        putWord8 0  -- ipc serialization type (0 = protobuf)

        let bs = runPut (encodeMessage context)
        putWord32be (fromIntegral (B.length bs))
        putByteString bs

    csCallId        <- newTVarIO 0
    csRecvCallbacks <- newTVarIO H.empty
    csSendQueue     <- newTQueueIO
    csFatalError    <- newTVarIO Nothing

    let cs = ConnectionState{..}

    _ <- forkSend cs
    _ <- forkRecv cs

    return (Connection 7 config protocol (enqueue cs))
  where
    enqueue :: ConnectionState
            -> Method
            -> RawRequest
            -> (RawResponse -> IO ())
            -> IO ()
    enqueue ConnectionState{..} method bs k = do
        merr <- atomically $ do
            merr <- readTVar csFatalError
            when (isNothing merr) $ writeTQueue csSendQueue (method, bs, k)
            return merr
        case merr of
            Just err -> throwIO err
            Nothing  -> return ()

    forkSend :: ConnectionState -> IO ThreadId
    forkSend cs@ConnectionState{..} = forkIO $ handle (onSocketError cs) $ forever $ do
        bs <- atomically $ do
            (method, requestBytes, k) <- readTQueue csSendQueue

            callId <- readTVar csCallId
            modifyTVar' csCallId succ
            modifyTVar' csRecvCallbacks (H.insert callId k)

            return $ runPut $ encodeLengthPrefixedMessage (requestHeaderProto callId)
                           >> encodeLengthPrefixedMessage (requestProto method requestBytes)

        S.runPut csStream $ do
            putWord32be (fromIntegral (B.length bs))
            putByteString bs

    forkRecv :: ConnectionState -> IO ThreadId
    forkRecv cs@ConnectionState{..} = forkIO $ handle (onSocketError cs) $ forever $ do
        hdr <- S.maybeGet csStream decodeLengthPrefixedMessage
        case hdr of
          Nothing     -> throwIO ConnectionClosed
          Just rspHdr -> do
            onResponse <- fromMaybe (return $ return ())
                      <$> lookupDelete csRecvCallbacks (fromIntegral $ getField $ rspCallId rspHdr)
            case getField (rspStatus rspHdr) of
              Success -> S.runGet csStream getResponse >>= onResponse . Right
              _       -> S.runGet csStream getError    >>= onResponse . Left . SomeException

    onSocketError :: ConnectionState -> SomeException -> IO ()
    onSocketError ConnectionState{..} ex = do
        ks <- atomically $ do
            writeTVar csFatalError (Just ex)
            sks <- map (\(_,_,k) -> k) <$> unfoldM (tryReadTQueue csSendQueue)
            rks <- H.elems <$> readTVar csRecvCallbacks
            return (sks ++ rks)

        mapM_ (\k -> handle ignore $ k $ Left ex) ks

    ignore :: SomeException -> IO ()
    ignore _ = return ()

    context = IpcConnectionContext
        { ctxProtocol = putField (Just (prName protocol))
        , ctxUserInfo = putField (Just UserInformation
            { effectiveUser = putField (Just hcUser)
            , realUser      = mempty
            })
        }

    requestHeaderProto callId = RpcRequestHeader
        { reqKind       = putField (Just ProtocolBuffer)
        , reqOp         = putField (Just FinalPacket)
        , reqCallId     = putField (fromIntegral callId)
        }

    requestProto method bytes = RpcRequest
        { reqMethodName      = putField method
        , reqBytes           = putField (Just bytes)
        , reqProtocolName    = putField (prName protocol)
        , reqProtocolVersion = putField (fromIntegral (prVersion protocol))
        }

unfoldM :: Monad m => m (Maybe a) -> m [a]
unfoldM f = go []
  where
    go xs = do
      m <- f
      case m of
        Nothing -> return xs
        Just x  -> go (xs ++ [x])

lookupDelete :: (Eq k, Hashable k) => TVar (H.HashMap k v) -> k -> IO (Maybe v)
lookupDelete var k = atomically $ do
    hm <- readTVar var
    writeTVar var (H.delete k hm)
    return (H.lookup k hm)

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
invoke connection method arg = do
    mv <- newEmptyMVar
    invokeAsync connection method arg (putMVar mv)
    e <- takeMVar mv
    case e of
      Left ex -> throwIO ex
      Right x -> return x

invokeAsync :: (Decode b, Encode a) => Connection -> Text -> a -> (Either SomeException b -> IO ()) -> IO ()
invokeAsync Connection{..} method arg k = invokeRaw method (encodeBytes arg) k'
  where
    k' (Left err) = k (Left err)
    k' (Right bs) = k (decodeBytes bs)

encodeBytes :: Encode a => a -> ByteString
encodeBytes = runPut . encodeMessage

decodeBytes :: Decode a => ByteString -> Either SomeException a
decodeBytes bs = case runGetState decodeMessage bs 0 of
    Left err      -> decodeError (T.pack err)
    Right (x, "") -> Right x
    Right (_, _)  -> decodeError "decoded response but did not consume enough bytes"
  where
    decodeError = Left . SomeException . DecodeError
