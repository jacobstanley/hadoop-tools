{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Hadoop.Rpc
    ( Connection(..)
    , Protocol(..)
    , User
    , Method
    , RawRequest
    , RawResponse

    , initConnectionV9
    , invokeAsync
    , invoke
    ) where

import           Control.Applicative ((<$>))
import           Control.Concurrent (ThreadId, forkIO, newEmptyMVar, putMVar, takeMVar)
import           Control.Concurrent.STM
import           Control.Exception (SomeException(..), throwIO, handle)
import           Control.Monad (forever, when)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as L
import qualified Data.HashMap.Strict as H
import           Data.Hashable (Hashable)
import           Data.Maybe (fromMaybe, isNothing)
import           Data.Monoid ((<>))
import           Data.Monoid (mempty)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.UUID as UUID
import           System.Random (randomIO)

import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import           Data.Serialize.Get
import           Data.Serialize.Put

import qualified Data.Hadoop.Protobuf.Headers as P
import           Data.Hadoop.Types
import qualified Network.Hadoop.Stream as S
import           Network.Socket (Socket)

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

type CallId = Int

newtype ClientId = ClientId { unClientId :: ByteString }

------------------------------------------------------------------------

-- hadoop-2.1.0-beta is on version 9
-- see https://issues.apache.org/jira/browse/HADOOP-8990 for differences

data ConnectionState = ConnectionState
    { csStream        :: !S.Stream
    , csClientId      :: !ClientId
    , csCallId        :: !(TVar CallId)
    , csRecvCallbacks :: !(TVar (H.HashMap CallId (RawResponse -> IO ())))
    , csSendQueue     :: !(TQueue (Method, RawRequest, RawResponse -> IO ()))
    , csFatalError    :: !(TVar (Maybe SomeException))
    }

initConnectionV9 :: HadoopConfig -> Protocol -> Socket -> IO Connection
initConnectionV9 config@HadoopConfig{..} protocol sock = do
    csStream   <- S.mkSocketStream sock
    csClientId <- mkClientId

    S.runPut csStream $ do
        putByteString "hrpc"
        putWord8 9  -- version
        putWord8 0 -- rpc service class (0 = default/protobuf, 1 = built-in, 2 = writable, 3 = protobuf
        putWord8 0 -- auth protocol (0 = none, -33/0xDF = sasl)
        putMessage $ delimitedBytesL (rpcRequestHeaderProto csClientId (-3))
                  <> delimitedBytesL (contextProto protocol hcUser)

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

            return $ delimitedBytesL (rpcRequestHeaderProto csClientId callId)
                  <> delimitedBytesL (requestHeaderProto protocol method)
                  <> L.fromStrict requestBytes

        S.runPut csStream (putMessage bs)

    forkRecv :: ConnectionState -> IO ThreadId
    forkRecv cs@ConnectionState{..} = forkIO $ handle (onSocketError cs) $ forever $ do
        mget <- S.maybeGet csStream $ do
            n <- fromIntegral <$> getWord32be
            -- TODO Would be nice if we didn't have to isolate here
            -- TODO and could stream instead. We could stream if we
            -- TODO were able to read the varint length prefix
            -- TODO ourselves and keep track of how many bytes were
            -- TODO remaining instead of calling `getRemaining`.
            isolate n $ do
                hdr <- decodeLengthPrefixedMessage
                msg <- case getField (P.rspStatus hdr) of
                    P.Success -> Right <$> getRemaining
                    _         -> return . Left . SomeException $ rspError hdr
                return (hdr, msg)

        case mget of
          Nothing -> throwIO ConnectionClosed
          Just (hdr, msg) -> do
            onResponse <- fromMaybe (return $ return ())
                      <$> lookupDelete csRecvCallbacks (fromIntegral $ getField $ P.rspCallId hdr)

            onResponse msg

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

mkClientId :: IO ClientId
mkClientId = ClientId . L.toStrict . UUID.toByteString <$> randomIO

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

contextProto :: Protocol -> User -> P.IpcConnectionContext
contextProto protocol user = P.IpcConnectionContext
    { P.ctxProtocol = putField (Just (prName protocol))
    , P.ctxUserInfo = putField (Just P.UserInformation
        { P.effectiveUser = putField (Just user)
        , P.realUser      = mempty
        })
    }

rpcRequestHeaderProto :: ClientId -> CallId -> P.RpcRequestHeader
rpcRequestHeaderProto clientId callId = P.RpcRequestHeader
    { P.reqKind       = putField (Just P.ProtocolBuffer)
    , P.reqOp         = putField (Just P.FinalPacket)
    , P.reqCallId     = putField (fromIntegral callId)
    , P.reqClientId   = putField (unClientId clientId)
    , P.reqRetryCount = putField (Just (-1))
    }

requestHeaderProto :: Protocol -> Method -> P.RequestHeader
requestHeaderProto protocol method = P.RequestHeader
    { P.reqMethodName      = putField method
    , P.reqProtocolName    = putField (prName protocol)
    , P.reqProtocolVersion = putField (fromIntegral (prVersion protocol))
    }

rspError :: P.RpcResponseHeader -> RemoteError
rspError rsp = RemoteError (fromMaybe "unknown error" $ getField $ P.rspExceptionClassName rsp)
                           (fromMaybe "unknown error" $ getField $ P.rspErrorMsg rsp)

putMessage :: L.ByteString -> Put
putMessage body = do
    putWord32be (fromIntegral (L.length body))
    putLazyByteString body

getRemaining :: Get ByteString
getRemaining = do
    n <- remaining
    getByteString n

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
invokeAsync Connection{..} method arg k = invokeRaw method (delimitedBytes arg) k'
  where
    k' (Left err) = k (Left err)
    k' (Right bs) = k (fromDelimitedBytes bs)

delimitedBytes :: Encode a => a -> ByteString
delimitedBytes = runPut . encodeLengthPrefixedMessage

delimitedBytesL :: Encode a => a -> L.ByteString
delimitedBytesL = L.fromStrict . delimitedBytes

fromDelimitedBytes :: Decode a => ByteString -> Either SomeException a
fromDelimitedBytes bs = case runGetState decodeLengthPrefixedMessage bs 0 of
    Left err      -> decodeError (T.pack err)
    Right (x, "") -> Right x
    Right (_, _)  -> decodeError "decoded response but did not consume enough bytes"
  where
    decodeError = Left . SomeException . DecodeError
