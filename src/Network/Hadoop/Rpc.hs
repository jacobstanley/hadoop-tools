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
import           Control.Monad.IO.Class (liftIO)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as L
import qualified Data.HashMap.Strict as H
import           Data.Hashable (Hashable)
import           Data.Maybe (fromMaybe, isNothing)
import           Data.Monoid ((<>))
import           Data.Monoid (mempty)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.UUID as UUID
import           System.Random (randomIO)

import qualified Network.Protocol.SASL.GNU as GS

import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import           Data.Serialize.Get
import           Data.Serialize.Put

import           Data.Hadoop.Configuration
import qualified Data.Hadoop.Protobuf.Headers as P
import           Data.Hadoop.Types
import           Network.Hadoop.Sasl
import qualified Network.Hadoop.Stream as S
import           Network.Hadoop.Types
import           Network.Socket (Socket)

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

initConnectionV9 :: HadoopConfig -> NameNode -> Protocol -> Socket -> IO Connection
initConnectionV9 config@HadoopConfig{..} nameNode protocol sock = do
    csStream   <- S.mkSocketStream sock
    csClientId <- mkClientId

    S.runPut csStream $ do
        putByteString "hrpc"
        putWord8 9  -- version
        putWord8 0 -- rpc service class (0 = default/protobuf, 1 = built-in, 2 = writable, 3 = protobuf
        -- auth protocol (0 = none, -33/0xDF = sasl)
        putWord8 $ maybe 0 (const 0xDF) (nnPrincipal nameNode)

    case nnPrincipal nameNode of
        Nothing -> return ()
        Just principal -> saslAuth principal csStream

    S.runPut csStream $
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
    invokeSasl :: (Encode a, Decode b) => S.Stream -> a -> IO b
    invokeSasl stream msgr = do
        let message = delimitedBytesL (rpcRequestHeaderProto (ClientId "") (-33))
                <> delimitedBytesL msgr

        S.runPut stream (putMessage message)
        mget <- S.maybeGet stream $ do
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
            Just (_, msg) ->
                case msg of
                    Left ex -> throwIO ex
                    Right m -> case fromDelimitedBytes m of
                        Left ex -> throwIO ex
                        Right x -> return x

    saslAuth :: Principal -> S.Stream -> IO ()
    saslAuth Principal{..} c = do
        negResp <- liftIO $ invokeSasl c RpcSaslProto
            { spState = putField Negotiate
            , spVersion = mempty
            , spToken = mempty
            , spAuths = mempty
            }
        case getField $ spState negResp of
            Negotiate -> return ()
            _  -> throwIO $ SomeException FailedNegotiation

        let auths = getField $ spAuths negResp
            authsMap = zip (map (getField . saMechanism) auths) auths

        res <- GS.runSASL $ do
            -- Select a common mechanism
            mSelectedMech <- GS.clientSuggestMechanism $ map (GS.Mechanism . fst) authsMap
            GS.Mechanism selectedMech <- liftIO $
                maybe (throwIO $ SomeException NoSharedMechanism) return mSelectedMech
            -- Safe because `selectedMech` was selected from one of the keys in `authsMap`
            let Just selectedAuth = lookup selectedMech authsMap
            GS.runClient (GS.Mechanism selectedMech) $ do
                GS.setProperty GS.PropertyService $ T.encodeUtf8 pService
                GS.setProperty GS.PropertyHostname $ T.encodeUtf8 pHost
                GS.setProperty GS.PropertyAuthID $ T.encodeUtf8 (authUser hcUser)
                (token,_) <- GS.step ""
                r <- liftIO $ invokeSasl c RpcSaslProto
                    { spState = putField Initiate
                    , spVersion = mempty
                    , spToken = putField $ Just token
                    , spAuths = putField $ [selectedAuth]
                    }
                saslAuthLoop c r

        case res of
            Left err -> throwIO $ SomeException $ SaslException err
            Right _ -> return ()

    saslAuthLoop :: S.Stream -> RpcSaslProto -> GS.Session ()
    saslAuthLoop c r = do
        case getField $ spState r of
            Challenge -> do
                let mToken = getField $ spToken r
                case mToken of
                    Nothing -> liftIO $ throwIO $ SomeException NoToken
                    Just token -> do
                        (resToken, _) <- GS.step token
                        resp <- liftIO $ invokeSasl c RpcSaslProto
                            { spToken = putField $ Just resToken
                            , spState = putField $ Response
                            , spVersion = mempty
                            , spAuths = mempty
                            }
                        saslAuthLoop c resp
            Success   -> return ()
            _         -> liftIO $ throwIO $ SomeException UnexptededState



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

contextProto :: Protocol -> UserDetails -> P.IpcConnectionContext
contextProto protocol user = P.IpcConnectionContext
    { P.ctxProtocol = putField (Just (prName protocol))
    , P.ctxUserInfo = putField (Just P.UserInformation
        { P.effectiveUser = putField (Just (authUser user))
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
