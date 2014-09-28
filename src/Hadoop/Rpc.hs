{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Hadoop.Rpc
    ( Remote
    , RemoteError(..)

    , NameNode
    , SocksProxy
    , Endpoint(..)
    , User
    , HostName
    , Port

    , runTcp
    , runSocks
    , runSocket

    , CreateParent
    , Recursive

    , getListing
    , getFileInfo
    , getContentSummary
    , mkdirs
    , delete
    , rename
    ) where

import           Control.Applicative ((<$>))
import           Control.Exception (Exception, bracket, throwIO)
import           Control.Monad.IO.Class (liftIO)

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as L
import           Data.Data (Data)
import           Data.IORef
import           Data.Maybe (fromMaybe)
import           Data.Monoid ((<>), mempty)
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Typeable (Typeable)
import qualified Data.UUID as UUID
import           Data.Word (Word64)
import           System.IO.Unsafe (unsafePerformIO)
import           System.Random (randomIO)

import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import           Data.Serialize.Get
import           Data.Serialize.Put

import           Data.Conduit
import           Data.Conduit.Cereal
import           Data.Conduit.Network
import           Network (PortID(PortNumber))
import           Network.Simple.TCP hiding (HostName)
import           Network.Socks5 (defaultSocksConf, socksConnectWith)

import           Hadoop.Protobuf.ClientNameNode
import           Hadoop.Protobuf.Hdfs
import           Hadoop.Protobuf.Headers

------------------------------------------------------------------------

type Remote a = ConduitM ByteString ByteString IO a

data RemoteCall a = RemoteCall
    { rpcProtocolName    :: Text
    , rpcProtocolVersion :: Word64
    , rpcMethodName      :: Text
    , rpcBytes           :: ByteString
    , rpcDecode          :: ByteString -> Either RemoteError a
    }

data RemoteError = RemoteError Text Text
    deriving (Show, Eq, Data, Typeable)

instance Exception RemoteError

------------------------------------------------------------------------

type CreateParent = Bool
type Recursive = Bool
type Overwrite = Bool

------------------------------------------------------------------------

type User     = Text
type HostName = Text
type Port     = Int

type NameNode   = Endpoint
type SocksProxy = Endpoint

data Endpoint = Endpoint
    { epHost :: HostName
    , epPort :: Port
    } deriving (Eq, Ord, Show)

------------------------------------------------------------------------

runTcp :: NameNode -> User -> Remote a -> IO a
runTcp nameNode user remote = connect host port (\(s,_) -> runSocket s user remote)
  where
    host = T.unpack (epHost nameNode)
    port = show (epPort nameNode)

runSocks :: SocksProxy -> NameNode -> User -> Remote a -> IO a
runSocks proxy nameNode user remote =
    bracket (socksConnectWith proxyConf host port) closeSock (\s -> runSocket s user remote)
  where
    proxyConf = defaultSocksConf (T.unpack $ epHost proxy)
                                 (fromIntegral $ epPort proxy)

    host = T.unpack (epHost nameNode)
    port = PortNumber (fromIntegral $ epPort nameNode)

runSocket :: Socket -> User -> Remote a -> IO a
runSocket sock user remote = do
    -- TODO Must be a better way to do this
    ref <- newIORef (error "_|_")
    let runWrite = login user >> remote >>= liftIO . atomicWriteIORef ref
    sourceSocket sock =$= runWrite $$ sinkSocket sock
    readIORef ref

------------------------------------------------------------------------

mkRemote :: (Decode b, Encode a) => Text -> Word64 -> Text -> a -> Remote b
mkRemote protocol ver method arg =
    invoke (RemoteCall protocol ver method (delimitedBytes arg) fromDelimtedBytes)

hdfs :: (Decode b, Encode a) => Text -> a -> Remote b
hdfs = mkRemote "org.apache.hadoop.hdfs.protocol.ClientProtocol" 1

getListing :: FilePath -> Remote (Maybe DirectoryListing)
getListing path = get lsDirList <$> hdfs "getListing" GetListingRequest
    { lsSrc          = putField (T.pack path)
    , lsStartAfter   = putField ""
    , lsNeedLocation = putField False
    }

getFileInfo :: FilePath -> Remote (Maybe FileStatus)
getFileInfo path = get fiFileStatus <$> hdfs "getFileInfo" GetFileInfoRequest
    { fiSrc = putField (T.pack path)
    }

getContentSummary :: FilePath -> Remote ContentSummary
getContentSummary path = get csSummary <$> hdfs "getContentSummary" GetContentSummaryRequest
    { csPath = putField (T.pack path)
    }

mkdirs :: FilePath -> CreateParent -> Remote Bool
mkdirs path createParent = get mdResult <$> hdfs "mkdirs" MkdirsRequest
    { mdSrc          = putField (T.pack path)
    , mdMasked       = putField (FilePermission (putField 0o755))
    , mdCreateParent = putField createParent
    }

delete :: FilePath -> Recursive -> Remote Bool
delete path recursive = get dlResult <$> hdfs "delete" DeleteRequest
    { dlSrc       = putField (T.pack path)
    , dlRecursive = putField recursive
    }

rename :: FilePath -> FilePath -> Overwrite -> Remote ()
rename src dst overwrite = ignore <$> hdfs "rename2" Rename2Request
    { mvSrc       = putField (T.pack src)
    , mvDst       = putField (T.pack dst)
    , mvOverwrite = putField overwrite
    }
  where
    ignore :: Rename2Response -> ()
    ignore = const ()

------------------------------------------------------------------------

-- TODO remove hack when we have our own monad
clientId :: ByteString
clientId = unsafePerformIO $ L.toStrict . UUID.toByteString <$> liftIO randomIO
{-# NOINLINE clientId #-}

login :: User -> Remote ()
login user = sourcePut $ do
    putByteString "hrpc"
    putWord8 9 -- version
    putWord8 0 -- rpc service class (0 = default/protobuf, 1 = built-in, 2 = writable, 3 = protobuf)
    putWord8 0 -- auth protocol (0 = none, -33/0xDF = sasl)
    putMessage $ delimitedBytesL header
              <> delimitedBytesL context
  where
    -- call-id must be -3 for the first request (the one with the
    -- IpcConnectionContext) after that set it to 0 and increment.
    header = RpcRequestHeader
        { reqKind       = putField (Just ProtocolBuffer)
        , reqOp         = putField (Just FinalPacket)
        , reqCallId     = putField (-3)
        , reqClientId   = putField clientId
        , reqRetryCount = putField (Just (-1))
        }

    context = IpcConnectionContext
        { ctxProtocol = putField (Just "org.apache.hadoop.hdfs.protocol.ClientProtocol")
        , ctxUserInfo = putField (Just UserInformation
            { effectiveUser = putField (Just user)
            , realUser      = mempty
            })
        }

------------------------------------------------------------------------

invoke :: RemoteCall a -> Remote a
invoke RemoteCall{..} = do
    sourcePut $ putMessage $ delimitedBytesL rpcRequestHeader
                          <> delimitedBytesL requestHeader
                          <> L.fromStrict rpcBytes
    x <- sinkGet $ do
      size <- fromIntegral <$> getWord32be
      isolate size $ do
        rsp <- decodeLengthPrefixedMessage
        case get rspStatus rsp of
          Success -> rpcDecode <$> getRemaining
          _       -> return (Left (rspError rsp))

    throwLeft x
  where
    throwLeft (Left err) = liftIO (throwIO err)
    throwLeft (Right x)  = return x

    rspError rsp = RemoteError (fromMaybe "unknown error" $ get rspExceptionClassName rsp)
                               (fromMaybe "unknown error" $ get rspErrorMsg rsp)

    rpcRequestHeader = RpcRequestHeader
        { reqKind       = putField (Just ProtocolBuffer)
        , reqOp         = putField (Just FinalPacket)
        , reqCallId     = putField 0
        , reqClientId   = putField clientId
        , reqRetryCount = putField (Just (-1))
        }

    requestHeader = RequestHeader
        { reqMethodName      = putField rpcMethodName
        , reqProtocolName    = putField rpcProtocolName
        , reqProtocolVersion = putField rpcProtocolVersion
        }

------------------------------------------------------------------------

-- hadoop-2.1.0-beta is on version 9
-- see https://issues.apache.org/jira/browse/HADOOP-8990 for differences

putMessage :: L.ByteString -> Put
putMessage body = do
    putWord32be (fromIntegral (L.length body))
    putLazyByteString body

getRemaining :: Get ByteString
getRemaining = do
    n <- remaining
    getByteString n

delimitedBytes :: Encode a => a -> ByteString
delimitedBytes = runPut . encodeLengthPrefixedMessage

delimitedBytesL :: Encode a => a -> L.ByteString
delimitedBytesL = L.fromStrict . delimitedBytes

fromDelimtedBytes :: Decode a => ByteString -> Either RemoteError a
fromDelimtedBytes bs = case runGetState decodeLengthPrefixedMessage bs 0 of
    Left err      -> Left (RemoteError "fromDelimtedBytes" (T.pack err))
    Right (x, "") -> Right x
    Right (_, _)  -> Left (RemoteError "fromDelimtedBytes" "decoded response but did not consume enough bytes")

get :: HasField a => (t -> a) -> t -> FieldType a
get f x = getField (f x)
