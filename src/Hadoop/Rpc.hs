{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}

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

import           Control.Applicative ((<$>), (<*>))
import           Control.Exception (Exception, bracket, throwIO)
import           Control.Monad.IO.Class (liftIO)

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Data (Data)
import           Data.IORef
import           Data.Monoid ((<>), mempty)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Typeable (Typeable)
import           Data.Word (Word64)

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
mkRemote protocol ver method arg = invoke (RemoteCall protocol ver method (toBytes arg) fromBytes)

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

login :: User -> Remote ()
login user = sourcePut (putContext context)
  where
    context = IpcConnectionContext
        { ctxProtocol = putField (Just "hadoop-hs")
        , ctxUserInfo = putField (Just UserInformation
            { effectiveUser = putField (Just user)
            , realUser      = mempty
            })
        }

------------------------------------------------------------------------

invoke :: RemoteCall a -> Remote a
invoke rpc = do
    sourcePut (putRequest header request)
    response <- sinkGet decodeLengthPrefixedMessage
    case get rspStatus response of
      Success -> sinkGet (rpcDecode rpc <$> getResponse) >>= throwLeft
      _       -> sinkGet getError >>= liftIO . throwIO
  where
    throwLeft (Left err) = liftIO (throwIO err)
    throwLeft (Right x)  = return x

    header = RpcRequestHeader
        { reqKind       = putField (Just ProtocolBuffer)
        , reqOp         = putField (Just FinalPacket)
        , reqCallId     = putField 1
        }

    request = RpcRequest
        { reqMethodName      = putField (rpcMethodName rpc)
        , reqBytes           = putField (Just (rpcBytes rpc))
        , reqProtocolName    = putField (rpcProtocolName rpc)
        , reqProtocolVersion = putField (rpcProtocolVersion rpc)
        }

------------------------------------------------------------------------

-- hadoop-2.1.0-beta is on version 9
-- see https://issues.apache.org/jira/browse/HADOOP-8990 for differences

putContext :: IpcConnectionContext -> Put
putContext ctx = do
    putByteString "hrpc"
    putWord8 7  -- version
    putWord8 80 -- auth method (80 = simple, 81 = kerberos/gssapi, 82 = token/digest-md5)
    putWord8 0  -- ipc serialization type (0 = protobuf)
    putBlob (toBytes ctx)

putRequest :: RpcRequestHeader -> RpcRequest -> Put
putRequest hdr req = putBlob (toLPBytes hdr <> toLPBytes req)

putBlob :: ByteString -> Put
putBlob bs = do
    putWord32be (fromIntegral (B.length bs))
    putByteString bs

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

toBytes :: Encode a => a -> ByteString
toBytes = runPut . encodeMessage

toLPBytes :: Encode a => a -> ByteString
toLPBytes = runPut . encodeLengthPrefixedMessage

fromBytes :: Decode a => ByteString -> Either RemoteError a
fromBytes bs = case runGetState decodeMessage bs 0 of
    Left err      -> Left (RemoteError "fromBytes" (T.pack err))
    Right (x, "") -> Right x
    Right (_, _)  -> Left (RemoteError "fromBytes" "decoded response but did not consume enough bytes")

get :: HasField a => (t -> a) -> t -> FieldType a
get f x = getField (f x)
