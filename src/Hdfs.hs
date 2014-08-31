{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# OPTIONS_GHC -w #-}

module Main (main) where

import           Control.Applicative ((<$>), (<*>))
import           Control.Exception (bracket)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import           Data.Maybe (fromJust)
import           Data.Monoid ((<>), mempty)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word (Word16, Word32)

import           Network (PortID(..), HostName, PortNumber, withSocketsDo, connectTo)
import           System.Environment (getArgs)
import           System.IO (Handle, BufferMode(..), hSetBuffering, hSetBinaryMode, hClose)
import           Text.Printf (printf)

import           Data.Binary.Get
import           Data.Binary.Put
import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import qualified Data.Serialize as Cereal

import           Hadoop.Messages.ClientNameNode
import           Hadoop.Messages.Hdfs
import           Hadoop.Messages.Headers

import qualified Data.HashMap.Strict as H
import           Data.ProtocolBuffers.Internal

------------------------------------------------------------------------

main :: IO ()
main = do
    [path] <- getArgs
    withConnectTo "hadoop1" 8020 $ \h -> do
        putStrLn "hdfs connected"

        let bs = runPut (putRequest reqCtx reqHdr (req (T.pack path)))
        L.hPut h bs
        putStrLn "sent request"

        bs <- B.hGetSome h 4096
        putStrLn $ "got response (" ++ show (B.length bs) ++ " bytes)"

        let (Right (rsp, bs')) = fromLPBytes bs
        if getField (rspStatus rsp) == RpcSuccess
           then do
             let rsp = runGet getResponse (L.fromStrict bs') :: GetListingResponse
             putStrLn $ "decoded response"

             let xs = getField . dlPartialListing
                    . fromJust . getField . glDirList
                    $ rsp

             print $ map (getField . fsPath) xs
             print $ map (getField . fsLength) xs

             let phex x = printf "%0o" (fromIntegral x :: Word16) :: String
             print $ map (phex . getField . fpPerm . getField . fsPermission) xs

             print $ map (getField . fsOwner) xs
             print $ map (getField . fsGroup) xs

             let hdfs2utc ms = posixSecondsToUTCTime (fromIntegral ms / 1000)
             print $ map (hdfs2utc . getField . fsModificationTime) xs
           else do
             let (cls, stk) = runGet getError (L.fromStrict bs')
             T.putStrLn cls
             T.putStrLn stk
  where
    reqCtx = IpcConnectionContext
        { ctxProtocol = putField (Just "haskell")
        , ctxUserInfo = putField (Just UserInformation
            { effectiveUser = putField (Just "cloudera")
            , realUser      = mempty
            })
        }

    reqHdr = RpcRequestHeader
        { reqKind       = putField (Just ProtocolBuffer)
        , reqOp         = putField (Just FinalPacket)
        , reqCallId     = putField 12345
        }

    req src = RpcRequest
        { reqMethodName      = putField "getListing"
        , reqBytes           = putField $ Just $ toBytes GetListingRequest
            { glSrc          = putField src
            , glStartAfter   = putField ""
            , glNeedLocation = putField True
            }
        , reqProtocolName    = putField "org.apache.hadoop.hdfs.protocol.ClientProtocol"
        , reqProtocolVersion = putField 1
        }

withConnectTo :: HostName -> PortNumber -> (Handle -> IO a) -> IO a
withConnectTo host port = bracket connect hClose
  where
    connect = withSocketsDo $ do
      h <- connectTo host (PortNumber port)
      --hSetBuffering h (BlockBuffering (Just 4096))
      hSetBuffering h NoBuffering
      hSetBinaryMode h True
      return h

-- TODO Handle SIGPIPE
-- import Posix
-- main = installHandler sigPIPE Ignore Nothing

------------------------------------------------------------------------

-- hadoop-2.1.0-beta is on version 9
-- see https://issues.apache.org/jira/browse/HADOOP-8990 for differences

putRequest :: IpcConnectionContext -> RpcRequestHeader -> RpcRequest -> Put
putRequest ctx hdr req = do
    putByteString "hrpc"
    putWord8 7  -- version
    putWord8 80 -- auth method (80 = simple, 81 = kerberos/gssapi, 82 = token/digest-md5)
    putWord8 0  -- ipc serialization type (0 = protobuf)

    let bs = toBytes ctx
    putWord32be (fromIntegral (B.length bs))
    putByteString bs

    let bs' = toLPBytes hdr <> toLPBytes req
    putWord32be (fromIntegral (B.length bs'))
    putByteString bs'

getResponse :: Decode a => Get a
getResponse = do
    n <- fromIntegral <$> getWord32be
    bs <- getByteString n
    case fromBytes bs of
        Left err      -> fail $ "getResponse: " ++ err
        Right (x, "") -> return x
        Right (_, _)  -> fail $ "getResponse: decoded response but did not consume enough bytes"

getError :: Get (Text, Text)
getError = (,) <$> getText <*> getText
  where
    getText = do
        n <- fromIntegral <$> getWord32be
        T.decodeUtf8 <$> getByteString n

toBytes :: Encode a => a -> ByteString
toBytes = Cereal.runPut . encodeMessage

toLPBytes :: Encode a => a -> ByteString
toLPBytes = Cereal.runPut . encodeLengthPrefixedMessage

fromBytes :: Decode a => ByteString -> Either String (a, ByteString)
fromBytes bs = Cereal.runGetState decodeMessage bs 0

fromLPBytes :: Decode a => ByteString -> Either String (a, ByteString)
fromLPBytes bs = Cereal.runGetState decodeLengthPrefixedMessage bs 0
