{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Hadoop.Read
    ( HdfsReadHandle
    , openRead
    , hdfsMapM_

    -- * Convenience utilities
    , hdfsCat
    ) where

import           Control.Applicative (Applicative(..), (<$>))
import           Control.Exception (throwIO)
import           Control.Monad (guard)
import           Control.Monad.IO.Class (MonadIO(..))
import           Data.Attoparsec.Text (Parser, char, decimal, parseOnly)
import           Data.Bits
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.List (intercalate)
import           Data.Maybe (fromMaybe)
import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import qualified Data.Text as T
import           Data.Word (Word32, Word64)

import           Data.Serialize.Get
import           Data.Serialize.Put

import           Data.Hadoop.Protobuf.ClientNameNode
import           Data.Hadoop.Protobuf.DataTransfer
import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Protobuf.Security
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs
import           Network.Hadoop.Rpc
import qualified Network.Hadoop.Socket as S
import           Network.Hadoop.Stream (Stream)
import qualified Network.Hadoop.Stream as Stream

------------------------------------------------------------------------

data HdfsReadHandle = HdfsReadHandle
    { readProxy     :: Maybe SocksProxy
    , readLocations :: LocatedBlocks
    }

-- | Open an HDFS path for reading.
-- This retrieves the block locations for an HDFS path, but does not
-- yet initiate any data transfers.
openRead :: FilePath -> Hdfs (Maybe HdfsReadHandle)
openRead path = do
    locs <- getField . locsLocations <$> hdfsInvoke "getBlockLocations" GetBlockLocationsRequest
        { catSrc    = putField (T.pack path)
        , catOffset = putField 0
        , catLength = putField maxBound
        }
    case locs of
        Nothing -> return Nothing
        Just ls -> do
            proxy <- hcProxy . cnConfig <$> getConnection
            return $ Just (HdfsReadHandle proxy ls)

-- | Print the contents of a file on HDFS to stdout
hdfsCat :: FilePath -> Hdfs ()
hdfsCat path = do
    h <- openRead path
    liftIO $ maybe (return ()) (hdfsMapM_ B.putStr) h

-- | Map an action over the contents of a file on HDFS.
hdfsMapM_ :: (ByteString -> IO ()) -> HdfsReadHandle -> IO ()
hdfsMapM_ f (HdfsReadHandle proxy l) = do
        let len = getField . lbFileLength $ l
        mapM_ (showBlock proxy len) (getField . lbBlocks $ l)
  where
    showBlock proxy len b = do
        let extended = getField . lbExtended $ b
            token = getField . lbToken $ b
        mapM_ (showLoc proxy len extended token) (getField . lbLocations $ b)
    showLoc proxy len extended token l = do
        let i = getField (dnId l)
            Right addr = parseOnly parseIPv4 . getField . dnIpAddr $ i
            host = getField . dnHostName $ i
            port = getField . dnXferPort $ i
        let endpoint = Endpoint host (fromIntegral port)
        catBlock proxy endpoint 0 len extended token

    catBlock :: Maybe SocksProxy -> Endpoint -> Word64 -> Word64 -> ExtendedBlock -> Token -> IO ()
    catBlock proxy endpoint offset len0 extended token = do
        let len = fromMaybe len0 . getField . ebNumBytes $ extended
        S.runTcp proxy endpoint $ readBlocks offset len extended token

    readBlocks :: Word64 -> Word64 -> ExtendedBlock -> Token -> S.Socket -> IO ()
    readBlocks offset len extended token sock = go 0 offset len
      where
        go nread0 offset0 rem0 = do
            len <- readBlock offset0 rem0 extended token sock
            let offset = offset0 + len
                nread = nread0 + len
                rem = rem0 - len
            if rem > 0 then go nread offset rem else return ()

    readBlock :: Word64 -> Word64 -> ExtendedBlock -> Token -> S.Socket -> IO Word64
    readBlock offset rem extended token sock = do
        stream <- Stream.mkSocketStream sock
        Stream.runPut stream putReadRequest
        b <- Stream.runGet stream decodeLengthPrefixedMessage
        readPackets b rem stream
      where

        putReadRequest = do
          putWord16be 28 -- Data Transfer Protocol Version, 2 bytes
          putWord8 81    -- READ_BLOCK
          encodeLengthPrefixedMessage opReadBlock

        opReadBlock = OpReadBlock
            { orbHeader = putField coh
            , orbOffset = putField offset
            , orbLen    = putField rem
            , orbSendChecksums   = putField Nothing
            , orbCachingStrategy = putField Nothing
            }
        coh = ClientOperationHeader
            { cohBaseHeader = putField bh
            , cohClientName = putField (T.pack "hh")
            }
        bh = BaseHeader
            { bhBlock = putField extended
            , bhToken = putField (Just token)
            }

        showBlockOpResponse :: BlockOpResponse -> String
        showBlockOpResponse = show

    readPackets :: BlockOpResponse -> Word64 -> Stream -> IO Word64
    readPackets BlockOpResponse{..} len stream = go 0 len
      where
        go nread0 rem0 = do
            len <- readPacket b rem0 stream
            let rem = rem0 - len
                nread = nread0 + len
            if rem > 0 then go nread rem else return nread

        m = getField borReadOpChecksumInfo
        c = getField . rociChecksum <$> m
        b = fromIntegral . getField . csBytesPerChecksum <$> c

    readPacket :: Maybe Word32 -> Word64 -> Stream -> IO Word64
    readPacket bytesPerChecksum remaining stream = do
        len <- Stream.runGet stream getWord32be
        sz <- Stream.runGet stream getWord16be

        bs <- Stream.runGet stream $ getByteString (fromIntegral sz)
        ph <- decodeBytes bs
        let numChunks = countChunks ph
            dataLen = fromIntegral . getField . phDataLen $ ph

        _ <- Stream.runGet stream (getByteString (4*numChunks))
        d <- Stream.runGet stream (getByteString dataLen)

        liftIO . f $ d
        return (fromIntegral dataLen)
      where
        showPacketHeader :: PacketHeader -> String
        showPacketHeader = show

        countChunks :: PacketHeader -> Int
        countChunks PacketHeader{..} = (dataLen + b - 1) `div` b
          where
            b = fromIntegral $ fromMaybe 512 bytesPerChecksum
            dataLen = fromIntegral $ getField phDataLen

    decodeBytes bs = case runGetState decodeMessage bs 0 of
        Left err      -> throwIO (RemoteError "DecodeError" (T.pack err))
        Right (x, "") -> return x
        Right (_, _)  -> throwIO (RemoteError "DecodeError" "decoded response but did not consume enough bytes")

------------------------------------------------------------------------

parseIPv4 :: Parser Word32
parseIPv4 = d >>= dd >>= dd >>= dd
  where
    d = do
      x <- decimal
      guard $ x < 256
      return x
    dd acc = do
      x <- char '.' *> d
      return $ (acc `shiftL` 8) .|. x

showIPv4 :: Word32 -> String
showIPv4 x = intercalate "." . map show $ [a,b,c,d]
  where
      a = (x .&. 0xFF000000) `shiftR` 24
      b = (x .&. 0xFF0000) `shiftR` 16
      c = (x .&. 0xFF00) `shiftR` 8
      d = (x .&. 0xFF)
