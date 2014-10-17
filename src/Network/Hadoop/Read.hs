{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Network.Hadoop.Read
    ( HdfsReadHandle
    , openRead
    , hdfsMapM_
    , hdfsFoldM

    -- * Convenience utilities
    , hdfsCat
    ) where

import           Control.Applicative (Applicative(..), (<$>))
import           Control.Exception (SomeException, throwIO)
import           Control.Monad (guard, foldM)
import           Control.Monad.Catch (MonadMask, catch)
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
import qualified Data.Text.Encoding as T
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
openRead :: HdfsPath -> Hdfs (Maybe HdfsReadHandle)
openRead path = do
    locs <- getField . locsLocations <$> hdfsInvoke "getBlockLocations" GetBlockLocationsRequest
        { catSrc    = putField (T.decodeUtf8 path)
        , catOffset = putField 0
        , catLength = putField maxBound
        }
    case locs of
        Nothing -> return Nothing
        Just ls -> do
            proxy <- hcProxy . cnConfig <$> getConnection
            return $ Just (HdfsReadHandle proxy ls)

-- | Print the contents of a file on HDFS to stdout
hdfsCat :: HdfsPath -> Hdfs ()
hdfsCat path = do
    h <- openRead path
    maybe (return ()) (hdfsMapM_ (liftIO . B.putStr)) h

-- | Map an action over the contents of a file on HDFS.
hdfsMapM_ :: (MonadIO m, MonadMask m) =>
    (ByteString -> m ()) -> HdfsReadHandle -> m ()
hdfsMapM_ f = hdfsFoldM (\_ x -> f x) ()

hdfsFoldM :: (MonadIO m, MonadMask m) =>
    (a -> ByteString -> m a) -> a -> HdfsReadHandle -> m a
hdfsFoldM f acc0 (HdfsReadHandle proxy l) = do
        let len = getField . lbFileLength $ l
        foldM (procBlock f proxy len) acc0 (getField . lbBlocks $ l)

procBlock :: (MonadIO m, MonadMask m) =>
    (a -> ByteString -> m a) -> Maybe SocksProxy -> Word64 -> a -> LocatedBlock -> m a
procBlock f proxy len acc0 b = do
        let extended = getField . lbExtended $ b
            token = getField . lbToken $ b
        case getField . lbLocations $ b of
            [] -> error $ "No locations for block " ++ show extended
            ls -> failover (error $ "All locations failed for block " ++ show extended)
                           (map (getLoc proxy len extended token) ls)
  where
    failover err [] = err
    failover err (x:xs) = catch x f
      where f (_ :: SomeException) = failover err xs

    getLoc proxy len extended token l = do
        let i = getField (dnId l)
            Right addr = parseOnly parseIPv4 . getField . dnIpAddr $ i
            host = getField . dnHostName $ i
            port = getField . dnXferPort $ i
        let endpoint = Endpoint host (fromIntegral port)
        runBlock proxy endpoint 0 len extended token

    runBlock proxy endpoint offset len0 extended token = do
        let len = fromMaybe len0 . getField . ebNumBytes $ extended
        S.bracketSocket proxy endpoint $ readBlock offset len extended token

    readBlock offset len extended token sock = go 0 offset len acc0
      where
        go nread0 offset0 rem0 acc = do
            (len, acc') <- readBlockPart offset0 rem0 extended token sock acc
            let offset = offset0 + len
                nread = nread0 + len
                rem = rem0 - len
            if rem > 0 then go nread offset rem acc' else return acc'

    readBlockPart offset rem extended token sock acc = do
        stream <- liftIO $ Stream.mkSocketStream sock
        b <- liftIO $ do
            Stream.runPut stream putReadRequest
            Stream.runGet stream decodeLengthPrefixedMessage
        readPackets b rem stream acc
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

    readPackets BlockOpResponse{..} len stream acc1 = go 0 len acc1
      where
        go nread0 rem0 acc = do
            (len, acc') <- readPacket b rem0 stream acc
            let rem = rem0 - len
                nread = nread0 + len
            if rem > 0 then go nread rem acc' else return (nread, acc')

        m = getField borReadOpChecksumInfo
        c = getField . rociChecksum <$> m
        b = fromIntegral . getField . csBytesPerChecksum <$> c

    readPacket bytesPerChecksum remaining stream acc = do
        (dataLen, d) <- liftIO $ do
            len <- Stream.runGet stream getWord32be
            sz <- Stream.runGet stream getWord16be

            bs <- Stream.runGet stream $ getByteString (fromIntegral sz)
            ph <- decodeBytes bs
            let numChunks = countChunks ph
                dataLen = fromIntegral . getField . phDataLen $ ph

            _ <- Stream.runGet stream (getByteString (4*numChunks))
            (fromIntegral dataLen,) <$>
                Stream.runGet stream (getByteString dataLen)

        acc' <- f acc d
        return (fromIntegral dataLen, acc')
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
