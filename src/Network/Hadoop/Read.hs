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

import           Control.Applicative ((<$>))
import           Control.Exception (SomeException, throwIO)
import           Control.Monad (foldM, when)
import           Control.Monad.Catch (MonadMask, catch)
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Trans.State
import           Control.Monad.Trans.Class (lift)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Maybe (fromMaybe)
import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Word (Word64)

import qualified Data.Serialize.Get as Get
import qualified Data.Serialize.Put as Put

import           Data.Hadoop.Protobuf.ClientNameNode
import           Data.Hadoop.Protobuf.DataTransfer
import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs
import           Network.Hadoop.Rpc
import qualified Network.Hadoop.Socket as S
import qualified Network.Hadoop.Stream as Stream

import           Prelude hiding (rem)

------------------------------------------------------------------------

data HdfsReadHandle = HdfsReadHandle (Maybe SocksProxy) LocatedBlocks

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
        foldM (flip (execStateT . procBlock f proxy len)) acc0 (getField $ lbBlocks l)

procBlock :: (MonadIO m, MonadMask m) =>
    (a -> ByteString -> m a) -> Maybe SocksProxy -> Word64 -> LocatedBlock -> StateT a m ()
procBlock f proxy blockSize block = do
        let extended = getField . lbExtended $ block
            token = getField . lbToken $ block
        case getField . lbLocations $ block of
            [] -> error $ "No locations for block " ++ show extended
            ls -> failover (error $ "All locations failed for block " ++ show extended)
                           (map (getLoc extended token) ls)
  where
    failover err [] = err
    failover err (x:xs) = catch x handler
      where handler (_ :: SomeException) = failover err xs

    getLoc extended token l = do
        let i = getField (dnId l)
            host = getField . dnHostName $ i
            port = getField . dnXferPort $ i
            endpoint = Endpoint host (fromIntegral port)
        runBlock endpoint 0 extended token

    runBlock endpoint offset extended token = do
        let len = fromMaybe blockSize . getField . ebNumBytes $ extended
        S.bracketSocket proxy endpoint $ readBlock offset len extended token

    readBlock offset0 len extended token sock = go 0 offset0 len
      where
        go nread offset rem = do
            len' <- readBlockPart offset rem extended token sock
            let offset' = offset + len'
                nread' = nread + len'
                rem' = rem - len'
            when (rem' > 0) $ go nread' offset' rem'

    readBlockPart offset rem extended token sock = do
        stream <- liftIO $ Stream.mkSocketStream sock
        b <- liftIO $ do
            Stream.runPut stream putReadRequest
            Stream.runGet stream decodeLengthPrefixedMessage
        readPackets b rem stream
      where

        putReadRequest = do
          Put.putWord16be 28 -- Data Transfer Protocol Version, 2 bytes
          Put.putWord8 81    -- READ_BLOCK
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

    readPackets BlockOpResponse{..} len stream = go 0 len
      where
        go nread rem = do
            len' <- readPacket (b :: Maybe Integer) stream
            let rem' = rem - len'
                nread' = nread + len'
            if rem' > 0 then go nread' rem' else return nread'

        m = getField borReadOpChecksumInfo
        c = getField . rociChecksum <$> m
        b = fromIntegral . getField . csBytesPerChecksum <$> c

    readPacket bytesPerChecksum stream = do
        (dataLen, d) <- liftIO $ do
            _len <- Stream.runGet stream Get.getWord32be
            sz <- Stream.runGet stream Get.getWord16be
            bs <- Stream.runGet stream $ Get.getByteString (fromIntegral sz)
            ph <- decodeBytes bs
            let numChunks = countChunks ph
                dataLen = fromIntegral . getField . phDataLen $ ph

            _ <- Stream.runGet stream (Get.getByteString (4*numChunks))
            (fromIntegral dataLen :: Integer,) <$>
                Stream.runGet stream (Get.getByteString dataLen)
        get >>= \x -> lift (f x d) >>= put
        return (fromIntegral dataLen)
      where
        countChunks :: PacketHeader -> Int
        countChunks PacketHeader{..} = (dataLen + b - 1) `div` b
          where
            b = fromIntegral $ fromMaybe 512 bytesPerChecksum
            dataLen = fromIntegral $ getField phDataLen

    decodeBytes bs = case Get.runGetState decodeMessage bs 0 of
        Left err      -> throwIO (RemoteError "DecodeError" (T.pack err))
        Right (x, "") -> return x
        Right (_, _)  -> throwIO (RemoteError "DecodeError" "decoded response but did not consume enough bytes")
