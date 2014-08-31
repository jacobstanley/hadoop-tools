{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# OPTIONS_GHC -w #-}

module Main (main) where

import           Control.Applicative ((<$>), (<*>), many)
import           Control.Monad (when, unless, replicateM, forever)
import           Control.Monad.Loops (unfoldM)
import           Data.Binary.Get
import           Data.Bits ((.|.), xor, shiftL)
import           Data.Int
import           Data.List (sort)
import           Data.Maybe (catMaybes)
import           Data.Monoid (Monoid, (<>), mempty, mconcat)
import qualified Data.Vector as V
import           Data.Word
import           Text.Printf (printf)

import           Control.Lens (zoom)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as L

import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T

import           System.Environment (getArgs)
import           System.IO (IOMode(..), withBinaryFile)

import           Pipes
import           Pipes.ByteString (fromHandle)
import qualified Pipes.Prelude as P

import qualified Codec.Compression.Snappy as Snappy

------------------------------------------------------------------------

main :: IO ()
main = do
    args <- getArgs
    case args of
      [path] -> hdshow path
      _      -> putStrLn "Usage: hdshow PATH"

hdshow :: FilePath -> IO ()
hdshow path =
    withBinaryFile path ReadMode $ \h -> do
    let p = fromHandle h :: Producer ByteString IO ()
    xs <- P.toListM (p >-> sequenceFile >-> P.map keys >-> P.take 1)
    print xs

------------------------------------------------------------------------

sequenceFile :: Monad m => Pipe ByteString RecordBlock m ()
sequenceFile = recordBlocks >-> P.map (decompressRecordBlock Snappy.decompress)

decompressRecordBlock :: (ByteString -> ByteString) -> CompressedRecordBlock -> RecordBlock
decompressRecordBlock codec CompressedRecordBlock{..} = RecordBlock{..}
  where
    keys   = decompressData keyLengths   cKeys
    values = decompressData valueLengths cValues

    keyLengths   = decompressLengths cKeyLengths
    valueLengths = decompressLengths cValueLengths

    decompressLengths = V.fromList . runGet (many getVInt) . decompressBlock
    decompressData ls = runGet (V.mapM getByteString ls) . decompressBlock

    decompressBlock CompressedBlock{..} = L.fromChunks (map codec compressedParts)

recordBlocks :: Monad m => Pipe ByteString CompressedRecordBlock m ()
recordBlocks = do
    (hdr, bs) <- feed (runGetIncremental getHeader)
    loop (runGetIncremental (getCompressedRecordBlock (sync hdr))) bs
  where
    feed decoder = do
        bs <- await
        case pushChunk decoder bs of
            Fail _ _ err -> error err
            Done bs' _ x -> return (x, bs')
            decoder'     -> feed decoder'

    loop decoder bs = forever $ do
        (x, bs') <- feed (decoder `pushChunk` bs)
        yield x
        loop decoder bs'

------------------------------------------------------------------------

data Header = Header
    { keyType         :: !Text
    , valueType       :: !Text
    , compressionType :: !Text
    , metadata        :: ![(Text, Text)]
    , sync            :: !MD5
    } deriving (Show)

data RecordBlock = RecordBlock
    { keys       :: !(V.Vector ByteString)
    , values     :: !(V.Vector ByteString)
    }

data CompressedRecordBlock = CompressedRecordBlock
    { cNumRecords   :: !Int
    , cKeyLengths   :: !CompressedBlock
    , cKeys         :: !CompressedBlock
    , cValueLengths :: !CompressedBlock
    , cValues       :: !CompressedBlock
    } deriving (Show)

data CompressedBlock = CompressedBlock
    { originalSize    :: !Int
    , compressedParts :: ![ByteString]
    } deriving (Show)

------------------------------------------------------------------------

getHeader :: Get Header
getHeader = do
    magic <- getByteString 3
    when (magic /= "SEQ")
         (fail "not a sequence file")

    version <- getWord8
    when (version /= 6)
         (fail $ "unknown version: " ++ show version)

    keyType   <- getText
    valueType <- getText

    compression      <- getBool
    blockCompression <- getBool

    unless (compression && blockCompression)
           (fail "only block compressed files supported")

    compressionType <- getText

    metadata <- getMetadata
    sync     <- getMD5

    return Header{..}

getMetadata :: Get [(Text, Text)]
getMetadata = do
    n <- fromIntegral <$> getWord32le
    replicateM n $ (,) <$> getText <*> getText

------------------------------------------------------------------------

getCompressedRecordBlock :: MD5 -> Get CompressedRecordBlock
getCompressedRecordBlock sync = label "record block" $ do
    escape <- getWord32le
    when (escape /= 0xffffffff)
         (fail $ "file corrupt, expected to find sync escape " ++
                 "<0xffffffff> but was " ++ printf "<0x%0x>" escape)

    sync' <- getMD5
    when (sync /= sync')
         (fail $ "file corrupt, expected to find sync marker " ++
                 "<" ++ show sync ++ "> but was <" ++ show sync' ++ ">")

    cNumRecords    <- getVInt
    cKeyLengths   <- label "key lengths"   getCompressedBlock
    cKeys         <- label "keys"          getCompressedBlock
    cValueLengths <- label "value lengths" getCompressedBlock
    cValues       <- label "values"        getCompressedBlock

    return CompressedRecordBlock{..}

getCompressedBlock :: Get CompressedBlock
getCompressedBlock = do
    size <- getVInt
    isolate size $ do
        originalSize    <- fromIntegral <$> getWord32be
        compressedParts <- catMaybes <$> untilEmpty getNext
        return CompressedBlock{..}
  where
    getNext = do
      n <- fromIntegral <$> getWord32be
      if n == 0 then return Nothing
                else Just <$> getByteString n

------------------------------------------------------------------------

isolateN :: [Int] -> Get a -> Get [a]
isolateN ns g = label "isolateN" $ mapM (`isolate` g) ns

runEmbeded :: Get a -> L.ByteString -> Get a
runEmbeded g lbs = label "runEmbeded" $ case runGetOrFail g lbs of
    Right (_, _, x)    -> return x
    Left (_, pos, msg) -> fail ("at sub-position " ++ show pos ++ ": " ++ msg)

------------------------------------------------------------------------

fromGet :: Get a -> ByteString -> a
fromGet g bs = runGet g (L.fromStrict bs)

getBool :: Get Bool
getBool = (/= 0) <$> getWord8

getBuffer :: Get ByteString
getBuffer = getByteString =<< getVInt

getText :: Get Text
getText = T.decodeUtf8 <$> getBuffer

getMD5 :: Get MD5
getMD5 = MD5 <$> getByteString 16

getVInt :: Get Int
getVInt = fromIntegral <$> getVInt64

getVInt64 :: Get Int64
getVInt64 = withFirst . fromIntegral =<< getWord8
  where
    withFirst :: Int8 -> Get Int64
    withFirst x | size == 1 = return (fromIntegral x)
                | otherwise = fixupSign . B.foldl' go 0 <$> getByteString (size - 1)
      where
        go :: Int64 -> Word8 -> Int64
        go i b = (i `shiftL` 8) .|. fromIntegral b

        size | x >= -112 = 1
             | x <  -120 = fromIntegral (-119 - x)
             | otherwise = fromIntegral (-111 - x)

        fixupSign v = if isNegative then v `xor` (-1) else v

        isNegative = x < -120 || (x >= -112 && x < 0)


untilEmpty :: Get a -> Get [a]
untilEmpty g = unfoldM $ do
    eof <- isEmpty
    if eof then return Nothing
           else Just <$> g

------------------------------------------------------------------------

newtype MD5 = MD5 ByteString
    deriving (Eq)

instance Show MD5 where
    show (MD5 bs) = printf "%0x%0x%0x%0x%0x%0x"
                           (bs `B.index` 0)
                           (bs `B.index` 1)
                           (bs `B.index` 2)
                           (bs `B.index` 3)
                           (bs `B.index` 4)
                           (bs `B.index` 5)
