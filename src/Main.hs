{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -w #-}

module Main (main) where

import qualified Codec.Compression.Snappy as Snappy
import           Control.Applicative ((<$>), (<*>), many)
import           Control.Monad (when, unless, replicateM)
import           Control.Monad.Loops (unfoldM)
import           Data.Binary.Get
import           Data.Bits ((.|.), xor, shiftL)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as L
import           Data.Int
import           Data.List (sort)
import           Data.Maybe (catMaybes)
import           Data.Monoid (Monoid, (<>), mempty, mconcat)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import           Data.Word
import           System.Environment (getArgs)
import           System.IO.Posix.MMap (unsafeMMapFile)
import           Text.Printf (printf)

main :: IO ()
main = do
    args <- getArgs
    case args of
      [path] -> hdshow path
      _      -> putStrLn "Usage: hdshow PATH"

hdshow :: FilePath -> IO ()
hdshow path = do
    bs <- unsafeMMapFile path
    let xs = runGet getSequenceFile (L.fromStrict bs)
    mapM_ go xs
  where
    go (k, v) = T.putStrLn (k <> "|" <> T.pack sz)
      where
        sz = show (L.length v) <> " bytes"
        v' = show (L.take 50 v)

------------------------------------------------------------------------

data Header = Header {
    keyType   :: Text
  , valueType :: Text
  , sync      :: MD5
  } deriving (Show)

------------------------------------------------------------------------

getSequenceFile :: Get [(Text, L.ByteString)]
getSequenceFile = do
    hdr <- getHeader

    let getKey = getText
        getVal = getRemainingLazyByteString

    concat <$> untilEmpty (getRecordBlock (sync hdr) getKey getVal)

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

    when (compressionType /= "org.apache.hadoop.io.compress.SnappyCodec")
         (fail $ "unsupported compression codec: " ++ T.unpack compressionType)

    metadata <- getMetadata

    sync <- getMD5

    return Header{..}

getMetadata :: Get [(Text, Text)]
getMetadata = do
    n <- fromIntegral <$> getWord32le
    replicateM n $ (,) <$> getText <*> getText

------------------------------------------------------------------------

getRecordBlock :: MD5 -> Get k -> Get v -> Get [(k, v)]
getRecordBlock sync gk gv = do
    escape <- getWord32le
    when (escape /= 0xffffffff)
         (fail $ "file corrupt, expected to find sync escape " ++
                 "<0xffffffff> but was " ++ printf "<0x%0x>" escape)

    sync' <- getMD5
    when (sync /= sync')
         (fail $ "file corrupt, expected to find sync marker " ++
                 "<" ++ show sync ++ "> but was <" ++ show sync' ++ ">")

    n <- getVInt

    let getSnappy = L.fromChunks . map Snappy.decompress <$> getBlock
        withSnappy g = label "withSnappy" $ runEmbeded g =<< getSnappy

    keyLengths <- withSnappy $ many getVInt
    keys       <- withSnappy $ isolateN keyLengths gk

    valueLengths <- withSnappy $ many getVInt
    values       <- withSnappy $ isolateN valueLengths gv

    return (zip keys values)

isolateN :: [Int] -> Get a -> Get [a]
isolateN ns g = label "isolateN" $ mapM (`isolate` g) ns

------------------------------------------------------------------------

runEmbeded :: Get a -> L.ByteString -> Get a
runEmbeded g lbs = label "runEmbeded" $ case runGetOrFail g lbs of
    Right (_, _, x)    -> return x
    Left (_, pos, msg) -> fail ("at sub-position " ++ show pos ++ ": " ++ msg)

getBlock :: Get [ByteString]
getBlock = label "getBlock" $ do
    total <- getVInt
    isolate total $ do
        _ <- getWord32be -- original uncompressed size
        catMaybes <$> untilEmpty getNext
  where
    getNext = do
      n <- fromIntegral <$> getWord32be
      if n == 0 then return Nothing
                else Just <$> getByteString n

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
