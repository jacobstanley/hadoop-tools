{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -w #-}

module Main (main) where

import           Codec.Compression.Snappy (decompress)
import           Control.Applicative ((<$>), (<*>))
import           Control.Monad (when, replicateM)
import           Control.Monad.Loops (unfoldM)
import           Data.Binary.Get
import           Data.Bits ((.|.), xor, shiftL)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as L
import           Data.Int
import           Data.Monoid ((<>))
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
    mapM_ T.putStrLn (runGet getSequenceFile (L.fromStrict bs))

------------------------------------------------------------------------

getSequenceFile :: Get [Text]
getSequenceFile = do
    magic <- getByteString 3
    when (magic /= "SEQ")
         (fail "not a sequence file")

    version <- getWord8
    when (version /= 6)
         (fail $ "unknown version: " ++ show version)

    keyClassName   <- getText
    valueClassName <- getText

    compression      <- getBool
    blockCompression <- getBool

    when (not (compression && blockCompression))
         (fail "only block compressed files supported")

    compressionClassName <- getText

    when (compressionClassName /= "org.apache.hadoop.io.compress.SnappyCodec")
         (fail $ "unsupported compression codec: " ++ T.unpack compressionClassName)

    metadata <- getMetadata

    sync <- getMD5

    blocks <- untilEmpty (getBlock sync)

    return $ head blocks ++ last blocks

getMetadata :: Get [(Text, Text)]
getMetadata = do
    n <- fromIntegral <$> getWord32le
    replicateM n $ (,) <$> getText <*> getText

------------------------------------------------------------------------

getBlock :: MD5 -> Get [Text]
getBlock sync = do
    escape <- getWord32le
    when (escape /= 0xffffffff)
         (fail $ "file corrupt, expected to find sync escape " ++
                 "<0xffffffff> but was " ++ printf "<0x%0x>" escape)

    sync' <- getMD5
    when (sync /= sync')
         (fail $ "file corrupt, expected to find sync marker " ++
                 "<" ++ show sync ++ "> but was <" ++ show sync' ++ ">")

    n <- getVInt

    keyLengths <- readVInts <$> getSnappyBuffer
    keys       <- map readText . splitBuffer keyLengths <$> getSnappyBuffer

    valueLengths <- readVInts <$> getSnappyBuffer
    values       <- splitBuffer valueLengths <$> getSnappyBuffer

    return keys

readVInts :: ByteString -> [Int]
readVInts = fromGet (untilEmpty getVInt)

readText :: ByteString -> Text
readText = fromGet getText

 -- TODO: error below is not nice
splitBuffer :: [Int] -> ByteString -> [ByteString]
splitBuffer [] ""      = []
splitBuffer [] _       = error "splitBuffer: split did not occur evenly"
splitBuffer (n:ns) bss = bs : splitBuffer ns bss'
  where
   (bs, bss') = B.splitAt n bss

------------------------------------------------------------------------

getSnappyBuffer :: Get ByteString
getSnappyBuffer = do
    total <- fromIntegral <$> getVInt
    start <- bytesRead

    expectedSize <- fromIntegral <$> getWord32be

    bss <- unfoldM $ do
      pos <- bytesRead
      case total - (pos - start) of
        0         -> return Nothing
        n | n > 0 -> Just <$> getNext
        otherwise -> fail "file corrupt, decompressed too many bytes"

    let bs = (B.concat bss)
    let actualSize = B.length bs

    when (expectedSize /= actualSize)
         (fail $ "file corrupt, expected <" ++ show expectedSize ++
                 " bytes> " ++ "but was <" ++ show actualSize ++ " bytes>")

    return bs
  where
    getNext = do
      n <- fromIntegral <$> getWord32be
      if n == 0 then return B.empty
                else decompress <$> getByteString n

------------------------------------------------------------------------

fromGet :: Get a -> ByteString -> a
fromGet g bs = runGet g (L.fromStrict bs)

getBool :: Get Bool
getBool = (/= 0) <$> getWord8

getText :: Get Text
getText = do
    n  <- getVInt
    bs <- getByteString n
    return (T.decodeUtf8 bs)

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
