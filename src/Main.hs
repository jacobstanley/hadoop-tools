{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# OPTIONS_GHC -w #-}

module Main (main) where

import           Control.Applicative ((<$>), (<*>), many)
import           Control.Monad (when, unless, replicateM)
import           Control.Monad.Loops (unfoldM)
import           Data.Binary.Get
import           Data.Bits ((.|.), xor, shiftL)
import           Data.Int
import           Data.List (sort)
import           Data.Maybe (catMaybes)
import           Data.Monoid (Monoid, (<>), mempty, mconcat)
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
import           Pipes.Binary
import qualified Pipes.ByteString as P
import           Pipes.Parse

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
    let p = P.fromHandle h
    runEffect $ do
        (Right hdr, p') <- runStateT (decodeGet getHeader) p
        lift (print hdr)
        rb <- evalStateT (decodeGet $ getRecordBlock (sync hdr)) p'
        lift (putStrLn $ take 200 $ show rb)

------------------------------------------------------------------------

data Header = Header
    { keyType         :: !Text
    , valueType       :: !Text
    , compressionType :: !Text
    , metadata        :: ![(Text, Text)]
    , sync            :: !MD5
    } deriving (Show)

data RecordBlock = RecordBlock
    { numRecords   :: !Int
    , keyLengths   :: !Block
    , keys         :: !Block
    , valueLengths :: !Block
    , values       :: !Block
    } deriving (Show)

data Block = Block
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

getRecordBlock :: MD5 -> Get RecordBlock
getRecordBlock sync = label "record block" $ do
    escape <- getWord32le
    when (escape /= 0xffffffff)
         (fail $ "file corrupt, expected to find sync escape " ++
                 "<0xffffffff> but was " ++ printf "<0x%0x>" escape)

    sync' <- getMD5
    when (sync /= sync')
         (fail $ "file corrupt, expected to find sync marker " ++
                 "<" ++ show sync ++ "> but was <" ++ show sync' ++ ">")

    numRecords   <- getVInt
    keyLengths   <- label "key lengths"   getBlock
    keys         <- label "keys"          getBlock
    valueLengths <- label "value lengths" getBlock
    values       <- label "values"        getBlock

    return RecordBlock{..}

    -- let getSnappy = L.fromChunks . map Snappy.decompress <$> getBlock
    --     withSnappy g = label "withSnappy" $ runEmbeded g =<< getSnappy

    -- keyLengths <- withSnappy $ many getVInt
    -- keys       <- withSnappy $ isolateN keyLengths gk

    -- valueLengths <- withSnappy $ many getVInt
    -- values       <- withSnappy $ isolateN valueLengths gv

getBlock :: Get Block
getBlock = do
    size <- getVInt
    isolate size $ do
        originalSize    <- fromIntegral <$> getWord32be
        compressedParts <- catMaybes <$> untilEmpty getNext
        return Block{..}
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
