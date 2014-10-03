{-# LANGUAGE OverloadedStrings #-}

--------------------------------------------------------------------------------
-- | Lightweight abstraction over an input/output stream.
-- (stolen from 'websockets' package)

module Network.Hadoop.Stream
    ( Stream
    , mkStream
    , mkSocketStream
    , mkEchoStream
    , parse
    , maybeGet
    , runGet
    , runPut
    , write
    , close
    ) where

import           Control.Applicative ((<$>))
import qualified Control.Concurrent.Chan as Chan
import           Control.Exception (throwIO)
import           Control.Monad (forM_)
import qualified Data.Attoparsec.ByteString as Atto
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import           Data.IORef (IORef, newIORef, readIORef, writeIORef)
import qualified Data.Serialize.Get as Get
import qualified Data.Serialize.Put as Put
import qualified Network.Socket as S
import qualified Network.Socket.ByteString as B (recv)
import qualified Network.Socket.ByteString.Lazy as L (sendAll)

import           Data.Hadoop.Types

--------------------------------------------------------------------------------

-- | State of the stream
data StreamState
    = Closed !B.ByteString  -- Remainder
    | Open   !B.ByteString  -- Buffer

--------------------------------------------------------------------------------

-- | Lightweight abstraction over an input/output stream.
data Stream = Stream
    { streamIn    :: IO (Maybe B.ByteString)
    , streamOut   :: (Maybe L.ByteString -> IO ())
    , streamState :: !(IORef StreamState)
    }

--------------------------------------------------------------------------------

mkStream
    :: IO (Maybe B.ByteString)        -- ^ Reading
    -> (Maybe L.ByteString -> IO ())  -- ^ Writing
    -> IO Stream                      -- ^ Resulting stream
mkStream i o = Stream i o <$> newIORef (Open B.empty)

--------------------------------------------------------------------------------

mkSocketStream :: S.Socket -> IO Stream
mkSocketStream socket = mkStream receive send
  where
    receive = do
        bs <- B.recv socket 4096
        return $ if B.null bs then Nothing else Just bs

    send Nothing   = return ()
    send (Just bs) = L.sendAll socket bs

--------------------------------------------------------------------------------

mkEchoStream :: IO Stream
mkEchoStream = do
    chan <- Chan.newChan
    mkStream (Chan.readChan chan) $ \mbBs -> case mbBs of
        Nothing -> Chan.writeChan chan Nothing
        Just bs -> forM_ (L.toChunks bs) $ \c -> Chan.writeChan chan (Just c)

--------------------------------------------------------------------------------

parse :: Stream -> Atto.Parser a -> IO (Maybe a)
parse stream parser = do
    state <- readIORef (streamState stream)
    case state of
        Closed remainder
            | B.null remainder -> return Nothing
            | otherwise        -> go (Atto.parse parser remainder) True
        Open buffer
            | B.null buffer -> do
                mbBs <- streamIn stream
                case mbBs of
                    Nothing -> do
                        writeIORef (streamState stream) (Closed B.empty)
                        return Nothing
                    Just bs -> go (Atto.parse parser bs) False
            | otherwise     -> go (Atto.parse parser buffer) False
  where
    -- Buffer is empty when entering this function.
    go (Atto.Done remainder x) closed = do
        writeIORef (streamState stream) $
            if closed then Closed remainder else Open remainder
        return (Just x)
    go (Atto.Partial f) closed
        | closed    = go (f B.empty) True
        | otherwise = do
            mbBs <- streamIn stream
            case mbBs of
                Nothing -> go (f B.empty) True
                Just bs -> go (f bs) False
    go (Atto.Fail _ _ err) _ = error ("parse: " ++ err)

maybeGet :: Stream -> Get.Get a -> IO (Maybe a)
maybeGet stream getter = do
    state <- readIORef (streamState stream)
    case state of
        Closed remainder
            | B.null remainder -> return Nothing
            | otherwise        -> go (Get.runGetPartial getter remainder) True
        Open buffer
            | B.null buffer -> do
                mbBs <- streamIn stream
                case mbBs of
                    Nothing -> do
                        writeIORef (streamState stream) (Closed B.empty)
                        return Nothing
                    Just bs -> go (Get.runGetPartial getter bs) False
            | otherwise     -> go (Get.runGetPartial getter buffer) False
  where
    -- Buffer is empty when entering this function.
    go (Get.Done x remainder) closed = do
        writeIORef (streamState stream) $
            if closed then Closed remainder else Open remainder
        return (Just x)
    go (Get.Partial f) closed
        | closed    = go (f B.empty) True
        | otherwise = do
            mbBs <- streamIn stream
            case mbBs of
                Nothing -> go (f B.empty) True
                Just bs -> go (f bs) False
    go (Get.Fail err _) _ = error ("runGetStream: " ++ err)

runGet :: Stream -> Get.Get a -> IO a
runGet stream getter = maybe throwClosed return =<< maybeGet stream getter
  where
    throwClosed = throwIO (RemoteError "ConnectionClosed" "The socket connection was closed")

--------------------------------------------------------------------------------

runPut :: Stream -> Put.Put -> IO ()
runPut stream = write stream . Put.runPutLazy

write :: Stream -> L.ByteString -> IO ()
write stream = streamOut stream . Just

--------------------------------------------------------------------------------

close :: Stream -> IO ()
close stream = streamOut stream Nothing
