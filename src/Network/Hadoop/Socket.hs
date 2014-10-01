module Network.Hadoop.Socket
    ( S.Socket
    , S.SockAddr(..)
    , runTcp
    , connectSocket
    , newSocket
    , closeSocket
    ) where

import           Control.Exception (bracket, bracketOnError)
import           Data.Hadoop.Types
import qualified Data.Text as T
import qualified Network.Socket as S

------------------------------------------------------------------------

runTcp :: Endpoint -> ((S.Socket, S.SockAddr) -> IO a) -> IO a
runTcp endpoint = bracket (connectSocket endpoint) (closeSocket . fst)

------------------------------------------------------------------------

connectSocket :: Endpoint -> IO (S.Socket, S.SockAddr)
connectSocket endpoint = do
    (addr:_) <- S.getAddrInfo (Just hints) (Just host) (Just port)
    bracketOnError (newSocket addr) closeSocket $ \sock -> do
       let sockAddr = S.addrAddress addr
       S.connect sock sockAddr
       return (sock, sockAddr)
  where
    host  = T.unpack (epHost endpoint)
    port  = show (epPort endpoint)
    hints = S.defaultHints { S.addrFlags = [S.AI_ADDRCONFIG]
                           , S.addrSocketType = S.Stream }

newSocket :: S.AddrInfo -> IO S.Socket
newSocket addr = S.socket (S.addrFamily addr)
                          (S.addrSocketType addr)
                          (S.addrProtocol addr)

closeSocket :: S.Socket -> IO ()
closeSocket = S.sClose
