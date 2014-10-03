module Network.Hadoop.Socket
    ( S.Socket
    , S.SockAddr(..)

    , runTcp
    , runSocks

    , connectSocket
    , newSocket
    , closeSocket
    ) where

import           Control.Applicative ((<$>))
import           Control.Exception (bracket, bracketOnError)
import           Data.Hadoop.Types
import qualified Data.Text as T
import           Network (PortID(PortNumber))
import qualified Network.Socket as S
import           Network.Socks5 (defaultSocksConf, socksConnectWith)

------------------------------------------------------------------------

runTcp :: Endpoint -> (S.Socket -> IO a) -> IO a
runTcp endpoint = bracket (fst <$> connectSocket endpoint) closeSocket

runSocks :: SocksProxy -> Endpoint -> (S.Socket -> IO a) -> IO a
runSocks proxy endpoint = bracket (socksConnectWith proxyConf host port) closeSocket
  where
    proxyConf = defaultSocksConf (T.unpack $ epHost proxy)
                                 (fromIntegral $ epPort proxy)

    host = T.unpack $ epHost endpoint
    port = PortNumber $ fromIntegral $ epPort endpoint

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
