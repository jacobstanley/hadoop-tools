module Network.Hadoop.Socket
    ( S.Socket
    , S.SockAddr(..)

    , runTcp

    , connectSocket
    , newSocket
    , closeSocket
    ) where

import           Control.Applicative ((<$>))
import           Control.Monad.Catch (MonadMask, bracket, bracketOnError)
import           Control.Monad.IO.Class (MonadIO(..))
import           Data.Hadoop.Types
import qualified Data.Text as T
import           Network (PortID(PortNumber))
import qualified Network.Socket as S
import           Network.Socks5 (defaultSocksConf, socksConnectWith)

------------------------------------------------------------------------

runTcp :: (MonadMask m, MonadIO m) => Maybe SocksProxy -> Endpoint -> (S.Socket -> m a) -> m a
runTcp Nothing      = runTcp'
runTcp (Just proxy) = runSocks proxy

runTcp' :: (MonadMask m, MonadIO m) => Endpoint -> (S.Socket -> m a) -> m a
runTcp' endpoint = bracket
    (liftIO $ fst <$> connectSocket endpoint)
    (liftIO . closeSocket)

runSocks :: (MonadMask m, MonadIO m) => SocksProxy -> Endpoint -> (S.Socket -> m a) -> m a
runSocks proxy endpoint = bracket
    (liftIO $ socksConnectWith proxyConf host port)
    (liftIO . closeSocket)
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
