module Network.Hadoop.Socket
    ( S.Socket
    , bracketSocket
    , connectSocket
    , closeSocket
    ) where

import           Control.Monad.Catch (MonadMask, bracket, bracketOnError)
import           Control.Monad.IO.Class (MonadIO(..))
import           Data.Hadoop.Types
import qualified Data.Text as T
import           Network (PortID(PortNumber))
import qualified Network.Socket as S
import           Network.Socks5 (defaultSocksConf, socksConnectWith)

------------------------------------------------------------------------

bracketSocket :: (MonadMask m, MonadIO m) => Maybe SocksProxy -> Endpoint -> (S.Socket -> m a) -> m a
bracketSocket proxy endpoint = bracket (connectSocket proxy endpoint) closeSocket

connectSocket :: (MonadMask m, MonadIO m) => Maybe SocksProxy -> Endpoint -> m S.Socket
connectSocket Nothing      = connectDirect
connectSocket (Just proxy) = connectSocks proxy

closeSocket :: MonadIO m => S.Socket -> m ()
closeSocket = liftIO . S.close

------------------------------------------------------------------------

connectDirect :: (MonadMask m, MonadIO m) => Endpoint -> m S.Socket
connectDirect endpoint = do
    (addr:_) <- liftIO $ S.getAddrInfo (Just hints) (Just host) (Just port)
    bracketOnError (newSocket addr) closeSocket $ \sock -> do
       liftIO $ S.connect sock (S.addrAddress addr)
       return sock
  where
    host  = T.unpack (epHost endpoint)
    port  = show (epPort endpoint)
    hints = S.defaultHints { S.addrFlags = [S.AI_ADDRCONFIG]
                           , S.addrSocketType = S.Stream }

newSocket :: MonadIO m => S.AddrInfo -> m S.Socket
newSocket addr = liftIO $ S.socket (S.addrFamily addr)
                                   (S.addrSocketType addr)
                                   (S.addrProtocol addr)

------------------------------------------------------------------------

connectSocks :: (MonadIO m) => SocksProxy -> Endpoint -> m S.Socket
connectSocks proxy endpoint = liftIO (socksConnectWith proxyConf host port)
  where
    proxyConf = defaultSocksConf (T.unpack $ epHost proxy)
                                 (fromIntegral $ epPort proxy)

    host = T.unpack $ epHost endpoint
    port = PortNumber $ fromIntegral $ epPort endpoint
