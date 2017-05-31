{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Hadoop.Tools.Configuration
    ( getConfig
    ) where

import           Control.Monad

import qualified Data.Configurator as C
import           Data.Configurator.Types (Worth(..))

import           System.Environment (getEnv)
import qualified System.FilePath as FilePath
import           System.IO.Unsafe (unsafePerformIO)


import           Data.Hadoop.Configuration (getHadoopConfig, readPrincipal)
import           Data.Hadoop.Types

-- | Augments any auto-discovered hadoop configuration parameters with
-- those found in your `hh` config.
getConfig :: IO HadoopConfig
getConfig = do
    hdfsUser   <- getHdfsUser
    authUser   <- getAuthUser
    nameNode   <- getNameNode
    socksProxy <- getSocksProxy

    liftM ( set hdfsUser   (\c@HadoopConfig{..} x -> c { hcUser = hcUser { udUser = x }})
          . set authUser   (\c@HadoopConfig{..} x -> c { hcUser = hcUser { udAuthUser = Just x }})
          . set nameNode   (\c x -> c { hcNameNodes = [x] })
          . set socksProxy (\c x -> c { hcProxy     = Just x })
          ) getHadoopConfig
  where
    set :: Maybe a -> (b -> a -> b) -> b -> b
    set m f c = maybe c (f c) m

------------------------------------------------------------------------

configPath :: FilePath
configPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home `FilePath.combine` ".hh")
{-# NOINLINE configPath #-}

getHdfsUser :: IO (Maybe User)
getHdfsUser = do
    cfg <- C.load [Optional configPath]
    C.lookup cfg "hdfs.user"

getAuthUser :: IO (Maybe User)
getAuthUser = do
    cfg <- C.load [Optional configPath]
    C.lookup cfg "auth.user"

-- getHdfsUser = C.load [Optional configPath] >>= flip C.lookup "hdfs.user"

getNameNode :: IO (Maybe NameNode)
getNameNode = do
    cfg     <- C.load [Optional configPath]
    host    <- C.lookup cfg "namenode.host"
    port    <- C.lookupDefault 8020 cfg "namenode.port"
    prinStr <- C.lookup cfg "namenode.principal"
    let endpoint  = Endpoint <$> host <*> pure port
        principal = join $ readPrincipal <$> prinStr <*> host
    return $ flip NameNode principal <$> endpoint

getSocksProxy :: IO (Maybe SocksProxy)
getSocksProxy = do
    cfg   <- C.load [Optional configPath]
    mhost <- C.lookup cfg "proxy.host"
    case mhost of
        Nothing   -> return Nothing
        Just host -> Just . Endpoint host <$> C.lookupDefault 1080 cfg "proxy.port"

