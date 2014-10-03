{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module HdfsSource where

import           Control.Concurrent.Async
import           Control.Concurrent.QSem
import           Control.Exception
import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Types
import           Data.Hashable
import           Data.Typeable (Typeable)
import qualified Data.Vector as V
import           Haxl.Core
import           Network.Hadoop.Hdfs

import           Hdfs

------------------------------------------------------------------------

haxlListing :: HdfsPath -> GenHaxl u (V.Vector FileStatus)
haxlListing path = uncachedRequest (GetListing path)

------------------------------------------------------------------------

deriving instance Typeable FileStatus

------------------------------------------------------------------------

data HdfsReq a where
    GetListing :: HdfsPath -> HdfsReq (V.Vector FileStatus)
  deriving Typeable

deriving instance Eq (HdfsReq a)
deriving instance Show (HdfsReq a)

instance Show1 HdfsReq where show1 = show

instance Hashable (HdfsReq a) where
  hashWithSalt s (GetListing path) = hashWithSalt s (0::Int, path)

instance StateKey HdfsReq where
  data State HdfsReq = HdfsState
       { numThreads :: Int
       , config     :: HadoopConfig
       }

instance DataSourceName HdfsReq where
  dataSourceName _ = "HDFS"

instance DataSource u HdfsReq where
  fetch = hdfsFetch

initGlobalState :: Int -> IO (State HdfsReq)
initGlobalState numThreads = do
    config <- getConfig
    return HdfsState{..}

hdfsFetch :: State HdfsReq -> Flags -> u -> [BlockedFetch HdfsReq] -> PerformFetch
hdfsFetch HdfsState{..} _flags _user bfs =
  AsyncFetch $ \inner -> do
    sem <- newQSem numThreads
    asyncs <- mapM (fetchAsync config sem) bfs
    inner
    mapM_ wait asyncs

fetchAsync :: HadoopConfig -> QSem -> BlockedFetch HdfsReq -> IO (Async ())
fetchAsync config sem (BlockedFetch req rvar) =
  async $ bracket_ (waitQSem sem) (signalQSem sem) $ do
    e <- Control.Exception.try (fetchReq config req)
    case e of
      Left ex -> putFailure rvar (ex :: SomeException)
      Right a -> putSuccess rvar a


fetchReq :: HadoopConfig -> HdfsReq a -> IO a
fetchReq config (GetListing path) = runHdfs' config (getListing' path)
