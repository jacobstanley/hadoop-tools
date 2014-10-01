{-# LANGUAGE OverloadedStrings #-}

module Data.Hadoop.Configuration
    ( getUser
    , getNameNodes
    ) where

import           Control.Applicative ((<$>), (<*>))
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Lazy as H
import           Data.Maybe (fromMaybe, mapMaybe)
import           Data.Monoid ((<>))
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Read as T

import           System.Environment (lookupEnv)
import           System.Posix.User (getEffectiveUserName)
import           Text.XmlHtml

import           Data.Hadoop.Types

------------------------------------------------------------------------

getUser :: IO User
getUser = maybe fromUnix return =<< fromEnv
  where
    fromEnv :: IO (Maybe User)
    fromEnv  = fmap T.pack <$> lookupEnv "HADOOP_USER_NAME"

    fromUnix :: IO User
    fromUnix = T.pack <$> getEffectiveUserName

------------------------------------------------------------------------

type HadoopConfig = H.HashMap Text Text

getNameNodes :: IO [NameNode]
getNameNodes = do
    cfg <- H.union <$> readHadoopConfig "/etc/hadoop/conf/core-site.xml"
                   <*> readHadoopConfig "/etc/hadoop/conf/hdfs-site.xml"
    return $ fromMaybe []
           $ resolveNameNode cfg <$> (stripProto =<< H.lookup fsDefaultNameKey cfg)
  where
    proto            = "hdfs://"
    fsDefaultNameKey = "fs.defaultFS"
    nameNodesPrefix  = "dfs.ha.namenodes."
    rpcAddressPrefix = "dfs.namenode.rpc-address."

    stripProto :: Text -> Maybe Text
    stripProto uri | proto `T.isPrefixOf` uri = Just (T.drop (T.length proto) uri)
                   | otherwise                = Nothing

    resolveNameNode :: HadoopConfig -> Text -> [NameNode]
    resolveNameNode cfg name = case parseEndpoint name of
        Just ep -> [ep] -- contains "host:port" directly
        Nothing -> mapMaybe (\nn -> lookupAddress cfg $ name <> "." <> nn)
                            (lookupNameNodes cfg name)

    lookupNameNodes :: HadoopConfig -> Text -> [Text]
    lookupNameNodes cfg name = fromMaybe []
                             $ T.splitOn "," <$> H.lookup (nameNodesPrefix <> name) cfg

    lookupAddress :: HadoopConfig -> Text -> Maybe Endpoint
    lookupAddress cfg name = parseEndpoint =<< H.lookup (rpcAddressPrefix <> name) cfg

    parseEndpoint :: Text -> Maybe Endpoint
    parseEndpoint ep = Endpoint host <$> port
      where
        host = T.takeWhile (/= ':') ep
        port = either (const Nothing) (Just . fst)
             $ T.decimal $ T.drop (T.length host + 1) ep

readHadoopConfig :: FilePath -> IO HadoopConfig
readHadoopConfig path = do
    exml <- parseXML path <$> B.readFile path
    case exml of
      Left  _   -> return H.empty
      Right xml -> return (toHashMap (docContent xml))
  where
    toHashMap = H.fromList . mapMaybe fromNode
              . concatMap (descendantElementsTag "property")

    fromNode n = (,) <$> (nodeText <$> childElementTag "name" n)
                     <*> (nodeText <$> childElementTag "value" n)
