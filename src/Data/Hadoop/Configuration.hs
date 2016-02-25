{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Data.Hadoop.Configuration
    ( authUser
    , getHadoopConfig
    , getHadoopUser
    , getNameNodes
    , readPrincipal
    , writePrincipal
    ) where

import           Control.Applicative ((<$>), (<*>))
import           Control.Exception (IOException, handle)
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

getHadoopConfig :: IO HadoopConfig
getHadoopConfig = do
    udUser <- getHadoopUser
    let udAuthUser = Nothing
    hcNameNodes <- getNameNodes
    let hcProxy = Nothing
    let hcUser = UserDetails{..}
    return HadoopConfig{..}

------------------------------------------------------------------------

getHadoopUser :: IO User
getHadoopUser = maybe fromUnix return =<< fromEnv
  where
    fromEnv :: IO (Maybe User)
    fromEnv  = fmap T.pack <$> lookupEnv "HADOOP_USER_NAME"

    fromUnix :: IO User
    fromUnix = T.pack <$> getEffectiveUserName

------------------------------------------------------------------------

-- Extract the name to be used for authentication
authUser :: UserDetails -> User
authUser UserDetails{..} = maybe udUser id udAuthUser

------------------------------------------------------------------------

readPrincipal :: Text -> HostName -> Maybe Principal
readPrincipal p host =
    case T.split (`elem` ['/', '@']) p of
        [pService,"_HOST",pRealm] -> let pHost = host in Just Principal{..}
        [pService,pHost,pRealm]   -> Just Principal{..}
        _                         ->
            case T.split (`elem` ['@']) p of
                [pService,pRealm] -> let pHost = "" in Just Principal{..}
                _                 -> Nothing

writePrincipal :: Principal -> Text
writePrincipal Principal{..} = case pHost of
    "" -> pService <> "@" <> pRealm
    _  -> pService <> "/" <> pHost <> "@" <> pRealm

------------------------------------------------------------------------
type HadoopXml = H.HashMap Text Text

getNameNodes :: IO [NameNode]
getNameNodes = do
    cfg <- H.union <$> readHadoopConfig "/etc/hadoop/conf/core-site.xml"
                   <*> readHadoopConfig "/etc/hadoop/conf/hdfs-site.xml"
    return $ fromMaybe []
           $ resolveNameNode cfg <$> (stripProto =<< H.lookup fsDefaultNameKey cfg)
  where
    proto             = "hdfs://"
    fsDefaultNameKey  = "fs.defaultFS"
    nameNodesPrefix   = "dfs.ha.namenodes."
    rpcAddressPrefix  = "dfs.namenode.rpc-address."
    namenodePrincipal = "dfs.namenode.kerberos.principal"

    stripProto :: Text -> Maybe Text
    stripProto uri | proto `T.isPrefixOf` uri = Just (T.drop (T.length proto) uri)
                   | otherwise                = Nothing

    resolveNameNode :: HadoopXml -> Text -> [NameNode]
    resolveNameNode cfg name = case parseEndpoint name of
        -- contains "host:port" directly
        Just ep@Endpoint{..} ->
            [ NameNode
                { nnEndPoint = ep
                , nnPrincipal = lookupPrincipal cfg epHost
                }
            ]
        Nothing -> mapMaybe (\nn -> do
                                ep <- lookupAddress cfg $ name <> "." <> nn
                                let pr = lookupPrincipal cfg (epHost ep)
                                return $ NameNode ep pr
                            ) (lookupNameNodes cfg name)

    lookupPrincipal :: HadoopXml -> HostName -> Maybe Principal
    lookupPrincipal cfg host = do
            p <- H.lookup namenodePrincipal cfg
            readPrincipal p host

    lookupNameNodes :: HadoopXml -> Text -> [Text]
    lookupNameNodes cfg name = fromMaybe []
                             $ T.splitOn "," <$> H.lookup (nameNodesPrefix <> name) cfg

    lookupAddress :: HadoopXml -> Text -> Maybe Endpoint
    lookupAddress cfg name = parseEndpoint =<< H.lookup (rpcAddressPrefix <> name) cfg

    parseEndpoint :: Text -> Maybe Endpoint
    parseEndpoint ep = Endpoint host <$> port
      where
        host = T.takeWhile (/= ':') ep
        port = either (const Nothing) (Just . fst)
             $ T.decimal $ T.drop (T.length host + 1) ep

readHadoopConfig :: FilePath -> IO HadoopXml
readHadoopConfig path = do
    exml <- readXML path
    case exml of
      Left  _   -> return H.empty
      Right xml -> return (toHashMap (docContent xml))
  where
    toHashMap = H.fromList . mapMaybe fromNode
              . concatMap (descendantElementsTag "property")

    fromNode n = (,) <$> (nodeText <$> childElementTag "name" n)
                     <*> (nodeText <$> childElementTag "value" n)

readXML :: FilePath -> IO (Either String Document)
readXML path = handle onError (parseXML path <$> B.readFile path)
  where
    onError :: IOException -> IO (Either String Document)
    onError e = return $ Left $ show e
