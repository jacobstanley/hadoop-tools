{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main (main) where

import           Control.Exception (SomeException, handle, throwIO)
import           Control.Monad
import           Control.Monad.IO.Class (MonadIO, liftIO)

import           Data.Bits ((.&.), shiftR)
import qualified Data.ByteString.Char8 as B
import           Data.Foldable (foldMap)
import qualified Data.HashMap.Lazy as H
import           Data.List (intercalate)
import           Data.List (isPrefixOf)
import           Data.List.Split (splitOn)
import           Data.Maybe (fromMaybe, maybeToList, mapMaybe, listToMaybe)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import qualified Data.Text.Read as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word (Word16, Word32, Word64)

import qualified Data.Configurator as C
import           Data.Configurator.Types (Worth(..))
import           System.Environment (getEnv, lookupEnv)
import           System.FilePath.Posix
import           System.IO.Unsafe (unsafePerformIO)
import           System.Locale (defaultTimeLocale)
import           System.Posix.User (getEffectiveUserName)
import           Text.PrettyPrint.Boxes hiding ((<>))
import           Text.XmlHtml

import           Data.Conduit (handleC)
import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()

import           Hadoop.Protobuf.Hdfs
import           Hadoop.Rpc

import           Options.Applicative hiding (Success)

------------------------------------------------------------------------

main :: IO ()
main = do
    cmd <- execParser optsParser
    handle printError (runRemote cmd)
  where
    optsParser = info (helper <*> options)
                      (fullDesc <> header "hh - Blazing fast interaction with HDFS")

runRemote :: Remote a -> IO a
runRemote remote = do
    hdfsUser   <- getHdfsUser
    nameNode   <- getNameNode
    socksProxy <- getSocksProxy

    let run = maybe runTcp runSocks socksProxy
    run nameNode hdfsUser remote

------------------------------------------------------------------------

configPath :: FilePath
configPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home </> ".hh")
{-# NOINLINE configPath #-}

getHdfsUser :: IO User
getHdfsUser = attempt [fromEnv, fromCfg] fromUnix
  where
    fromEnv :: IO (Maybe User)
    fromEnv  = fmap T.pack <$> lookupEnv "HADOOP_USER_NAME"

    fromCfg :: IO (Maybe User)
    fromCfg  = C.load [Optional configPath] >>= flip C.lookup "hdfs.user"

    fromUnix :: IO User
    fromUnix = T.pack <$> getEffectiveUserName

getNameNode :: IO NameNode
getNameNode = attempt [fromHadoopCfg, fromUserCfg] explode
  where
    fromHadoopCfg :: IO (Maybe NameNode)
    fromHadoopCfg = listToMaybe . maybe [] id <$> readHadoopNameNodes

    fromUserCfg :: IO (Maybe NameNode)
    fromUserCfg = do
        cfg  <- C.load [Optional configPath]
        host <- C.lookup cfg "namenode.host"
        port <- C.lookupDefault 8020 cfg "namenode.port"
        return (Endpoint <$> host <*> pure port)

    explode :: IO a
    explode = error $ "could not find namenode details in /etc/hadoop/conf/ or " <> configPath

getSocksProxy :: IO (Maybe SocksProxy)
getSocksProxy = do
    cfg   <- C.load [Optional configPath]
    mhost <- C.lookup cfg "proxy.host"
    case mhost of
        Nothing   -> return Nothing
        Just host -> Just . Endpoint host <$> C.lookupDefault 1080 cfg "proxy.port"

attempt :: Monad m => [m (Maybe a)] -> m a -> m a
attempt [] def     = def
attempt (io:ios) def = io >>= \mx -> case mx of
    Just x  -> return x
    Nothing -> attempt ios def

------------------------------------------------------------------------

type HadoopConfig = H.HashMap Text Text

readHadoopNameNodes :: IO (Maybe [NameNode])
readHadoopNameNodes = do
    cfg <- H.union <$> readHadoopConfig "/etc/hadoop/conf/core-site.xml"
                   <*> readHadoopConfig "/etc/hadoop/conf/hdfs-site.xml"
    return $ resolveNameNode cfg <$> (stripProto =<< H.lookup fsDefaultNameKey cfg)
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
    lookupNameNodes cfg name = maybe [] id
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

------------------------------------------------------------------------

workingDirConfigPath :: FilePath
workingDirConfigPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home </> ".hhwd")
{-# NOINLINE workingDirConfigPath #-}

getWorkingDir :: MonadIO m => m FilePath
getWorkingDir = liftIO $ handle onError
                       $ T.unpack . T.takeWhile (/= '\n')
                     <$> T.readFile workingDirConfigPath
  where
    onError :: SomeException -> IO FilePath
    onError _ = (("/user" </>) . T.unpack) <$> getHdfsUser

setWorkingDir :: MonadIO m => FilePath -> m ()
setWorkingDir path = liftIO $ T.writeFile workingDirConfigPath
                            $ T.pack path <> "\n"

getAbsolute :: MonadIO m => FilePath -> m FilePath
getAbsolute path = liftIO (normalizePath <$> getPath)
  where
    getPath = if "/" `isPrefixOf` path
              then return path
              else getWorkingDir >>= \pwd -> return (pwd </> path)

normalizePath :: FilePath -> FilePath
normalizePath = intercalate "/" . dropAbsParentDir . splitOn "/"

dropAbsParentDir :: [FilePath] -> [FilePath]
dropAbsParentDir []       = error "dropAbsParentDir: not an absolute path"
dropAbsParentDir (p : ps) = p : (reverse $ fst $ go [] ps)
  where
    go []       (".." : ys) = go [] ys
    go (_ : xs) (".." : ys) = go xs ys
    go xs       (y    : ys) = go (y : xs) ys
    go xs       []          = (xs, [])

------------------------------------------------------------------------

data SubCommand = SubCommand
    { subName :: String
    , subDescription :: String
    , subMethod :: Parser (Remote ())
    }

sub :: SubCommand -> Mod CommandFields (Remote ())
sub SubCommand{..} = command subName (info subMethod $ progDesc subDescription)

options :: Parser (Remote ())
options = subparser (foldMap sub allSubCommands)

allSubCommands :: [SubCommand]
allSubCommands =
    [ subChDir
    , subDiskUsage
    , subList
    , subMkDir
    , subPwd
    , subRemove
    , subRename
    ]

completePath :: Mod ArgumentFields a
completePath = completer (fileCompletion (const True)) <> metavar "PATH"

completeDir :: Mod ArgumentFields a
completeDir  = completer (fileCompletion (== Dir)) <> metavar "DIRECTORY"

subChDir :: SubCommand
subChDir = SubCommand "cd" "Change working directory" go
  where
    go = cd <$> argument str (completeDir <> help "the directory to change to")
    cd path = setWorkingDir  =<< getAbsolute path

subDiskUsage :: SubCommand
subDiskUsage = SubCommand "du" "Show the amount of space used by file or directory" go
  where
    go = du <$> optional (argument str (completePath <> help "the file/directory to check the usage of"))
    du path = printDiskUsage =<< getAbsolute (maybe "" id path)

subList :: SubCommand
subList = SubCommand "ls" "List the contents of a directory" go
  where
    go = ls <$> (optional (argument str (completeDir <> help "the directory to list")))
    ls path = printListing =<< getAbsolute (fromMaybe "" path)

subMkDir :: SubCommand
subMkDir = SubCommand "mkdir" "Create a directory in the specified location" go
  where
    go = mkdir <$> argument str (completeDir       <> help "the directory to create")
               <*> switch       (short 'p' <> help "create intermediate directories")
    mkdir path parent =  do
      absPath <- getAbsolute path
      ok <- mkdirs absPath parent
      unless ok $ liftIO . putStrLn $ "Failed to create: " <> absPath

subPwd :: SubCommand
subPwd = SubCommand "pwd" "Print working directory" go
  where
    go = pure pwd
    pwd = liftIO . putStrLn =<< getWorkingDir

subRemove :: SubCommand
subRemove = SubCommand "rm" "Delete a file or directory" go
  where
    go = rm <$> argument str (completePath      <> help "the file/directory to remove")
            <*> switch       (short 'r' <> help "recursively remove the whole file hierarchy")
    rm path recursive = do
      absPath <- getAbsolute path
      ok <- delete absPath recursive
      unless ok $ liftIO . putStrLn $ "Failed to remove: " <> absPath

subRename :: SubCommand
subRename = SubCommand "mv" "Rename a file or directory" go
  where
    go = mv <$> argument str (completePath      <> help "source file/directory")
            <*> argument str (completePath      <> help "destination file/directory")
            <*> switch       (short 'f' <> help "overwrite destination if it exists")
    mv src dst force = do
      absSrc <- getAbsolute src
      absDst <- getAbsolute dst
      rename absSrc absDst force

------------------------------------------------------------------------

fileCompletion :: (FileType -> Bool) -> Completer
fileCompletion p = mkCompleter $ \path -> handle ignore $ runRemote $ do
    let (dir, _) = splitFileName' path
    ls <- getListing =<< getAbsolute dir

    return $ filter (path `isPrefixOf`)
           . map (displayPath dir)
           . filter (p . get fsFileType)
           . concatMap (get dlPartialListing)
           . maybeToList  $ ls
  where
    ignore (RemoteError _ _) = return []

    splitFileName' x = case splitFileName x of
        ("./", f) -> ("", f)
        (d, f)    -> (d, f)

displayPath :: FilePath -> FileStatus -> FilePath
displayPath parent file = parent </> B.unpack (get fsPath file) <> suffix
  where
    suffix = case get fsFileType file of
        Dir -> "/"
        _   -> ""

------------------------------------------------------------------------

printDiskUsage :: FilePath -> Remote ()
printDiskUsage path = do
    mls <- getListing path
    case mls of
      Nothing -> liftIO $ putStrLn $ "File/directory does not exist: " <> path
      Just ls -> do
        let files = map (displayPath path) (get dlPartialListing ls)
        css <- zip files <$> mapM getDirSize files

        let col a f = vcat a (map (text . f) css)

        liftIO $ do
            printBox $ col right snd
                   <+> col left  fst
  where
    getDirSize f = handleC (\e -> if isAccessDenied e then return "-" else liftIO (throwIO e))
                           (formatSize . get csLength <$> getContentSummary f)

printListing :: FilePath -> Remote ()
printListing path = do
    mls <- getListing path
    case mls of
      Nothing -> liftIO . putStrLn $ "Directory does not exist: " <> path
      Just ls -> do
        let getPerms     = fromIntegral . get fpPerm . get fsPermission
            getBlockRepl = fromMaybe 0 . get fsBlockReplication

            hdfs2utc ms  = posixSecondsToUTCTime (fromIntegral ms / 1000)
            getModTime   = hdfs2utc . get fsModificationTime

            -- TODO: Fetch rest of partial listing
            xs      = get dlPartialListing ls
            col a f = vcat a (map (text . f) xs)

        liftIO $ do
            putStrLn $ "Found " <> show (length xs) <> " items"

            printBox $ col left  (\x -> formatMode (get fsFileType x) (getPerms x))
                   <+> col right (formatBlockRepl . getBlockRepl)
                   <+> col left  (T.unpack . get fsOwner)
                   <+> col left  (T.unpack . get fsGroup)
                   <+> col right (formatSize . get fsLength)
                   <+> col right (formatUTC . getModTime)
                   <+> col left  (T.unpack . T.decodeUtf8 . get fsPath)

------------------------------------------------------------------------

get :: HasField a => (t -> a) -> t -> FieldType a
get f x = getField (f x)

------------------------------------------------------------------------

type Perms = Word16

formatSize :: Word64 -> String
formatSize b | b <= 0            = "0"
             | b < 1000          = show b <> "B"
             | b < 1000000       = show (b `div` 1000) <> "K"
             | b < 1000000000    = show (b `div` 1000000) <> "M"
             | b < 1000000000000 = show (b `div` 1000000000) <> "G"
             | otherwise         = show (b `div` 1000000000000) <> "T"

formatBlockRepl :: Word32 -> String
formatBlockRepl x | x == 0    = "-"
                  | otherwise = show x

formatUTC :: UTCTime -> String
formatUTC = formatTime defaultTimeLocale "%Y-%m-%d %H:%M"

formatMode :: FileType -> Perms -> String
formatMode File    = ("-" <>) . formatPerms
formatMode Dir     = ("d" <>) . formatPerms
formatMode SymLink = ("l" <>) . formatPerms

formatPerms :: Perms -> String
formatPerms perms = format (perms `shiftR` 6)
                 <> format (perms `shiftR` 3)
                 <> format perms
  where
    format p = conv 0x4 "r" p
            <> conv 0x2 "w" p
            <> conv 0x1 "x" p

    conv bit rep p | (p .&. bit) /= 0 = rep
                   | otherwise        = "-"

------------------------------------------------------------------------

printError :: RemoteError -> IO ()
printError (RemoteError subject body)
    | oneLiner  = T.putStrLn firstLine
    | otherwise = T.putStrLn subject >> T.putStrLn body
  where
    oneLiner  = subject `elem` [ "org.apache.hadoop.security.AccessControlException"
                               , "org.apache.hadoop.fs.FileAlreadyExistsException"
                               , "java.io.FileNotFoundException" ]
    firstLine = T.takeWhile (/= '\n') body

isAccessDenied :: RemoteError -> Bool
isAccessDenied (RemoteError s _) = s == "org.apache.hadoop.security.AccessControlException"
