{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main (main) where

import           Control.Exception (SomeException)
import           Control.Monad
import           Control.Monad.Catch (handle, throwM)
import           Control.Monad.IO.Class (MonadIO, liftIO)

import           Data.Bits ((.&.), shiftR)
import qualified Data.ByteString.Char8 as B
import           Data.Foldable (foldMap)
import           Data.List (intercalate, isPrefixOf)
import           Data.List.Split (splitOn)
import           Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word (Word16, Word32, Word64)
import qualified Data.Vector as V

import qualified Data.Configurator as C
import           Data.Configurator.Types (Worth(..))
import           System.Environment (getEnv)
import           System.FilePath.Posix
import           System.IO.Unsafe (unsafePerformIO)
import           System.Locale (defaultTimeLocale)
import           Text.PrettyPrint.Boxes hiding ((<>))

import           Data.Hadoop.Configuration (getHadoopConfig)
import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Types
import           Data.ProtocolBuffers (HasField(..), FieldType, getField)
import           Network.Hadoop.Hdfs hiding (runHdfs)

import           Options.Applicative hiding (Success)

import           Data.Version (showVersion)
import           Paths_hadoop_tools (version)

------------------------------------------------------------------------

main :: IO ()
main = do
    cmd <- execParser optsParser
    handle printError (runHdfs cmd)
  where
    optsParser = info (helper <*> options)
                      (fullDesc <> header "hh - Blazing fast interaction with HDFS")

runHdfs :: Hdfs a -> IO a
runHdfs hdfs = do
    config <- getConfig
    runHdfs' config hdfs

getConfig :: IO HadoopConfig
getConfig = do
    hdfsUser   <- getHdfsUser
    nameNode   <- getNameNode
    socksProxy <- getSocksProxy

    liftM ( set hdfsUser   (\c x -> c { hcUser      = x })
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
    return (home </> ".hh")
{-# NOINLINE configPath #-}

getHdfsUser :: IO (Maybe User)
getHdfsUser = C.load [Optional configPath] >>= flip C.lookup "hdfs.user"

getNameNode :: IO (Maybe NameNode)
getNameNode = do
    cfg  <- C.load [Optional configPath]
    host <- C.lookup cfg "namenode.host"
    port <- C.lookupDefault 8020 cfg "namenode.port"
    return (Endpoint <$> host <*> pure port)

getSocksProxy :: IO (Maybe SocksProxy)
getSocksProxy = do
    cfg   <- C.load [Optional configPath]
    mhost <- C.lookup cfg "proxy.host"
    case mhost of
        Nothing   -> return Nothing
        Just host -> Just . Endpoint host <$> C.lookupDefault 1080 cfg "proxy.port"

------------------------------------------------------------------------

workingDirConfigPath :: FilePath
workingDirConfigPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home </> ".hhwd")
{-# NOINLINE workingDirConfigPath #-}

getDefaultWorkingDir :: MonadIO m => m FilePath
getDefaultWorkingDir = liftIO $ (("/user" </>) . T.unpack . hcUser) <$> getConfig

getWorkingDir :: MonadIO m => m FilePath
getWorkingDir = liftIO $ handle onError
                       $ T.unpack . T.takeWhile (/= '\n')
                     <$> T.readFile workingDirConfigPath
  where
    onError :: SomeException -> IO FilePath
    onError = const getDefaultWorkingDir

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
dropAbsParentDir (p : ps) = p : reverse (fst $ go [] ps)
  where
    go []       (".." : ys) = go [] ys
    go (_ : xs) (".." : ys) = go xs ys
    go xs       (y    : ys) = go (y : xs) ys
    go xs       []          = (xs, [])

------------------------------------------------------------------------

data SubCommand = SubCommand
    { subName :: String
    , subDescription :: String
    , subMethod :: Parser (Hdfs ())
    }

sub :: SubCommand -> Mod CommandFields (Hdfs ())
sub SubCommand{..} = command subName (info subMethod $ progDesc subDescription)

options :: Parser (Hdfs ())
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
    , subVersion
    ]

completePath :: Mod ArgumentFields a
completePath = completer (fileCompletion (const True)) <> metavar "PATH"

completeDir :: Mod ArgumentFields a
completeDir  = completer (fileCompletion (== Dir)) <> metavar "DIRECTORY"

subChDir :: SubCommand
subChDir = SubCommand "cd" "Change working directory" go
  where
    go = cd <$> optional (argument str (completeDir <> help "the directory to change to"))
    cd mpath = do
        path <- getAbsolute =<< maybe getDefaultWorkingDir return mpath
        mls <- getListing path
        liftIO $ case mls of
            Nothing -> putStrLn $ "Directory does not exist: " <> path
            Just _  -> setWorkingDir path

subDiskUsage :: SubCommand
subDiskUsage = SubCommand "du" "Show the amount of space used by file or directory" go
  where
    go = du <$> optional (argument str (completePath <> help "the file/directory to check the usage of"))
    du path = printDiskUsage =<< getAbsolute (fromMaybe "" path)

subList :: SubCommand
subList = SubCommand "ls" "List the contents of a directory" go
  where
    go = ls <$> optional (argument str (completeDir <> help "the directory to list"))
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

subVersion :: SubCommand
subVersion = SubCommand "version" "Show version information" go
  where
    go = pure $ liftIO $ putStrLn $ "hh version " <> showVersion version

------------------------------------------------------------------------

fileCompletion :: (FileType -> Bool) -> Completer
fileCompletion p = mkCompleter $ \path -> handle ignore $ runHdfs $ do
    let (dir, _) = splitFileName' path
    ls <- getListing' =<< getAbsolute dir

    return $ V.toList
           . V.filter (path `isPrefixOf`)
           . V.map (displayPath dir)
           . V.filter (p . get fsFileType)
           $ ls
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

printDiskUsage :: FilePath -> Hdfs ()
printDiskUsage path = do
    mls <- getListing path
    case mls of
      Nothing -> liftIO $ putStrLn $ "File/directory does not exist: " <> path
      Just ls -> do
        let files = V.map (displayPath path) ls
        css <- V.zip files <$> V.mapM getDirSize files

        let col a f = vcat a (map (text . f) (V.toList css))

        liftIO $ printBox $ col right snd
                        <+> col left  fst
  where
    getDirSize f = handle (\e -> if isAccessDenied e then return "-" else throwM e)
                          (formatSize . get csLength <$> getContentSummary f)

printListing :: FilePath -> Hdfs ()
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
            col a f = vcat a (map (text . f) (V.toList ls))

        liftIO $ do
            putStrLn $ "Found " <> show (V.length ls) <> " items"

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
