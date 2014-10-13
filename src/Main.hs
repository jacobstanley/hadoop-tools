{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main (main) where

import           Control.Exception (SomeException, bracket)
import           Control.Monad
import           Control.Monad.Catch (handle, throwM)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Data.Attoparsec.ByteString.Char8 as Atto
import           Data.Bits ((.&.), shiftR)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Char (ord)
import           Data.Foldable (foldMap)
import           Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import           Data.Time
import           Data.Time.Clock.POSIX
import qualified Data.Vector as V
import           Data.Word (Word16, Word32, Word64)

import qualified Data.Configurator as C
import           Data.Configurator.Types (Worth(..))
import           System.Environment (getEnv)
import           System.FilePath.Posix
import           System.IO
import           System.IO.Unsafe (unsafePerformIO)
import           System.Locale (defaultTimeLocale)
import           Text.PrettyPrint.Boxes hiding ((<>), (//))

import           Data.Hadoop.Configuration (getHadoopConfig)
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs hiding (runHdfs)
import           Network.Hadoop.Read

import           Options.Applicative hiding (Success)

import           Data.Version (showVersion)
import           Paths_hadoop_tools (version)

------------------------------------------------------------------------

main :: IO ()
main = do
    cmd <- execParser optsParser
    case cmd of
      SubIO   io   -> io
      SubHdfs hdfs -> handle printError (runHdfs hdfs)
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

getDefaultWorkingDir :: MonadIO m => m HdfsPath
getDefaultWorkingDir = liftIO $ (("/user" //) . T.encodeUtf8 . hcUser) <$> getConfig

getWorkingDir :: MonadIO m => m HdfsPath
getWorkingDir = liftIO $ handle onError
                       $ B.takeWhile (/= '\n')
                     <$> B.readFile workingDirConfigPath
  where
    onError :: SomeException -> IO HdfsPath
    onError = const getDefaultWorkingDir

setWorkingDir :: MonadIO m => HdfsPath -> m ()
setWorkingDir path = liftIO $ B.writeFile workingDirConfigPath
                            $ path <> "\n"

getAbsolute :: MonadIO m => HdfsPath -> m HdfsPath
getAbsolute path = liftIO (normalizePath <$> getPath)
  where
    getPath = if "/" `B.isPrefixOf` path
              then return path
              else getWorkingDir >>= \pwd -> return (pwd // path)

normalizePath :: HdfsPath -> HdfsPath
normalizePath = B.intercalate "/" . dropAbsParentDir . B.split '/'

dropAbsParentDir :: [HdfsPath] -> [HdfsPath]
dropAbsParentDir []       = error "dropAbsParentDir: not an absolute path"
dropAbsParentDir (p : ps) = p : reverse (fst $ go [] ps)
  where
    go []       (".." : ys) = go [] ys
    go (_ : xs) (".." : ys) = go xs ys
    go xs       (y    : ys) = go (y : xs) ys
    go xs       []          = (xs, [])

------------------------------------------------------------------------

data SubCommand = SubCommand
    { subName        :: String
    , subDescription :: String
    , subMethod      :: Parser SubMethod
    }

data SubMethod = SubIO   (IO ())
               | SubHdfs (Hdfs ())

sub :: SubCommand -> Mod CommandFields SubMethod
sub SubCommand{..} = command subName (info subMethod $ progDesc subDescription)

options :: Parser SubMethod
options = subparser (foldMap sub allSubCommands)

allSubCommands :: [SubCommand]
allSubCommands =
    [ subCat
    , subChDir
    , subChMod
    , subDiskUsage
    -- , subFind
    , subGet
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

bstr :: Monad m => String -> m ByteString
bstr x = B.pack `liftM` str x

subCat :: SubCommand
subCat = SubCommand "cat" "Print the contents of a file to stdout" go
  where
    go = cat <$> many (argument bstr (completePath <> help "the file to cat"))
    cat paths = SubHdfs $ mapM_ (hdfsCat <=< getAbsolute) paths

subChDir :: SubCommand
subChDir = SubCommand "cd" "Change working directory" go
  where
    go = cd <$> optional (argument bstr (completeDir <> help "the directory to change to"))
    cd mpath = SubHdfs $ do
        path <- getAbsolute =<< maybe getDefaultWorkingDir return mpath
        _ <- getListingOrFail path
        setWorkingDir path

subChMod :: SubCommand
subChMod = SubCommand "chmod" "Change permissions" go
  where
    go = chmod <$> argument bstr (help "permissions mode")
               <*> argument bstr (completeDir <> help "the file/directory to chmod")
    chmod modeS path = either
        (\_ -> error $ "Unknown mode" ++ B.unpack modeS)
        (\mode -> SubHdfs $ modifyPerms mode path)
        (Atto.parseOnly parseMode modeS)

    parseMode = octal

    octal :: Atto.Parser Word16
    octal = B.foldl' step 0 `fmap` Atto.takeWhile1 isDig
      where
        isDig w = w >= '0' && w <= '7'
        step a w = a * 8 + fromIntegral (ord w - 48)

    modifyPerms :: Word16 -> HdfsPath -> Hdfs ()
    modifyPerms mode path = do
        absPath <- getAbsolute path
        info <- getFileInfo absPath
        case info of
            Nothing -> fail $ unwords ["No such file", B.unpack absPath]
            Just FileStatus{..} -> do
                {-
                liftIO . putStrLn . unwords $ ["Setting perms on", B.unpack absPath]
                liftIO . putStrLn . unwords $ ["OLD:", formatMode fsFileType fsPermission]
                liftIO . putStrLn . unwords $ ["NEW:", formatMode fsFileType mode]
                -}
                setPermissions (fromIntegral mode) absPath

subDiskUsage :: SubCommand
subDiskUsage = SubCommand "du" "Show the amount of space used by file or directory" go
  where
    go = du <$> optional (argument bstr (completePath <> help "the file/directory to check the usage of"))
    du path = SubHdfs $ printDiskUsage =<< getAbsolute (fromMaybe "" path)

subFind :: SubCommand
subFind = SubCommand "find" "Recursively search a directory tree" go
  where
    go = find <$> (argument bstr (completeDir <> help "the path to recursively search"))
              <*> (optional (option bstr (long "name" <> metavar "FILENAME"
                                                      <> help "the file name to match exactly")))
    find path mexpr = SubHdfs $ do
        absPath <- getAbsolute path
        printFindResults absPath $ maybe (const True) feq mexpr

    feq expr FileStatus{..} = expr == fsPath

subGet :: SubCommand
subGet = SubCommand "get" "Get a file" go
  where
    go = get <$> argument bstr (completePath <> help "source file")
             <*> optional (argument str (completePath <> help "destination file"))
    get src mdst = SubHdfs $ do
      let dst = fromMaybe (takeFileName $ B.unpack src) mdst
      absSrc <- getAbsolute src
      mReadHandle <- openRead absSrc
      let doRead readHandle = liftIO $ bracket
              (openFile dst WriteMode)
              (hClose)
              (\writeHandle -> hdfsMapM_ (B.hPut writeHandle) readHandle)
      maybe (return ()) doRead mReadHandle

subList :: SubCommand
subList = SubCommand "ls" "List the contents of a directory" go
  where
    go = ls <$> optional (argument bstr (completePath <> help "the directory to list"))
    ls path = SubHdfs $ printListing =<< getAbsolute (fromMaybe "" path)

subMkDir :: SubCommand
subMkDir = SubCommand "mkdir" "Create a directory in the specified location" go
  where
    go = mkdir <$> argument bstr (completeDir <> help "the directory to create")
               <*> switch        (short 'p' <> help "create intermediate directories")
    mkdir path parent = SubHdfs $ do
      absPath <- getAbsolute path
      ok <- mkdirs parent absPath
      unless ok $ liftIO . B.putStrLn $ "Failed to create: " <> absPath

subPwd :: SubCommand
subPwd = SubCommand "pwd" "Print working directory" go
  where
    go = pure $ SubIO $ B.putStrLn =<< getWorkingDir

subRemove :: SubCommand
subRemove = SubCommand "rm" "Delete a file or directory" go
  where
    go = rm <$> argument bstr (completePath <> help "the file/directory to remove")
            <*> switch        (short 'r' <> help "recursively remove the whole file hierarchy")
    rm path recursive = SubHdfs $ do
      absPath <- getAbsolute path
      ok <- delete recursive absPath
      unless ok $ liftIO . B.putStrLn $ "Failed to remove: " <> absPath

subRename :: SubCommand
subRename = SubCommand "mv" "Rename a file or directory" go
  where
    go = mv <$> argument bstr (completePath <> help "source file/directory")
            <*> argument bstr (completePath <> help "destination file/directory")
            <*> switch        (short 'f' <> help "overwrite destination if it exists")
    mv src dst force = SubHdfs $ do
      absSrc <- getAbsolute src
      absDst <- getAbsolute dst
      rename force absSrc absDst

subVersion :: SubCommand
subVersion = SubCommand "version" "Show version information" go
  where
    go = pure $ SubIO $ putStrLn $ "hh version " <> showVersion version

------------------------------------------------------------------------

fileCompletion :: (FileType -> Bool) -> Completer
fileCompletion p = mkCompleter $ \spath -> handle ignore $ runHdfs $ do
    let dir  = B.pack $ fst $ splitFileName' spath
        path = B.pack spath

    ls <- getListing' =<< getAbsolute dir

    return $ V.toList
           . V.map B.unpack
           . V.filter (path `B.isPrefixOf`)
           . V.map (displayPath dir)
           . V.filter (p . fsFileType)
           $ ls
  where
    ignore (RemoteError _ _) = return []

    splitFileName' x = case splitFileName x of
        ("./", f) -> ("", f)
        (d, f)    -> (d, f)

displayPath :: HdfsPath -> FileStatus -> HdfsPath
displayPath parent file = parent // fsPath file <> suffix
  where
    suffix = case fsFileType file of
        Dir -> "/"
        _   -> ""

------------------------------------------------------------------------

printDiskUsage :: HdfsPath -> Hdfs ()
printDiskUsage path = do
    ls <- getListingOrFail path

    let files = V.map (displayPath path) ls
    css <- V.zip files <$> V.mapM getDirSize files

    let col a f = vcat a (map (text . f) (V.toList css))

    liftIO $ printBox $ col right snd
                    <+> col left  (B.unpack . fst)
  where
    getDirSize f = handle (\e -> if isAccessDenied e then return "-" else throwM e)
                          (formatSize . csLength <$> getContentSummary f)

printListing :: HdfsPath -> Hdfs ()
printListing path = do
    ls <- getListingOrFail path

    let hdfs2utc ms  = posixSecondsToUTCTime (fromIntegral ms / 1000)
        getModTime   = hdfs2utc . fsModificationTime

        col a f = vcat a (map (text . f) (V.toList ls))

    liftIO $ do
        putStrLn $ "Found " <> show (V.length ls) <> " items"

        printBox $ col left  (\x -> formatMode (fsFileType x) (fsPermission x))
               <+> col right (formatBlockReplication . fsBlockReplication)
               <+> col left  (T.unpack . fsOwner)
               <+> col left  (T.unpack . fsGroup)
               <+> col right (formatSize . fsLength)
               <+> col right (formatUTC . getModTime)
               <+> col left  (ifEmpty basePath . T.unpack . T.decodeUtf8 . fsPath)
  where
    ifEmpty def x = if x=="" then def else x
    basePath = takeFileName (B.unpack path)

printFindResults :: HdfsPath -> (FileStatus -> Bool) -> Hdfs ()
printFindResults path cond = handle (liftIO . printError) $ do
    ls <- getListingOrFail path
    V.mapM_ printMatch ls
  where
    printMatch :: FileStatus -> Hdfs ()
    printMatch fs@FileStatus{..} = do
        let path' = displayPath path fs
        when (cond fs) (liftIO $ B.putStrLn path')
        case fsFileType of
          Dir -> printFindResults path' cond
          _   -> return ()

------------------------------------------------------------------------

formatSize :: Word64 -> String
formatSize b | b <= 0            = "0"
             | b < 1000          = show b <> "B"
             | b < 1000000       = show (b `div` 1000) <> "K"
             | b < 1000000000    = show (b `div` 1000000) <> "M"
             | b < 1000000000000 = show (b `div` 1000000000) <> "G"
             | otherwise         = show (b `div` 1000000000000) <> "T"

formatBlockReplication :: Word16 -> String
formatBlockReplication x | x == 0    = "-"
                         | otherwise = show x

formatUTC :: UTCTime -> String
formatUTC = formatTime defaultTimeLocale "%Y-%m-%d %H:%M"

formatMode :: FileType -> Permission -> String
formatMode File    = ("-" <>) . formatPermission
formatMode Dir     = ("d" <>) . formatPermission
formatMode SymLink = ("l" <>) . formatPermission

formatPermission :: Permission -> String
formatPermission perms = format (perms `shiftR` 6)
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
    | oneLiner    = T.putStrLn firstLine
    | T.null body = T.putStrLn subject
    | otherwise   = T.putStrLn subject >> T.putStrLn body
  where
    oneLiner  = subject `elem` [ "org.apache.hadoop.security.AccessControlException"
                               , "org.apache.hadoop.fs.FileAlreadyExistsException"
                               , "java.io.FileNotFoundException" ]
    firstLine = T.takeWhile (/= '\n') body

isAccessDenied :: RemoteError -> Bool
isAccessDenied (RemoteError s _) = s == "org.apache.hadoop.security.AccessControlException"

------------------------------------------------------------------------

infixr 5 //

(//) :: HdfsPath -> HdfsPath -> HdfsPath
(//) xs ys | B.null xs        = ys
           | B.null ys        = xs
           | B.head ys == '/' = ys
           | B.last xs == '/' = xs <> ys
           | otherwise        = xs <> "/" <> ys

------------------------------------------------------------------------

getListingOrFail :: HdfsPath -> Hdfs (V.Vector FileStatus)
getListingOrFail path = do
    mls <- getListing path
    case mls of
      Nothing -> throwM $ RemoteError ("File/directory does not exist: " <> T.decodeUtf8 path) T.empty
      Just ls -> return ls
