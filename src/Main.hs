{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import           Control.Concurrent.STM
import           Control.Exception (SomeException, throwIO, fromException)
import           Control.Monad
import           Control.Monad.Catch (handle, throwM)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Data.Attoparsec.ByteString.Char8 as Atto
import           Data.Bits ((.&.), shiftL, shiftR)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import           Data.Time
import           Data.Time.Clock.POSIX
import qualified Data.Vector as V
import           Data.Version (showVersion)
import           Data.Word (Word16, Word64)

import qualified Data.Configurator as C
import           Data.Configurator.Types (Worth(..))
import           Options.Applicative hiding (Success)
import           System.Environment (getEnv)
import           System.Exit (exitFailure)
import qualified System.FilePath as FilePath
import qualified System.FilePath.Posix as Posix
import           System.IO
import           System.IO.Unsafe (unsafePerformIO)
import           System.Posix.User (GroupEntry(..), getGroups, getGroupEntryForID)
import           Text.PrettyPrint.Boxes hiding ((<>), (//))

import           Data.Hadoop.Configuration (getHadoopConfig, getHadoopUser, readPrincipal)
import           Data.Hadoop.HdfsPath
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs hiding (runHdfs)
import           Network.Hadoop.Read

import           Chmod
import qualified Glob

import           Paths_hadoop_tools (version)

------------------------------------------------------------------------

main :: IO ()
main = do
    cmd <- execParser optsParser
    case cmd of
      SubIO   io   -> io
      SubHdfs hdfs -> runHdfs hdfs
  where
    optsParser = info (helper <*> options)
                      (fullDesc <> header "hh - Blazing fast interaction with HDFS")

runHdfs :: forall a. Hdfs a -> IO a
runHdfs hdfs = handle exitError $ do
    config <- getConfig
    run config
  where
    exitError :: RemoteError -> IO a
    exitError err = printError err >> exitFailure

    run :: HadoopConfig -> IO a
    run cfg = handle (runAgain cfg) (runHdfs' cfg hdfs)

    runAgain :: HadoopConfig -> RemoteError -> IO a
    runAgain cfg e | isStandbyError e = maybe (throwM e) run (dropNameNode cfg)
                   | otherwise        = throwM e

    dropNameNode :: HadoopConfig -> Maybe HadoopConfig
    dropNameNode cfg | null ns   = Nothing
                     | otherwise = Just (cfg { hcNameNodes = ns })
      where
        ns = drop 1 (hcNameNodes cfg)

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
    return (home `FilePath.combine` ".hh")
{-# NOINLINE configPath #-}

getHdfsUser :: IO (Maybe UserDetails)
getHdfsUser = do
    cfg <- C.load [Optional configPath]
    udUser <- C.lookup cfg "hdfs.user"
    udAuthUser <- C.lookup cfg "auth.user"
    return $ UserDetails <$> udUser <*> pure udAuthUser


-- getHdfsUser = C.load [Optional configPath] >>= flip C.lookup "hdfs.user"

getGroupNames :: IO [Group]
getGroupNames = do
    groups <- getGroups
    entries <- mapM getGroupEntryForID groups
    return $ map (T.pack . groupName) entries

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

------------------------------------------------------------------------

workingDirConfigPath :: FilePath
workingDirConfigPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home `FilePath.combine` ".hhwd")
{-# NOINLINE workingDirConfigPath #-}

getHomeDir :: MonadIO m => m HdfsPath
getHomeDir = liftIO $ (("/user" </>) . T.encodeUtf8 . (udUser . hcUser)) <$> getConfig

getWorkingDir :: MonadIO m => m HdfsPath
getWorkingDir = liftIO $ handle onError
                       $ B.takeWhile (/= '\n')
                     <$> B.readFile workingDirConfigPath
  where
    onError :: SomeException -> IO HdfsPath
    onError = const getHomeDir

setWorkingDir :: MonadIO m => HdfsPath -> m ()
setWorkingDir path = liftIO $ B.writeFile workingDirConfigPath
                            $ path <> "\n"

getAbsolute :: MonadIO m => HdfsPath -> m HdfsPath
getAbsolute path = liftIO (normalizePath <$> getPath)
  where
    getPath = if "/" `B.isPrefixOf` path
              then return path
              else getWorkingDir >>= \pwd -> return (pwd </> path)

normalizePath :: HdfsPath -> HdfsPath
normalizePath = B.intercalate "/" . dropAbsParentDir . B.split '/'

dropAbsParentDir :: [HdfsPath] -> [HdfsPath]
dropAbsParentDir []       = error "dropAbsParentDir: not an absolute path"
dropAbsParentDir (p : ps) = p : reverse (fst $ go [] ps)
  where
    go []       (".." : ys) = go [] ys
    go (_ : xs) (".." : ys) = go xs ys
    go xs       ("."  : ys) = go xs ys
    go xs       (y    : ys) = go (y : xs) ys
    go xs       []          = (xs, [])

dropFileName :: HdfsPath -> HdfsPath
dropFileName = B.pack . Posix.dropFileName . B.unpack

takeFileName :: HdfsPath -> HdfsPath
takeFileName = B.pack . Posix.takeFileName . B.unpack

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
    , subFind
    , subGet
    , subList
    , subMkDir
    , subPwd
    , subRemove
    , subRename
    , subTest
    , subTestNewer
    , subTestOlder
    , subVersion
    ]

completePath :: Mod ArgumentFields a
completePath = completer (fileCompletion (const True)) <> metavar "PATH"

completeDir :: Mod ArgumentFields a
completeDir  = completer (fileCompletion (== Dir)) <> metavar "DIRECTORY"

bstr :: ReadM ByteString
bstr = B.pack <$> str

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
        path <- getAbsolute =<< maybe getHomeDir return mpath
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
        (Atto.parseOnly parseChmod modeS)

    modifyPerms :: [Chmod] -> HdfsPath -> Hdfs ()
    modifyPerms mode path = do
        absPath <- getAbsolute path
        minfo <- getFileInfo absPath
        case minfo of
            Nothing -> fail $ unwords ["No such file", B.unpack absPath]
            Just FileStatus{..} -> do
                {-
                liftIO . putStrLn . unwords $ ["Setting perms on", B.unpack absPath]
                liftIO . putStrLn . unwords $ ["OLD:", formatMode fsFileType fsPermission]
                liftIO . putStrLn . unwords $ ["NEW:", formatMode fsFileType mode]
                -}
                setPermissions (fromIntegral $ applyChmod fsFileType mode fsPermission) absPath

subDiskUsage :: SubCommand
subDiskUsage = SubCommand "du" "Show the amount of space used by file or directory" go
  where
    go = du <$> optional (argument bstr (completePath <> help "the file/directory to check the usage of"))
    du path = SubHdfs $ printDiskUsage =<< getAbsolute (fromMaybe "" path)

subFind :: SubCommand
subFind = SubCommand "find" "Recursively search a directory tree" go
  where
    go = find <$> optional (argument bstr (completeDir <> help "the path to recursively search"))
              <*> optional (option bstr (long "name" <> metavar "FILENAME"
                                                     <> help "the file name to match"))
    find mpath mexpr = SubHdfs $ do
        _ <- getFileInfo "/"
        matcher <- liftIO (mkMatcher mexpr)
        printFindResults (fromMaybe "" mpath) matcher

    mkMatcher :: Maybe ByteString -> IO (FileStatus -> Bool)
    mkMatcher Nothing     = return (const True)
    mkMatcher (Just expr) = do
        glob <- Glob.compile expr
        return (Glob.matches glob . fsPath)

subGet :: SubCommand
subGet = SubCommand "get" "Get a file" go
  where
    go = get <$> argument bstr (completePath <> help "source file")
             <*> optional (argument str (completePath <> help "destination file"))
    get src mdst = SubHdfs $ do
      let dst = fromMaybe (Posix.takeFileName $ B.unpack src) mdst
      absSrc <- getAbsolute src
      mReadHandle <- openRead absSrc
      let doRead readHandle = liftIO $ withFile dst WriteMode
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
            <*> switch        (short 's' <> long "skipTrash" <> help "immediately delete, bypassing trash")
    rm path recursive skipTrash = SubHdfs $ do
      absPath <- getAbsolute path
      if skipTrash || ".Trash" `elem` B.split '/' absPath
          then do
              ok <- delete recursive absPath
              unless ok $ liftIO . B.putStrLn $ "Failed to remove: " <> absPath
          else do
              home <- getHomeDir
              let absDst = home </> ".Trash" </> "Current" <> absPath
                  absDstDir = dropFileName absDst
              ok <- mkdirs True absDstDir
              unless ok $ liftIO . B.putStrLn $ "Failed to make trash folder: " <> absDstDir
              rename True absPath absDst
              liftIO . B.putStrLn . B.unwords $ ["Moved:", absPath, "to trash at:", absDst]

subRename :: SubCommand
subRename = SubCommand "mv" "Rename a file or directory" go
  where
    go = mv <$> argument bstr (completePath <> help "source file/directory")
            <*> argument bstr (completePath <> help "destination file/directory")
            <*> switch        (short 'f' <> help "overwrite destination if it exists")
    mv src dst force = SubHdfs $ do
      absSrc <- getAbsolute src
      absDst <- getAbsolute dst
      mSrcType <- fmap fsFileType <$> getFileInfo absSrc
      mDstType <- fmap fsFileType <$> getFileInfo absDst
      let absDst' = if (mSrcType, mDstType) == (Just File, Just Dir)
          then absDst </> takeFileName src
          else absDst
      rename force absSrc absDst'

subTest :: SubCommand
subTest = SubCommand "test" "If file exists, has zero length, is a directory then return 0, else return 1" go
  where
    go = test <$> argument bstr (completePath <> help "file/directory")
               <*> switch        (short 'e' <> help "Test exists")
               <*> switch        (short 'z' <> help "Test is zero length")
               <*> switch        (short 'd' <> help "Test is a directory")
               <*> switch        (short 'f' <> help "Test is a regular file")
               <*> switch        (short 'l' <> help "Test is a symbolic link")
               <*> switch        (short 'r' <> help "Test read permission is granted")
               <*> switch        (short 'w' <> help "Test write permission is granted")
               <*> switch        (short 'x' <> help "Test exectute permission is granted")
    test path _e z d f l r w x = SubHdfs $ do
        absPath <- getAbsolute path
        minfo <- getFileInfo absPath
        user <- liftIO getHadoopUser
        groups <- liftIO getGroupNames
        case minfo of
            Nothing -> fail $ unwords ["No such file/directory", B.unpack absPath]
            Just fs@FileStatus{..} -> do
                when (d && fsFileType /= Dir) $ fail . unwords $ ["Not a directory"]
                when (f && fsFileType /= File) $ fail . unwords $ ["Not a regular file"]
                when (l && fsFileType /= SymLink) $ fail . unwords $ ["Not a regular file"]
                when (z && fsLength /= 0) $ fail . unwords $ ["Not zero length"]
                when (r && not (hasPerm user groups fs 4)) $  fail . unwords $ ["Not readable"]
                when (w && not (hasPerm user groups fs 2)) $  fail . unwords $ ["Not writable"]
                when (x && not (hasPerm user groups fs 1)) $  fail . unwords $ ["Not executable"]
      where
        hasPerm user groups FileStatus{..} p = (fsOwner == user && (fsPermission .&. (p `shiftL` 6)) /= 0) ||
                                               (fsGroup `elem` groups && (fsPermission .&. (p `shiftL` 3)) /= 0) ||
                                               ((fsPermission .&. p) /= 0)

subTestNewer :: SubCommand
subTestNewer = SubCommand "test-newer" "file1 is newer (modification time) than file2" go
  where
    go = testNewer <$> argument bstr (completePath <> help "file/directory")
                   <*> argument bstr (completePath <> help "file/directory")
                   <*> pure False

subTestOlder :: SubCommand
subTestOlder = SubCommand "test-older" "file1 is older (modification time) than file2" go
  where
    go = testNewer <$> argument bstr (completePath <> help "file/directory")
                   <*> argument bstr (completePath <> help "file/directory")
                   <*> pure True

testNewer :: HdfsPath -> HdfsPath -> Bool -> SubMethod
testNewer path1 path2 older = SubHdfs $ do
    absPath1 <- getAbsolute path1
    absPath2 <- getAbsolute path2
    minfo1 <- getFileInfo absPath1
    minfo2 <- getFileInfo absPath2
    case (minfo1, minfo2, older) of
        (Just fs1, Just fs2, False) ->
            when (fsModificationTime fs1 < fsModificationTime fs2) $
                fail . unwords $ [B.unpack path1, "is older than", B.unpack path2]
        (Just fs1, Just fs2, True) ->
            when (fsModificationTime fs1 > fsModificationTime fs2) $
                fail . unwords $ [B.unpack path1, "is newer than", B.unpack path2]
        _ -> fail $ unwords ["No such file/directory"]

subVersion :: SubCommand
subVersion = SubCommand "version" "Show version information" go
  where
    go = pure $ SubIO $ putStrLn $ "hh version " <> showVersion version

------------------------------------------------------------------------

fileCompletion :: (FileType -> Bool) -> Completer
fileCompletion p = mkCompleter $ \strPath -> handle ignore $ runHdfs $ do
    let path = B.pack strPath
        dir  = takeParent path

    ls <- getListing' =<< getAbsolute dir

    return $ V.toList
           . V.map B.unpack
           . V.filter (path `B.isPrefixOf`)
           . V.map (displayPath dir)
           . V.filter (p . fsFileType)
           $ ls
  where
    ignore (RemoteError _ _) = return []

takeParent :: HdfsPath -> HdfsPath
takeParent bs = case B.elemIndexEnd '/' bs of
    Nothing -> B.empty
    Just 0  -> "/"
    Just ix -> B.take ix bs

displayPath :: HdfsPath -> FileStatus -> HdfsPath
displayPath parent file = parent </> fsPath file <> suffix
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
    basePath = Posix.takeFileName (B.unpack path)

printFindResults :: HdfsPath -> (FileStatus -> Bool) -> Hdfs ()
printFindResults path cond = do
    absPath <- getAbsolute path
    q <- getListingRecursive absPath
    liftIO $ loop q $ printMatch (replaceFullPathWithInputPath absPath path)
  where
    loop :: TBQueue (Maybe (HdfsPath, Either SomeException (V.Vector FileStatus)))
         -> (HdfsPath -> FileStatus -> IO ())
         -> IO ()
    loop q io = do
        mx <- atomically (readTBQueue q)
        case mx of
          Nothing -> return ()
          Just (parent, x) -> do
            case x of
              (Left err) -> printOrThrow err
              (Right ls) -> V.mapM_ (io parent) ls
            loop q io

    printOrThrow :: SomeException -> IO ()
    printOrThrow ex = case fromException ex of
        Nothing  -> throwIO ex
        Just err -> printError err

    printMatch :: (HdfsPath -> HdfsPath) -> HdfsPath -> FileStatus -> IO ()
    printMatch fixup parent fs@FileStatus{..} = do
        let path' = fixup (displayPath parent fs)
        when (cond fs) (B.putStrLn path')

replaceFullPathWithInputPath :: HdfsPath -> HdfsPath -> HdfsPath -> HdfsPath
replaceFullPathWithInputPath fullPath inputPath path
    | fullPath' `B.isPrefixOf` path = inputPath </> B.drop (B.length fullPath') path
    | otherwise                     = path
  where
    fullPath' = addDirSlash fullPath

    addDirSlash dir | "/" `B.isSuffixOf` dir = dir
                    | otherwise              = dir <> "/"

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
    | oneLiner    = T.hPutStrLn stderr firstLine
    | T.null body = T.hPutStrLn stderr subject
    | otherwise   = T.hPutStrLn stderr subject >> T.putStrLn body
  where
    oneLiner  = subject `elem` [ "org.apache.hadoop.security.AccessControlException"
                               , "org.apache.hadoop.fs.FileAlreadyExistsException"
                               , "java.io.FileNotFoundException" ]
    firstLine = T.takeWhile (/= '\n') body

isAccessDenied :: RemoteError -> Bool
isAccessDenied (RemoteError s _) = s == "org.apache.hadoop.security.AccessControlException"

isStandbyError :: RemoteError -> Bool
isStandbyError (RemoteError s _) = s == "org.apache.hadoop.ipc.StandbyException"

------------------------------------------------------------------------

getListingOrFail :: HdfsPath -> Hdfs (V.Vector FileStatus)
getListingOrFail path = do
    mls <- getListing path
    case mls of
      Nothing -> throwM $ RemoteError ("File/directory does not exist: " <> T.decodeUtf8 path) T.empty
      Just ls -> return ls
