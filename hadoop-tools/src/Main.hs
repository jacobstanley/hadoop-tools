{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import           Control.Applicative
import           Control.Concurrent.STM
import           Control.Exception (SomeException, throwIO, fromException)
import           Control.Monad
import           Control.Monad.Catch (handle, throwM)
import           Control.Monad.IO.Class (liftIO)

import qualified Data.Attoparsec.ByteString.Char8 as Atto
import           Data.Bits ((.&.), shiftL, shiftR)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Maybe (fromMaybe)
import           Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Foldable as Foldable
import           Data.Time
import           Data.Time.Clock.POSIX
import qualified Data.Vector as V
import           Data.Version (showVersion)
import           Data.Word (Word16, Word64)

import           Options.Applicative hiding (Success)
import           Options.Applicative.Types (readerAsk)

import qualified System.FilePath.Posix as Posix
import           System.IO

import           Text.PrettyPrint.Boxes hiding ((<>), (//))

import           Data.Hadoop.Configuration (getHadoopUser)
import           Data.Hadoop.HdfsPath
import           Data.Hadoop.Types
import           Network.Hadoop.Hdfs hiding (runHdfs)
import           Network.Hadoop.Read

import           Chmod
import qualified Glob

import           Paths_hadoop_tools (version)

import Hadoop.Tools.Run
import Hadoop.Tools.Options
import Hadoop.Tools.Configuration

------------------------------------------------------------------------

main :: IO ()
main = do
    cmd <- execParser optsParser
    case cmd of
      SubIO   io   -> io
      SubHdfs hdfs -> handle exitError $ runHdfs hdfs
  where
    optsParser = info (helper <*> options)
                      (fullDesc <> header "hh - Blazing fast interaction with HDFS")

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
options = subparser (Foldable.foldMap sub allSubCommands)

allSubCommands :: [SubCommand]
allSubCommands =
    [ subCat
    , subChDir
    , subChMod
    , subChOwn
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

subCat :: SubCommand
subCat = SubCommand "cat" "Print the contents of a file to stdout" go
  where
    go = cat <$> many (hdfsPathArg $ help "the file to cat")
    cat paths = SubHdfs $ mapM_ (hdfsCat <=< getAbsolute) paths

subChDir :: SubCommand
subChDir = SubCommand "cd" "Change working directory" go
  where
    go = cd <$> optional (hdfsDirArg $ help "the directory to change to")
    cd mpath = SubHdfs $ do
        path <- getAbsolute =<< maybe getHomeDir return mpath
        _ <- getListingOrFail path
        setWorkingDir path

subChMod :: SubCommand
subChMod = SubCommand "chmod" "Change permissions" go
  where
    go = chmod <$> argument bstr (help "permissions mode")
               <*> hdfsPathArg (help "the file/directory to chmod")
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

subChOwn :: SubCommand
subChOwn = SubCommand "chown" "Change ownership and/or group" go
  where
    go = chown <$> argument userGroup (help "[USER][:GROUP]")
               <*> argument bstr (completeDir <> help "the file/directory to chown")

    chown (Nothing,Nothing) _ = SubHdfs $ fail "You must supply either a user or a group"
    chown (user,group) path = SubHdfs $ do
        absPath <- getAbsolute path
        minfo <- getFileInfo absPath
        case minfo of
            Nothing -> fail $ unwords ["No such file", B.unpack absPath]
            Just FileStatus{..} -> do
                setOwner user group absPath

    userGroup :: ReadM (Maybe User, Maybe Group)
    userGroup = unpackUserGroup <$> str

    unpackUserGroup :: String -> (Maybe User, Maybe Group)
    unpackUserGroup s =
        let
            userPart = takeWhile (/= ':') s

            user = if null userPart then Nothing else Just $ T.pack userPart

            group = case dropWhile (/= ':') s of
                ":"     -> Nothing
                ':':grp -> Just (T.pack grp)
                _       -> Nothing
        in (user, group)

subDiskUsage :: SubCommand
subDiskUsage = SubCommand "du" "Show the amount of space used by file or directory" go
  where
    go = du <$> optional (hdfsPathArg $ help "the file/directory to check the usage of")
    du path = SubHdfs $ printDiskUsage =<< getAbsolute (fromMaybe "" path)

timeArg :: ReadM (Ordering, NominalDiffTime)
timeArg = do
    res <- Atto.parseOnly parseTimeArg . B.pack <$> readerAsk
    case res of
        Right t -> return t
        Left s -> readerError $ "could't parse duration argument: " ++ s
  where
    parseTimeArg :: Atto.Parser (Ordering, NominalDiffTime)
    parseTimeArg = do
        rel <- Atto.choice
            [ Atto.char '-' *> pure LT
            , Atto.char '+' *> pure GT
            , pure EQ
            ]
        len <- Atto.choice [parseDuration] Atto.<?> "Expected a duration such as 1h30m"
        Atto.endOfInput Atto.<?> "malformed time format"
        return (rel, len)

    parseDuration = do
        durs <- some parseSegment
        return $ sum durs

    parseSegment = do
        d <- Atto.decimal
        mul <- Atto.choice
            [ Atto.char 's' *> pure (1 :: Int)
            , Atto.char 'm' *> pure 60
            , Atto.char 'h' *> pure 3600
            , Atto.char 'd' *> pure 86400
            , Atto.char 'w' *> pure 604800
            ]
        return $ fromIntegral (d * mul)

subFind :: SubCommand
subFind = SubCommand "find" "Recursively search a directory tree" go
  where
    go = find <$> optional (hdfsDirArg $ help "the path to recursively search")
              <*> optional (option bstr (long "name" <> metavar "FILENAME"
                                                     <> help "the file name to match"))
              <*> optional (option timeArg (long "atime" <> metavar "n[smhdw]"
                                                         <> help "access time query"))
              <*> optional (option timeArg (long "mtime" <> metavar "n[smhdw]"
                                                         <> help "modification time query"))
    find mpath mexpr atime mtime = SubHdfs $ do
        _ <- getFileInfo "/"
        matcher <- liftIO (mkMatcher mexpr)
        atimeP <- liftIO (timePredicate fsAccessTime atime)
        mtimeP <- liftIO (timePredicate fsModificationTime mtime)
        printFindResults (fromMaybe "" mpath) $ allPreds
            [ matcher
            , atimeP
            , mtimeP
            ]

    allPreds :: [FileStatus -> Bool] -> FileStatus -> Bool
    allPreds preds fs = and $ map ($ fs) preds

    mkMatcher :: Maybe ByteString -> IO (FileStatus -> Bool)
    mkMatcher Nothing     = return (const True)
    mkMatcher (Just expr) = do
        glob <- Glob.compile expr
        return (Glob.matches glob . fsPath)

    timePredicate :: (FileStatus -> HdfsTime) -> Maybe (Ordering, NominalDiffTime) -> IO (FileStatus -> Bool)
    timePredicate _ Nothing  = return (const True)
    timePredicate sel (Just (rel, n)) = do
        cur <- getCurrentTime
        return $ \f ->
            compare (diffUTCTime cur (hdfs2utc (sel f))) n == rel

subGet :: SubCommand
subGet = SubCommand "get" "Get a file" go
  where
    go = get <$> hdfsPathArg (help "source file")
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
    go = ls <$> optional (hdfsPathArg $ help "the directory to list")
    ls path = SubHdfs $ printListing =<< getAbsolute (fromMaybe "" path)

subMkDir :: SubCommand
subMkDir = SubCommand "mkdir" "Create a directory in the specified location" go
  where
    go = mkdir <$> hdfsDirArg (help "the directory to create")
               <*> switch (short 'p' <> help "create intermediate directories")
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
    go = rm <$> hdfsPathArg (help "the file/directory to remove")
            <*> switch      (short 'r' <> help "recursively remove the whole file hierarchy")
            <*> switch      (short 's' <> long "skipTrash" <> help "immediately delete, bypassing trash")
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
    go = mv <$> hdfsPathArg (help "source file/directory")
            <*> hdfsPathArg (help "destination file/directory")
            <*> switch      (short 'f' <> help "overwrite destination if it exists")
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
    go = test <$> hdfsPathArg (help "file/directory")
              <*> switch      (short 'e' <> help "Test exists")
              <*> switch      (short 'z' <> help "Test is zero length")
              <*> switch      (short 'd' <> help "Test is a directory")
              <*> switch      (short 'f' <> help "Test is a regular file")
              <*> switch      (short 'l' <> help "Test is a symbolic link")
              <*> switch      (short 'r' <> help "Test read permission is granted")
              <*> switch      (short 'w' <> help "Test write permission is granted")
              <*> switch      (short 'x' <> help "Test exectute permission is granted")
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
    go = testNewer <$> hdfsPathArg (help "file/directory")
                   <*> hdfsPathArg (help "file/directory")
                   <*> pure False

subTestOlder :: SubCommand
subTestOlder = SubCommand "test-older" "file1 is older (modification time) than file2" go
  where
    go = testNewer <$> hdfsPathArg (help "file/directory")
                   <*> hdfsPathArg (help "file/directory")
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

hdfs2utc :: HdfsTime -> UTCTime
hdfs2utc ms = posixSecondsToUTCTime (fromIntegral ms / 1000)

printListing :: HdfsPath -> Hdfs ()
printListing path = do
    ls <- getListingOrFail path

    let getModTime   = hdfs2utc . fsModificationTime

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


getListingOrFail :: HdfsPath -> Hdfs (V.Vector FileStatus)
getListingOrFail path = do
    mls <- getListing path
    case mls of
      Nothing -> throwM $ RemoteError ("File/directory does not exist: " <> T.decodeUtf8 path) T.empty
      Just ls -> return ls
