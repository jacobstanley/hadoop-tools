{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RebindableSyntax #-}

module Main (main, (//)) where

import           Control.Monad (unless, when)
import           Control.Monad.Catch (handle, throwM)
import           Control.Monad.IO.Class (liftIO)

import           Data.Bits ((.&.), shiftR)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Foldable (foldMap)
import           Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import           Data.Time
import           Data.Time.Clock.POSIX
import qualified Data.Vector as V
import           Data.Word (Word16, Word32, Word64)

import           System.FilePath.Posix
import           System.IO (stderr)
import           System.Locale (defaultTimeLocale)
import           Text.PrettyPrint.Boxes hiding ((<>), (//))

import           Data.Hadoop.Protobuf.Hdfs
import           Data.Hadoop.Types
import           Data.ProtocolBuffers (HasField(..), FieldType, getField)
import           Network.Hadoop.Hdfs hiding (runHdfs)

import           Options.Applicative hiding (Success)

import           Data.Version (showVersion)
import           Paths_hadoop_tools (version)

import           Haxl.Core
import qualified Haxl.Core.Monad as Haxl
import           Haxl.Prelude
import           Hdfs
import           HdfsSource

------------------------------------------------------------------------

main :: IO ()
main = do
    cmd <- execParser optsParser
    handle printError (runHdfs cmd)
  where
    optsParser = info (helper <*> options)
                      (fullDesc <> header "hh - Blazing fast interaction with HDFS")

------------------------------------------------------------------------

data SubCommand = SubCommand
    { subName        :: String
    , subDescription :: String
    , subMethod      :: Parser (Hdfs ())
    }

sub :: SubCommand -> Mod CommandFields (Hdfs ())
sub SubCommand{..} = command subName (info subMethod $ progDesc subDescription)

options :: Parser (Hdfs ())
options = subparser (foldMap sub allSubCommands)

allSubCommands :: [SubCommand]
allSubCommands =
    [ subChDir
    , subDiskUsage
    , subFind
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
bstr x = str x >>= return . B.pack

subChDir :: SubCommand
subChDir = SubCommand "cd" "Change working directory" go
  where
    go = cd <$> optional (argument bstr (completeDir <> help "the directory to change to"))
    cd mpath = do
        path <- getAbsolute =<< maybe getDefaultWorkingDir return mpath
        _    <- getListingOrFail path
        setWorkingDir path

subDiskUsage :: SubCommand
subDiskUsage = SubCommand "du" "Show the amount of space used by file or directory" go
  where
    go = du <$> optional (argument bstr (completePath <> help "the file/directory to check the usage of"))
    du path = printDiskUsage =<< getAbsolute (fromMaybe "" path)

subFind :: SubCommand
subFind = SubCommand "find" "Recursively search a directory tree" go
  where
    go = find <$> (argument bstr (completeDir <> help "the path to recursively search"))
              <*> (optional (argument bstr (metavar "EXPRESSION" <> help "the expression to match")))
    find path expr = printFindResults expr =<< getAbsolute path

subList :: SubCommand
subList = SubCommand "ls" "List the contents of a directory" go
  where
    go = ls <$> optional (argument bstr (completeDir <> help "the directory to list"))
    ls path = printListing =<< getAbsolute (fromMaybe "" path)

subMkDir :: SubCommand
subMkDir = SubCommand "mkdir" "Create a directory in the specified location" go
  where
    go = mkdir <$> argument bstr (completeDir <> help "the directory to create")
               <*> switch        (short 'p' <> help "create intermediate directories")
    mkdir path parent =  do
      absPath <- getAbsolute path
      ok <- mkdirs parent absPath
      unless ok $ liftIO . B.putStrLn $ "Failed to create: " <> absPath

subPwd :: SubCommand
subPwd = SubCommand "pwd" "Print working directory" go
  where
    go = pure pwd
    pwd = liftIO . B.putStrLn =<< getWorkingDir

subRemove :: SubCommand
subRemove = SubCommand "rm" "Delete a file or directory" go
  where
    go = rm <$> argument bstr (completePath <> help "the file/directory to remove")
            <*> switch        (short 'r' <> help "recursively remove the whole file hierarchy")
    rm path recursive = do
      absPath <- getAbsolute path
      ok <- delete recursive absPath
      unless ok $ liftIO . B.putStrLn $ "Failed to remove: " <> absPath

subRename :: SubCommand
subRename = SubCommand "mv" "Rename a file or directory" go
  where
    go = mv <$> argument bstr (completePath <> help "source file/directory")
            <*> argument bstr (completePath <> help "destination file/directory")
            <*> switch        (short 'f' <> help "overwrite destination if it exists")
    mv src dst force = do
      absSrc <- getAbsolute src
      absDst <- getAbsolute dst
      rename force absSrc absDst

subVersion :: SubCommand
subVersion = SubCommand "version" "Show version information" go
  where
    go = pure $ liftIO $ putStrLn $ "hh version " <> showVersion version

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
           . V.filter (p . get fsFileType)
           $ ls
  where
    ignore (RemoteError _ _) = return []

    splitFileName' x = case splitFileName x of
        ("./", f) -> ("", f)
        (d, f)    -> (d, f)

displayPath :: HdfsPath -> FileStatus -> HdfsPath
displayPath parent file = parent // get fsPath file <> suffix
  where
    suffix = case get fsFileType file of
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
                          (formatSize . get csLength <$> getContentSummary f)

printListing :: HdfsPath -> Hdfs ()
printListing path = do
    ls <- getListingOrFail path

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

--type Haxl a = GenHaxl () a

printFindResults :: Maybe ByteString -> HdfsPath -> Hdfs ()
printFindResults mexpr p = liftIO $ do
    hdfsState <- initGlobalState 10
    env' <- initEnv (stateSet hdfsState stateEmpty) ()
    runHaxl env' (loop p)
  where
    matches path = case mexpr of
        Nothing   -> True
        Just expr -> expr `B.isInfixOf` path

    loop path = (flip Haxl.catch) (Haxl.unsafeLiftIO . printError) $ do
        ls <- haxlListing path
        mapM_ printMatch ls
      where
        printMatch fs@FileStatus{..} = do
            let path' = displayPath path fs
            when (matches path') (Haxl.unsafeLiftIO $ B.putStrLn path')
            case getField fsFileType of
              Dir -> loop path'
              _   -> return ()

--printFindResults :: Maybe String -> HdfsPath -> Hdfs ()
--printFindResults expr path = handle (liftIO . printError) $ do
--    ls <- getListingOrFail path
--    V.mapM_ printMatch ls
--  where
--    printMatch :: FileStatus -> Hdfs ()
--    printMatch fs@FileStatus{..} = do
--        let path' = displayPath path fs
--        liftIO $ B.putStrLn path'
--        case getField fsFileType of
--          Dir -> printFindResults expr path'
--          _   -> return ()

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
    | oneLiner  = T.hPutStrLn stderr firstLine
    | otherwise = T.hPutStrLn stderr subject >> T.hPutStrLn stderr body
  where
    oneLiner  = subject `elem` [ "org.apache.hadoop.security.AccessControlException"
                               , "org.apache.hadoop.fs.FileAlreadyExistsException"
                               , "java.io.FileNotFoundException" ]
    firstLine = T.takeWhile (/= '\n') body

isAccessDenied :: RemoteError -> Bool
isAccessDenied (RemoteError s _) = s == "org.apache.hadoop.security.AccessControlException"
